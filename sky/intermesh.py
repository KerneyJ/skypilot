"""Intermesh integration for SkyPilot cross-cloud networking."""
import asyncio
import re
import shlex
import subprocess
import textwrap
from typing import Any, List, Tuple, TYPE_CHECKING

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # Python < 3.11 fallback

from sky import exceptions
from sky import provision as provision_lib
from sky import sky_logging
from sky.backends import backend_utils
from sky.serve import serve_state
from sky.utils import command_runner
from sky.utils.command_runner import CommandRunner
from sky.utils.command_runner import LocalProcessCommandRunner

if TYPE_CHECKING:
    from typing import Union

    from sky import dag as dag_lib
    from sky import Task
    from sky.backends.cloud_vm_ray_backend import CloudVmRayResourceHandle

logger = sky_logging.init_logger(__name__)

# TODO(kerneyj): THIS IS FOR DEMO PURPOSES
# this is not how we plan to configure this
# Bootstrap script to install Intermesh binary
INTERMESH_INSTALL_SCRIPT = textwrap.dedent("""
# Download precompiled Intermesh binary from GitHub
curl -sL https://raw.githubusercontent.com/KerneyJ/intermesh-binaries/main/intermesh-linux-x86_64 \\
  -o /tmp/intermesh
sudo mv /tmp/intermesh /usr/bin/intermesh
sudo chmod +x /usr/bin/intermesh
echo "Intermesh binary installed"
""")


def _validate_ip_address(ip: str) -> None:
    """Validate IP address format to prevent shell injection.

    Args:
        ip: IP address string to validate (IPv4 or IPv6)

    Raises:
        ValueError: If the IP address format is invalid
    """
    # Allow only characters valid in IPv4/IPv6 addresses
    if not re.match(r'^[\d.:a-fA-F]+$', ip):
        raise ValueError(f'Invalid IP address format: {ip}')


def _make_daemon_start_script(ip: str) -> str:
    """Generate script to start Intermesh daemon with the given IP.

    Args:
        ip: The IP address to use for this node (typically external IP)

    Returns:
        Shell script to start the daemon

    Raises:
        ValueError: If the IP address format is invalid
    """
    _validate_ip_address(ip)
    escaped_ip = shlex.quote(ip)
    return textwrap.dedent(f"""
# Only start daemon if not already running
if sudo intermesh status &>/dev/null; then
  echo "Intermesh daemon already running"
else
  # Start daemon (properly detached from SSH session)
  nohup sudo intermesh daemon --intercept --ip {escaped_ip} --log-file /var/log/intermesh.log > /dev/null 2>&1 &
  disown

  # Give it a moment to start
  sleep 2

  # Wait for daemon to be ready (up to 30 seconds)
  for i in {{1..30}}; do
    if sudo intermesh status &>/dev/null; then
      echo "Intermesh daemon is ready"
      break
    fi
    sleep 1
  done
fi

echo "Intermesh daemon started with IP {escaped_ip}"
""")


# Shared mesh domain for all SkyPilot services on a controller.
# All services share one mesh because the controller is shared across services.
# Intermesh only supports one mesh per node, so using per-service mesh domains
# would cause the second service to fail initialization.
SKYPILOT_MESH_DOMAIN = 'sky.mesh'

# Ports required for Intermesh cross-node communication.
# 9898: GOSSIP_PORT - mTLS endorsement exchange between nodes
# 9797: EXTERNAL_PORT - HTTP CONNECT proxy for incoming mTLS tunnels
INTERMESH_REQUIRED_PORTS = ['9898', '9797']


def _make_controller_name() -> str:
    """Generate the mesh name for the controller node.

    Returns:
        Full mesh name (e.g., "controller.sky.mesh")
    """
    return f'controller.{SKYPILOT_MESH_DOMAIN}'


def _make_replica_name(replica_id: int, service_name: str) -> str:
    """Generate the mesh name for a replica node.

    Args:
        replica_id: SkyPilot replica ID
        service_name: SkyPilot service name

    Returns:
        Full mesh name (e.g., "replica-1.my-service.sky.mesh")
    """
    return f'replica-{replica_id}.{service_name}.{SKYPILOT_MESH_DOMAIN}'


# TODO(kerneyj): make this more rich, we should return which
# nodes failed to install Intermesh. And honestly, if it worked
# on the head node we can probably proceed without issue
def install_intermesh(runner: CommandRunner, external_ip: str) -> None:
    """Install Intermesh on a node and start daemon.

    Args:
        runner: CommandRunner for the node
        external_ip: External IP address for this node

    Raises:
        exceptions.IntermeshError: If installation or daemon start fails
    """
    logger.debug(f'[Intermesh] Installing with IP {external_ip}...')

    try:
        # Install the binary
        returncode = runner.run(
            cmd=['bash', '-c', INTERMESH_INSTALL_SCRIPT],
            stream_logs=False,
        )

        if returncode != 0:
            raise exceptions.IntermeshError(
                'Failed to install Intermesh binary')

        # Start daemon with external IP
        start_script = _make_daemon_start_script(external_ip)
        returncode = runner.run(
            cmd=['bash', '-c', start_script],
            stream_logs=False,
        )
        if returncode != 0:
            raise exceptions.IntermeshError('Failed to start Intermesh daemon')

        logger.debug('[Intermesh] Successfully installed and started')

    except (OSError, subprocess.SubprocessError) as e:
        raise exceptions.IntermeshError(
            f'Error installing Intermesh: {e}') from e


def is_intermesh_enabled(task_or_dag: 'Union[Task, dag_lib.Dag]') -> bool:
    """Determine if Intermesh is enabled for a task or DAG.

    For Tasks (SkyServe): checks resources.cluster_config_overrides
    For Dags (Job Groups): checks dag.intermesh_config from header

    Args:
        task_or_dag: A Task or Dag object

    Returns:
        True if intermesh is enabled
    """
    # Check if this is a Dag (has intermesh_config attribute)
    if hasattr(task_or_dag, 'intermesh_config'):
        intermesh_config = task_or_dag.intermesh_config or {}
        return intermesh_config.get('enabled', False)

    # Task case: check resources.cluster_config_overrides
    for resource in task_or_dag.resources:
        intermesh_config = resource.cluster_config_overrides.get(
            'intermesh', {})
        if intermesh_config:
            return intermesh_config.get('enabled', False)
    return False


def get_imid(runner: CommandRunner) -> str:
    """Return the IMID of a node via runner.

    Args:
        runner: CommandRunner for the node (LocalProcessCommandRunner for local,
                SSHCommandRunner for remote)

    Returns:
        IMID string

    Raises:
        exceptions.IntermeshError: If IMID retrieval fails
    """
    try:
        returncode, stdout, stderr = runner.run(
            cmd=['sudo', 'intermesh', 'debug', 'dump', '--toml'],
            stream_logs=False,
            require_outputs=True,
        )
        if returncode != 0:
            raise exceptions.IntermeshError(
                f'Could not retrieve imid: {stderr}')

        # Parse TOML output to extract my_imid from derivation section
        try:
            data = tomllib.loads(stdout)
            imid = data.get('derivation', {}).get('my_imid')
            if not imid:
                raise exceptions.IntermeshError(
                    'Could not find my_imid in debug dump output')
            return imid
        except (tomllib.TOMLDecodeError, KeyError, TypeError) as e:
            raise exceptions.IntermeshError(
                f'Failed to parse debug dump output: {e}') from e

    except (OSError, subprocess.SubprocessError) as e:
        raise exceptions.IntermeshError(f'Could not retrieve imid: {e}') from e


def _get_endorsed_ip(runner: CommandRunner) -> str:
    """Get the local IP from Intermesh daemon state.

    Args:
        runner: CommandRunner for the node

    Returns:
        Local IP string

    Raises:
        exceptions.IntermeshError: If IP retrieval fails
    """
    try:
        returncode, stdout, stderr = runner.run(
            cmd=['sudo', 'intermesh', 'debug', 'dump', '--toml'],
            stream_logs=False,
            require_outputs=True,
        )
        if returncode != 0:
            raise exceptions.IntermeshError(
                f'Could not retrieve local ip: {stderr}')

        data = tomllib.loads(stdout)
        # Get the node's own IMID from derivation section
        derivation = data.get('derivation', {})
        my_imid = derivation.get('my_imid')
        if not my_imid:
            raise exceptions.IntermeshError(
                'Could not find my_imid in debug dump output')

        # Look up the IP for this IMID in imid_to_ip mapping
        imid_to_ip = derivation.get('imid_to_ip', {})
        ip_list = imid_to_ip.get(my_imid)
        if not ip_list or len(ip_list) == 0:
            raise exceptions.IntermeshError(
                f'Could not find IP for IMID {my_imid} in debug dump')

        return ip_list[0]

    except (OSError, subprocess.SubprocessError, tomllib.TOMLDecodeError,
            KeyError, TypeError, IndexError) as e:
        raise exceptions.IntermeshError(
            f'Could not retrieve local ip: {e}') from e


def _get_resolved_names(runner: CommandRunner) -> set:
    """Get set of resolved mesh names from intermesh derivation state.

    Args:
        runner: CommandRunner for the node

    Returns:
        Set of mesh name strings that are currently resolvable on this node.
        Returns empty set if intermesh is not ready or command fails.
    """
    try:
        returncode, stdout, stderr = runner.run(
            cmd=['sudo', 'intermesh', 'debug', 'dump', '--toml'],
            stream_logs=False,
            require_outputs=True,
        )
        if returncode != 0:
            logger.debug(f'[Intermesh] debug dump failed: {stderr}')
            return set()

        data = tomllib.loads(stdout)
        name_to_imid = data.get('derivation', {}).get('name_to_imid', {})
        return set(name_to_imid.keys())

    except (OSError, subprocess.SubprocessError, tomllib.TOMLDecodeError,
            KeyError, TypeError) as e:
        logger.debug(f'[Intermesh] Failed to get resolved names: {e}')
        return set()


def register_replica(replica_imid: str, replica_id: int, service_name: str,
                     replica_ip: str) -> str:
    """Register a replica with the controller for Intermesh.

    This runs on the controller node to add the replica to the mesh.

    Args:
        replica_imid: intermesh id (imid) of the replica
        replica_id: id of the replica from skypilot
        service_name: service name from skypilot
        replica_ip: external ip address of the replica for cross-cloud routing

    Returns:
        The mesh name of the replica

    Raises:
        exceptions.IntermeshError: If registration fails
    """
    mesh_name = _make_replica_name(replica_id, service_name)

    try:
        subprocess.run(
            [
                'sudo',
                'intermesh',
                'adhoc',
                'add',
                '--name',
                mesh_name,
                '--imid',
                replica_imid,
                '--ip',
                replica_ip,
            ],
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        )
        logger.debug(f'[Intermesh] Registered replica as {mesh_name}')
        return mesh_name
    except subprocess.CalledProcessError as e:
        raise exceptions.IntermeshError(
            f'Failed to register replica with controller: {e.stderr}') from e
    except (OSError, subprocess.SubprocessError) as e:
        raise exceptions.IntermeshError(
            f'Failed to register replica with controller: {e}') from e


def initialize_mesh(runner: CommandRunner) -> None:
    """Configure Intermesh on controller (initialize mesh as root).

    Args:
        runner: CommandRunner for controller node

    Raises:
        RuntimeError: If configuration fails
    """
    logger.debug('[Intermesh] Initializing mesh on controller...')

    # Use --quiet to suppress interactive prompts and only output the token
    config_cmd = [
        'sudo', 'intermesh', 'adhoc', 'init', '--name',
        _make_controller_name(), '--mesh', SKYPILOT_MESH_DOMAIN, '--quiet'
    ]

    try:
        returncode, _, stderr = runner.run(
            cmd=config_cmd,
            stream_logs=False,
            require_outputs=True,
        )

        if returncode != 0:
            raise RuntimeError(f'Failed to initialize mesh: {stderr}')

        logger.debug('[Intermesh] Controller mesh initialized successfully')

    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f'Error initializing mesh: {e}') from e


def replica_join_mesh(runner: CommandRunner, controller_imid: str,
                      controller_ip: str) -> str:
    """Configure Intermesh on replica (join existing mesh).

    Uses direct join mode - the replica writes local membership from explicit
    root identity provided by the orchestrator. No token exchange needed.

    Args:
        runner: CommandRunner for replica node
        controller_imid: Controller's IMID
        controller_ip: Controller's internal IP address

    Returns:
        Replica IMID

    Raises:
        exceptions.IntermeshError: If joining mesh fails
    """
    logger.debug('[Intermesh] Joining mesh from replica...')

    # Use direct join mode: no token needed, orchestrator provides root identity
    join_cmd = [
        'sudo', 'intermesh', 'adhoc', 'join', '--mesh', SKYPILOT_MESH_DOMAIN,
        '--root-imid', controller_imid, '--root-ip', controller_ip, '--yes'
    ]

    try:
        returncode, _, stderr = runner.run(
            cmd=join_cmd,
            stream_logs=False,
            require_outputs=True,
        )

        if returncode != 0:
            raise exceptions.IntermeshError(f'Failed to join mesh: {stderr}')

        # Get the replica's IMID after joining
        replica_imid = get_imid(runner)
        logger.debug(
            f'[Intermesh] Replica joined mesh, IMID: {replica_imid[:20]}...')
        return replica_imid

    except (OSError, subprocess.SubprocessError) as e:
        raise exceptions.IntermeshError(f'Error joining mesh: {e}') from e


def _open_intermesh_ports(handle: 'CloudVmRayResourceHandle') -> None:
    """Open Intermesh ports on a cluster's security group.

    Args:
        handle: CloudVmRayResourceHandle for the cluster

    Raises:
        exceptions.IntermeshError: If port opening fails
    """
    cloud = handle.launched_resources.cloud
    cluster_info = handle.cached_cluster_info
    provider_config = cluster_info.provider_config if cluster_info else None

    if provider_config is None:
        raise exceptions.IntermeshError(
            f'No provider_config available for opening ports on '
            f'{handle.cluster_name}')

    # Ensure firewall_rule is set for GCP (required by open_ports)
    # This may be missing if the cluster was provisioned without ports
    if 'firewall_rule' not in provider_config or \
            provider_config.get('firewall_rule') is None:
        provider_config = dict(provider_config)
        provider_config['firewall_rule'] = (
            f'sky-ports-{handle.cluster_name_on_cloud}')

    logger.debug(f'[Intermesh] Opening ports {INTERMESH_REQUIRED_PORTS} on '
                 f'{handle.cluster_name}')
    provision_lib.open_ports(repr(cloud), handle.cluster_name_on_cloud,
                             INTERMESH_REQUIRED_PORTS, provider_config)


def configure_intermesh_replica(handle: 'CloudVmRayResourceHandle',
                                replica_id: int, service_name: str) -> bool:
    """Configures Intermesh on a replica.

    This function:
    1. Installs Intermesh on the replica
    2. Has the replica join the mesh (connecting to the controller)
    3. Registers the replica with the controller

    Args:
        handle: resource handle for the replica
        replica_id: id of the replica in skypilot
        service_name: name of the sky serve service

    Returns:
        True if successful, False otherwise
    """
    # Create an SSH runner for replica
    ssh_credentials = backend_utils.ssh_credential_from_yaml(
        handle.cluster_yaml,
        handle.docker_user,
        handle.ssh_user,
    )

    external_ips = handle.cached_external_ips
    internal_ips = handle.cached_internal_ips
    external_ssh_ports = handle.cached_external_ssh_ports

    if (external_ips is None or external_ssh_ports is None or
            internal_ips is None):
        logger.warning(f'[Intermesh] Could not get IPs/ports for replica '
                       f'{replica_id}')
        return False

    replica_runners = command_runner.SSHCommandRunner.make_runner_list(
        zip(external_ips, external_ssh_ports), **ssh_credentials)

    replica_external_ip = external_ips[0]
    head_runner = replica_runners[0]

    try:
        install_intermesh(head_runner, replica_external_ip)
        _open_intermesh_ports(handle)

        # Get controller info - runs locally on the serve controller node
        local_runner = command_runner.LocalProcessCommandRunner()
        controller_imid = get_imid(local_runner)
        controller_ip = _get_endorsed_ip(local_runner)

        # Replica joins the mesh using controller's identity
        replica_imid = replica_join_mesh(head_runner, controller_imid,
                                         controller_ip)

        # Register the replica on the controller side with external IP
        mesh_name = register_replica(replica_imid, replica_id, service_name,
                                     replica_external_ip)

    except exceptions.IntermeshError as e:
        logger.warning(f'[Intermesh] Configuration failed for replica '
                       f'{replica_id}: {e}')
        return False
    except (RuntimeError, OSError, ValueError) as e:
        logger.warning(f'[Intermesh] Failed to open ports for replica '
                       f'{replica_id}: {e}')
        return False

    # Update replica info in serve state
    replica_info = serve_state.get_replica_info_from_id(service_name,
                                                        replica_id)
    if not replica_info:
        logger.warning(f'[Intermesh] Failed to retrieve replica_info for '
                       f'replica {replica_id}')
        return False

    replica_info.mesh_name = mesh_name
    serve_state.add_or_update_replica(service_name, replica_id, replica_info)

    return True


def configure_intermesh_controller(handle: 'CloudVmRayResourceHandle') -> None:
    """Configures Intermesh on SkyServe controller.

    Args:
        handle: CloudVmRayResourceHandle of the controller cluster

    Raises:
        exceptions.IntermeshError: If configuration fails
    """
    logger.debug('[Intermesh] Configuring for SkyServe controller')

    external_ips = handle.external_ips()
    if not external_ips:
        raise exceptions.IntermeshError(
            'Failed to get external IP address of controller')

    head_ip = external_ips[0]
    runners = handle.get_command_runners()
    head_runner = runners[0]

    install_intermesh(head_runner, head_ip)
    logger.debug('[Intermesh] Successfully installed on controller')

    _open_intermesh_ports(handle)
    initialize_mesh(head_runner)


# ============================================================
# Job Groups Support
# ============================================================


def _make_job_group_mesh_domain(job_group_name: str) -> str:
    """Generate mesh domain for job group.

    Args:
        job_group_name: Name of the job group

    Returns:
        Mesh domain (e.g., "my-job-group.sky.mesh")
    """
    return f'{job_group_name}.{SKYPILOT_MESH_DOMAIN}'


def _make_task_mesh_name(task_name: str, node_idx: int,
                         job_group_name: str) -> str:
    """Generate mesh name for a task node in a job group.

    Args:
        task_name: Name of the task
        node_idx: Node index within the task (0 for head, 1+ for workers)
        job_group_name: Name of the job group

    Returns:
        Full mesh name (e.g., "trainer-0.my-job-group.sky.mesh")
    """
    return f'{task_name}-{node_idx}.{job_group_name}.{SKYPILOT_MESH_DOMAIN}'


def _setup_job_group_coordinator(
    runner: CommandRunner,
    job_group_name: str,
    coordinator_name: str,
) -> str:
    """Initialize mesh on coordinator node.

    Args:
        runner: CommandRunner for the coordinator node
        job_group_name: Name of the job group
        coordinator_name: Mesh name for the coordinator

    Returns:
        IMID of the coordinator

    Raises:
        exceptions.IntermeshError: If initialization fails
    """
    mesh_domain = _make_job_group_mesh_domain(job_group_name)
    logger.debug(f'[Intermesh] Initializing mesh {mesh_domain} on coordinator')

    config_cmd = [
        'sudo', 'intermesh', 'adhoc', 'init', '--name', coordinator_name,
        '--mesh', mesh_domain, '--quiet'
    ]

    try:
        returncode, _, stderr = runner.run(
            cmd=config_cmd,
            stream_logs=False,
            require_outputs=True,
        )

        if returncode != 0:
            raise exceptions.IntermeshError(
                f'Failed to initialize mesh: {stderr}')

        imid = get_imid(runner)
        logger.debug(f'[Intermesh] Coordinator initialized, IMID: {imid[:20]}')
        return imid

    except (OSError, subprocess.SubprocessError) as e:
        raise exceptions.IntermeshError(f'Error initializing mesh: {e}') from e


def _job_group_node_join(
    runner: CommandRunner,
    job_group_name: str,
    coordinator_imid: str,
    coordinator_ip: str,
) -> str:
    """Join mesh on non-coordinator node.

    Args:
        runner: CommandRunner for the joining node
        job_group_name: Name of the job group
        coordinator_imid: IMID of the coordinator
        coordinator_ip: External IP of the coordinator

    Returns:
        IMID of the joining node

    Raises:
        exceptions.IntermeshError: If joining fails
    """
    mesh_domain = _make_job_group_mesh_domain(job_group_name)
    logger.debug(f'[Intermesh] Node joining mesh {mesh_domain}')

    join_cmd = [
        'sudo', 'intermesh', 'adhoc', 'join', '--mesh', mesh_domain,
        '--root-imid', coordinator_imid, '--root-ip', coordinator_ip, '--yes'
    ]

    try:
        returncode, _, stderr = runner.run(
            cmd=join_cmd,
            stream_logs=False,
            require_outputs=True,
        )

        if returncode != 0:
            raise exceptions.IntermeshError(f'Failed to join mesh: {stderr}')

        imid = get_imid(runner)
        logger.debug(f'[Intermesh] Node joined mesh, IMID: {imid[:20]}...')
        return imid

    except (OSError, subprocess.SubprocessError) as e:
        raise exceptions.IntermeshError(f'Error joining mesh: {e}') from e


def _register_job_group_node(
    coordinator_runner: CommandRunner,
    mesh_name: str,
    node_imid: str,
    node_ip: str,
) -> None:
    """Register a node with the coordinator.

    Args:
        coordinator_runner: CommandRunner for the coordinator node
        mesh_name: Full mesh name for the node
        node_imid: IMID of the node to register
        node_ip: External IP of the node

    Raises:
        exceptions.IntermeshError: If registration fails
    """
    logger.debug(f'[Intermesh] Registering node {mesh_name}')

    add_cmd = [
        'sudo', 'intermesh', 'adhoc', 'add', '--name', mesh_name, '--imid',
        node_imid, '--ip', node_ip
    ]

    try:
        returncode, _, stderr = coordinator_runner.run(
            cmd=add_cmd,
            stream_logs=False,
            require_outputs=True,
        )

        if returncode != 0:
            raise exceptions.IntermeshError(
                f'Failed to register node {mesh_name}: {stderr}')

        logger.debug(f'[Intermesh] Registered {mesh_name}')

    except (OSError, subprocess.SubprocessError) as e:
        raise exceptions.IntermeshError(
            f'Error registering node {mesh_name}: {e}') from e


async def _install_on_node_async(
    runner: CommandRunner,
    external_ip: str,
) -> None:
    """Install intermesh on a node asynchronously.

    Raises:
        exceptions.IntermeshError: If installation fails
    """
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None,
                               lambda: install_intermesh(runner, external_ip))


async def _open_ports_async(handle: 'CloudVmRayResourceHandle') -> None:
    """Open intermesh ports on a cluster asynchronously.

    Raises:
        exceptions.IntermeshError: If port opening fails
    """
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, lambda: _open_intermesh_ports(handle))


async def _node_join_async(
    runner: CommandRunner,
    job_group_name: str,
    coordinator_imid: str,
    coordinator_ip: str,
) -> str:
    """Have a node join the mesh asynchronously.

    Returns:
        IMID of the joined node

    Raises:
        exceptions.IntermeshError: If joining fails
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None, lambda: _job_group_node_join(runner, job_group_name,
                                           coordinator_imid, coordinator_ip))


async def _wait_for_mesh_resolution_async(
    runner: CommandRunner,
    expected_names: set,
    timeout: float = 60.0,
    poll_interval: float = 2.0,
) -> None:
    """Wait for all expected mesh names to be resolvable on a node.

    Polls the node's intermesh derivation state until all expected names
    appear in the name_to_imid map, indicating the mesh has propagated.

    Args:
        runner: CommandRunner for the node
        expected_names: Set of mesh name strings that must be resolvable
        timeout: Maximum time to wait in seconds (default 60s)
        poll_interval: Time between polls in seconds (default 2s)

    Raises:
        exceptions.IntermeshError: If timeout is reached before all names
            resolve
    """
    loop = asyncio.get_running_loop()
    start_time = loop.time()

    while True:
        resolved = await loop.run_in_executor(
            None, lambda: _get_resolved_names(runner))

        if expected_names.issubset(resolved):
            return  # All names resolved

        elapsed = loop.time() - start_time
        if elapsed >= timeout:
            missing = expected_names - resolved
            raise exceptions.IntermeshError(
                f'Mesh resolution timeout after {timeout}s. '
                f'Missing names: {missing}')

        await asyncio.sleep(poll_interval)


async def setup_job_group_intermesh(
    job_group_name: str,
    tasks_handles: List[Tuple['Task', 'CloudVmRayResourceHandle']],
) -> None:
    """Set up Intermesh networking for a Job Group.

    This orchestrates the full intermesh setup:
    1. Install intermesh on all nodes (parallel)
    2. Open ports on all clusters (parallel)
    3. Get controller mesh info (already set up by client)
    4. All job nodes join the controller mesh (parallel)
    5. Register all nodes with coordinator (sequential)
    6. Wait for mesh name resolution on all nodes (parallel polling)

    Args:
        job_group_name: Name of the job group
        tasks_handles: List of (Task, ResourceHandle) tuples

    Raises:
        exceptions.IntermeshError: If setup fails
    """

    if not tasks_handles:
        raise exceptions.IntermeshError('No tasks to set up')

    logger.info(f'[Intermesh] Setting up mesh for job group: {job_group_name}')

    # Collect all nodes: (runner, task_name, node_idx, external_ip, handle)
    # Node info: (runner, task_name, node_idx, external_ip, handle)
    all_nodes: List[Tuple[Any, str, int, str, 'CloudVmRayResourceHandle']] = []
    for task, handle in tasks_handles:
        if handle is None:
            logger.warning(f'[Intermesh] Skipping task {task.name}: no handle')
            continue

        task_name = task.name
        if task_name is None:
            logger.warning('[Intermesh] Skipping task with no name')
            continue

        external_ips = handle.cached_external_ips
        if not external_ips:
            logger.warning(f'[Intermesh] Skipping task {task_name}: '
                           'no external IPs')
            continue

        try:
            runners = handle.get_command_runners()
        except (RuntimeError, ValueError, OSError) as e:
            raise exceptions.IntermeshError(
                f'Failed to get runners for {task_name}: {e}') from e

        for node_idx, (runner,
                       external_ip) in enumerate(zip(runners, external_ips)):
            all_nodes.append((runner, task_name, node_idx, external_ip, handle))

    if not all_nodes:
        raise exceptions.IntermeshError('No valid nodes found')

    logger.info(f'[Intermesh] Found {len(all_nodes)} nodes across '
                f'{len(tasks_handles)} tasks')

    # Step 1: Install intermesh on all nodes in parallel
    logger.info('[Intermesh] Installing on all nodes...')
    install_tasks = [
        _install_on_node_async(runner, external_ip)
        for runner, task_name, node_idx, external_ip, _ in all_nodes
    ]
    install_results = await asyncio.gather(*install_tasks,
                                           return_exceptions=True)
    install_errors = [r for r in install_results if isinstance(r, Exception)]
    if install_errors:
        raise exceptions.IntermeshError(
            f'Installation failed on {len(install_errors)} nodes: '
            f'{install_errors[0]}')

    # Step 2: Open ports on all clusters in parallel (deduplicated)
    logger.info('[Intermesh] Opening ports on all clusters...')
    seen_clusters: set[str] = set()
    port_tasks = []
    for task, handle in tasks_handles:
        if handle is None or handle.cluster_name in seen_clusters:
            continue
        seen_clusters.add(handle.cluster_name)
        port_tasks.append(_open_ports_async(handle))

    if port_tasks:
        port_results = await asyncio.gather(*port_tasks, return_exceptions=True)
        port_errors = [r for r in port_results if isinstance(r, Exception)]
        if port_errors:
            raise exceptions.IntermeshError(
                f'Port opening failed on {len(port_errors)} clusters: '
                f'{port_errors[0]}')

    # Step 3: Get controller's mesh info (already set up by client side)
    # The controller is the coordinator, set up via
    # configure_intermesh_controller() from the client before job launch.
    # We just need to get its IMID and IP.
    coord_runner = LocalProcessCommandRunner()
    logger.info('[Intermesh] Getting controller mesh info...')

    loop = asyncio.get_running_loop()
    try:
        coord_imid = await loop.run_in_executor(None,
                                                lambda: get_imid(coord_runner))
        coord_ip = await loop.run_in_executor(
            None, lambda: _get_endorsed_ip(coord_runner))
    except exceptions.IntermeshError as e:
        raise exceptions.IntermeshError(
            f'Controller intermesh not running. Was '
            f'configure_intermesh_controller called from client? '
            f'Error: {e}') from e

    logger.info(f'[Intermesh] Controller IMID: {coord_imid[:20]}..., '
                f'IP: {coord_ip}')

    # Step 4: All job nodes join the controller (all nodes are joiners)
    joined_nodes: List[Tuple[str, str, str]] = []  # (mesh_name, imid, ip)

    logger.info(
        f'[Intermesh] {len(all_nodes)} nodes joining controller mesh...')
    join_tasks = [
        _node_join_async(runner, job_group_name, coord_imid, coord_ip)
        for runner, _, _, _, _ in all_nodes
    ]
    join_results = await asyncio.gather(*join_tasks, return_exceptions=True)

    for i, result in enumerate(join_results):
        _, task_name, node_idx, external_ip, _ = all_nodes[i]
        if isinstance(result, Exception):
            raise exceptions.IntermeshError(
                f'Node {task_name}-{node_idx} failed to join: {result}')
        # result is the IMID string since we checked isinstance above
        assert isinstance(result, str)
        imid = result
        mesh_name = _make_task_mesh_name(task_name, node_idx, job_group_name)
        joined_nodes.append((mesh_name, imid, external_ip))

    # Step 5: Register all nodes with coordinator (sequential)
    logger.info('[Intermesh] Registering nodes with coordinator...')
    for mesh_name, imid, external_ip in joined_nodes:
        await loop.run_in_executor(None, _register_job_group_node, coord_runner,
                                   mesh_name, imid, external_ip)

    # Step 6: Wait for mesh resolution on all nodes
    # Poll each node's intermesh derivation state until all expected mesh names
    # are resolvable. This verifies the mesh is actually working rather than
    # relying on an arbitrary gossip timeout.
    logger.info('[Intermesh] Waiting for mesh name resolution...')
    expected_names = {mesh_name for mesh_name, _, _ in joined_nodes}
    expected_names.add(_make_controller_name())  # controller.sky.mesh

    resolution_tasks = [
        _wait_for_mesh_resolution_async(runner, expected_names)
        for runner, _, _, _, _ in all_nodes
    ]
    resolution_results = await asyncio.gather(*resolution_tasks,
                                              return_exceptions=True)
    resolution_errors = [
        r for r in resolution_results if isinstance(r, Exception)
    ]
    if resolution_errors:
        error_msgs = [str(e) for e in resolution_errors]
        raise exceptions.IntermeshError(
            f'Mesh resolution failed on {len(resolution_errors)} node(s): '
            f'{"; ".join(error_msgs)}')

    logger.info(f'[Intermesh] Job group {job_group_name} mesh setup complete')
