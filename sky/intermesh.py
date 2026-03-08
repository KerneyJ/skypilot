"""Intermesh integration for SkyPilot cross-cloud networking."""
import re
import shlex
import subprocess
import textwrap
from typing import TYPE_CHECKING

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

if TYPE_CHECKING:
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


def is_intermesh_enabled(task: 'Task') -> bool:
    """Determine if Intermesh is enabled in a task.

    Args:
        task: Task config dictionary

    Returns:
        True if there exists an intermesh resource in task and that
        resource has a field enabled that has value True
    """
    for resource in task.resources:
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


def _get_local_ip(runner: CommandRunner) -> str:
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

        # Open Intermesh ports on the replica's firewall/security group
        cloud = handle.launched_resources.cloud
        cluster_info = handle.cached_cluster_info
        provider_config = (cluster_info.provider_config
                           if cluster_info else None)
        if provider_config is None:
            raise exceptions.IntermeshError(
                'No provider_config available for opening ports')
        logger.debug(f'[Intermesh] Opening ports '
                     f'{INTERMESH_REQUIRED_PORTS} on replica')
        provision_lib.open_ports(repr(cloud), handle.cluster_name_on_cloud,
                                 INTERMESH_REQUIRED_PORTS, provider_config)

        # Get controller info - runs locally on the serve controller node
        local_runner = command_runner.LocalProcessCommandRunner()
        controller_imid = get_imid(local_runner)
        controller_ip = _get_local_ip(local_runner)

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

    # Open Intermesh ports on the controller's firewall/security group
    cloud = handle.launched_resources.cloud
    cluster_info = handle.cached_cluster_info
    provider_config = cluster_info.provider_config if cluster_info else None
    if provider_config is None:
        raise exceptions.IntermeshError(
            'No provider_config available for opening ports on controller')
    logger.debug(f'[Intermesh] Opening ports '
                 f'{INTERMESH_REQUIRED_PORTS} on controller')
    provision_lib.open_ports(repr(cloud), handle.cluster_name_on_cloud,
                             INTERMESH_REQUIRED_PORTS, provider_config)

    initialize_mesh(head_runner)
