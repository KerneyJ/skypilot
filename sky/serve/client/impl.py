"""Implementation of SDK for SkyServe."""
import json
import typing
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import click

from sky.client import common as client_common
from sky.server import common as server_common
from sky.server.requests import payloads
from sky.server.requests import request_names
from sky.utils import admin_policy_utils
from sky.utils import dag_utils

if typing.TYPE_CHECKING:
    import io

    import sky
    from sky.serve import serve_utils


def up(
    task: Union['sky.Task', 'sky.Dag'],
    service_name: str,
    pool: bool = False,
    # Internal only:
    # pylint: disable=invalid-name
    _need_confirmation: bool = False
) -> server_common.RequestId[Tuple[str, str]]:
    assert not pool, 'Command `up` is not supported for pool.'
    # Avoid circular import.
    from sky.client import sdk  # pylint: disable=import-outside-toplevel

    dag = dag_utils.convert_entrypoint_to_dag(task)
    with admin_policy_utils.apply_and_use_config_in_current_request(
            dag,
            request_name=request_names.AdminPolicyRequestName.SERVE_UP,
            at_client_side=True) as dag:
        sdk.validate(dag)
        request_id = sdk.optimize(dag)
        sdk.stream_and_get(request_id)
        if _need_confirmation:
            noun = 'pool' if pool else 'service'
            prompt = f'Launching a new {noun} {service_name!r}. Proceed?'
            if prompt is not None:
                click.confirm(prompt,
                              default=True,
                              abort=True,
                              show_default=True)

        dag = client_common.upload_mounts_to_api_server(dag)
        dag_str = dag_utils.dump_chain_dag_to_yaml_str(dag)

        body = payloads.ServeUpBody(
            task=dag_str,
            service_name=service_name,
        )

        response = server_common.make_authenticated_request(
            'POST',
            '/serve/up',
            json=json.loads(body.model_dump_json()),
            timeout=(5, None))
        return server_common.get_request_id(response)


def _is_intermesh_only_update(yaml_content: str) -> bool:
    """Check if update is only enabling intermesh.

    Returns True if YAML contains only intermesh configuration
    (and optionally config/cluster_config_overrides), with no
    other significant fields like resources, run, service, etc.

    Args:
        yaml_content: The YAML content as a string.

    Returns:
        True if this is an intermesh-only update, False otherwise.
    """
    try:
        import yaml
        from sky import sky_logging

        logger = sky_logging.init_logger(__name__)
        config = yaml.safe_load(yaml_content)

        # Must have intermesh key
        if 'intermesh' not in config:
            return False

        # Check allowed keys (these don't trigger full update)
        allowed_keys = {'intermesh', 'config', 'cluster_config_overrides'}
        actual_keys = set(config.keys())

        # If any other keys present, it's a full update
        significant_keys = actual_keys - allowed_keys
        if significant_keys:
            return False

        # Intermesh must be enabled
        intermesh_config = config.get('intermesh', {})
        if not intermesh_config.get('enabled', False):
            return False

        return True
    except Exception as e:
        from sky import sky_logging
        logger = sky_logging.init_logger(__name__)
        logger.warning(f'Failed to parse update yaml file: {e}')
        return False


def update(
    task: Union['sky.Task', 'sky.Dag'],
    service_name: str,
    mode: 'serve_utils.UpdateMode',
    pool: bool = False,
    # Internal only:
    # pylint: disable=invalid-name
    _need_confirmation: bool = False
) -> server_common.RequestId[None]:
    assert not pool, 'Command `update` is not supported for pool.'
    # Avoid circular import.
    from sky.client import sdk  # pylint: disable=import-outside-toplevel
    from sky import sky_logging

    logger = sky_logging.init_logger(__name__)
    noun = 'pool' if pool else 'service'

    dag = dag_utils.convert_entrypoint_to_dag(task)

    # Get YAML content for detection
    dag_str = dag_utils.dump_chain_dag_to_yaml_str(dag)

    # Special case: intermesh-only update
    # Skip normal validation/replacement and just install intermesh
    if _is_intermesh_only_update(dag_str):
        logger.info('Detected intermesh-only update. Installing Intermesh '
                   'without replica replacement...')
        return update_intermesh(service_name, pool, _need_confirmation)

    # Normal update flow continues...
    with admin_policy_utils.apply_and_use_config_in_current_request(
            dag,
            request_name=request_names.AdminPolicyRequestName.SERVE_UPDATE,
            at_client_side=True) as dag:
        sdk.validate(dag)
        request_id = sdk.optimize(dag)
        sdk.stream_and_get(request_id)
        if _need_confirmation:
            click.confirm(f'Updating {noun} {service_name!r}. Proceed?',
                          default=True,
                          abort=True,
                          show_default=True)

        dag = client_common.upload_mounts_to_api_server(dag)
        # Re-serialize after upload_mounts (dag may have been modified)
        dag_str = dag_utils.dump_chain_dag_to_yaml_str(dag)

        body = payloads.ServeUpdateBody(
            task=dag_str,
            service_name=service_name,
            mode=mode,
        )

        response = server_common.make_authenticated_request(
            'POST',
            '/serve/update',
            json=json.loads(body.model_dump_json()),
            timeout=(5, None))
        return server_common.get_request_id(response)


def update_intermesh(
    service_name: str,
    pool: bool = False,
    yes: bool = False
) -> server_common.RequestId[Dict[str, Any]]:
    """Install Intermesh on an existing service.

    This function retrofits Intermesh onto a service that was deployed without
    it, enabling E2E encryption between the controller and replicas.

    Args:
        service_name: Name of the service.
        pool: Whether this is a pool (not supported).
        yes: Skip confirmation prompt.

    Returns:
        The request ID of the installation operation.

    Raises:
        ValueError: if service doesn't exist or is not in READY status
        AssertionError: if pool is True (not supported for pools)
    """
    from sky import sky_logging
    from sky.client import sdk  # pylint: disable=import-outside-toplevel
    from sky.serve import serve_state

    logger = sky_logging.init_logger(__name__)
    assert not pool, 'Command `update intermesh` is not supported for pool.'

    # Get service status to validate it exists
    status_request_id = status([service_name], pool=False)
    services = sdk.get(status_request_id)

    if not services:
        raise ValueError(f'Service {service_name!r} not found.')

    service_info = services[0]

    # Check if service is in a good state
    # service_info['status'] returns a string like 'READY', 'FAILED', etc.
    status_str = str(service_info['status'])
    # Handle both enum string format (ServiceStatus.READY) and plain string (READY)
    is_ready = 'READY' in status_str
    if not is_ready:
        raise ValueError(
            f'Service {service_name!r} is in status {service_info["status"]}. '
            'Can only install Intermesh on services in READY status.')

    # Confirmation prompt
    if not yes:
        replica_count = len(service_info.get('replica_info', []))
        click.confirm(
            f'This will install Intermesh on service {service_name!r} '
            f'(1 controller + {replica_count} replicas). Proceed?',
            default=True,
            abort=True,
            show_default=True)

    # Create request body
    body = payloads.ServeUpdateIntermeshBody(
        service_name=service_name,
    )

    # Send to API server
    response = server_common.make_authenticated_request(
        'POST',
        '/serve/update-intermesh',
        json=json.loads(body.model_dump_json()),
        timeout=(5, None))

    return server_common.get_request_id(response)


def apply(
    task: Optional[Union['sky.Task', 'sky.Dag']],
    workers: Optional[int],
    service_name: str,
    mode: 'serve_utils.UpdateMode',
    pool: bool = False,
    # Internal only:
    # pylint: disable=invalid-name
    _need_confirmation: bool = False
) -> server_common.RequestId[None]:
    assert pool, 'Command `apply` is only supported for pool.'
    # Avoid circular import.
    from sky.client import sdk  # pylint: disable=import-outside-toplevel

    noun = 'pool' if pool else 'service'
    # There are two cases here. If task is None, we should be trying to
    # update the number of workers in the pool. If task is not None, we should
    # be trying to apply a new config to the pool. The two code paths
    # are slightly different with us needing to craft the dag and validate
    # it if we have a task. In the future we could move this logic to the
    # server side and simplify this code, for the time being we keep it here.
    if task is None:
        if workers is None:
            raise ValueError(f'Cannot create a new {noun} without specifying '
                             f'task or workers. Please provide either a task '
                             f'or specify the number of workers.')

        body = payloads.JobsPoolApplyBody(
            workers=workers,
            pool_name=service_name,
            mode=mode,
        )

        response = server_common.make_authenticated_request(
            'POST',
            '/jobs/pool_apply',
            json=json.loads(body.model_dump_json()),
            timeout=(5, None))
        return server_common.get_request_id(response)
    else:
        dag = dag_utils.convert_entrypoint_to_dag(task)
        with admin_policy_utils.apply_and_use_config_in_current_request(
                dag,
                request_name=request_names.AdminPolicyRequestName.
                JOBS_POOL_APPLY,
                at_client_side=True) as dag:
            sdk.validate(dag)
            request_id = sdk.optimize(dag)
            sdk.stream_and_get(request_id)
            if _need_confirmation:
                prompt = f'Applying config to {noun} {service_name!r}. Proceed?'
                if prompt is not None:
                    click.confirm(prompt,
                                  default=True,
                                  abort=True,
                                  show_default=True)

            dag = client_common.upload_mounts_to_api_server(dag)
            dag_str = dag_utils.dump_chain_dag_to_yaml_str(dag)

            body = payloads.JobsPoolApplyBody(
                task=dag_str,
                pool_name=service_name,
                mode=mode,
            )
            response = server_common.make_authenticated_request(
                'POST',
                '/jobs/pool_apply',
                json=json.loads(body.model_dump_json()),
                timeout=(5, None))
            return server_common.get_request_id(response)


def down(
    service_names: Optional[Union[str, List[str]]],
    all: bool = False,  # pylint: disable=redefined-builtin
    purge: bool = False,
    pool: bool = False,
) -> server_common.RequestId[None]:
    if pool:
        body = payloads.JobsPoolDownBody(
            pool_names=service_names,
            all=all,
            purge=purge,
        )
    else:
        body = payloads.ServeDownBody(
            service_names=service_names,
            all=all,
            purge=purge,
        )
    response = server_common.make_authenticated_request(
        'POST',
        '/jobs/pool_down' if pool else '/serve/down',
        json=json.loads(body.model_dump_json()),
        timeout=(5, None))
    return server_common.get_request_id(response)


def status(
    service_names: Optional[Union[str, List[str]]],
    pool: bool = False,
) -> server_common.RequestId[List[Dict[str, Any]]]:
    if pool:
        body = payloads.JobsPoolStatusBody(pool_names=service_names)
    else:
        body = payloads.ServeStatusBody(service_names=service_names)
    response = server_common.make_authenticated_request(
        'POST',
        '/jobs/pool_status' if pool else '/serve/status',
        json=json.loads(body.model_dump_json()),
        timeout=(5, None))
    return server_common.get_request_id(response)


def tail_logs(service_name: str,
              target: Union[str, 'serve_utils.ServiceComponent'],
              replica_id: Optional[int] = None,
              follow: bool = True,
              output_stream: Optional['io.TextIOBase'] = None,
              tail: Optional[int] = None,
              pool: bool = False) -> None:
    # Avoid circular import.
    from sky.client import sdk  # pylint: disable=import-outside-toplevel

    if pool:
        body = payloads.JobsPoolLogsBody(
            pool_name=service_name,
            target=target,
            worker_id=replica_id,
            follow=follow,
            tail=tail,
        )
    else:
        body = payloads.ServeLogsBody(
            service_name=service_name,
            target=target,
            replica_id=replica_id,
            follow=follow,
            tail=tail,
        )
    response = server_common.make_authenticated_request(
        'POST',
        '/jobs/pool_logs' if pool else '/serve/logs',
        json=json.loads(body.model_dump_json()),
        timeout=(5, None),
        stream=True)
    request_id: server_common.RequestId[None] = server_common.get_request_id(
        response)
    sdk.stream_response(request_id=request_id,
                        response=response,
                        output_stream=output_stream,
                        resumable=True,
                        get_result=follow)


def sync_down_logs(service_name: str,
                   local_dir: str,
                   *,
                   targets: Optional[Union[
                       str, 'serve_utils.ServiceComponent',
                       Sequence[Union[str,
                                      'serve_utils.ServiceComponent']]]] = None,
                   replica_ids: Optional[List[int]] = None,
                   tail: Optional[int] = None,
                   pool: bool = False) -> None:
    # Avoid circular import.
    from sky.client import sdk  # pylint: disable=import-outside-toplevel

    if pool:
        body = payloads.JobsPoolDownloadLogsBody(
            pool_name=service_name,
            local_dir=local_dir,
            targets=targets,
            worker_ids=replica_ids,
            tail=tail,
        )
    else:
        body = payloads.ServeDownloadLogsBody(
            service_name=service_name,
            # No need to set here, since the server will override it
            # to a directory on the API server.
            local_dir=local_dir,
            targets=targets,
            replica_ids=replica_ids,
            tail=tail,
        )
    response = server_common.make_authenticated_request(
        'POST',
        '/jobs/pool_sync-down-logs' if pool else '/serve/sync-down-logs',
        json=json.loads(body.model_dump_json()),
        timeout=(5, None))
    request_id: server_common.RequestId[str] = server_common.get_request_id(
        response)
    remote_dir = sdk.stream_and_get(request_id)

    # Download from API server paths to the client's local_dir
    client_common.download_logs_from_api_server([remote_dir], remote_dir,
                                                local_dir)
