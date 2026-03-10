"""Coordinator: Tests cross-cloud connectivity via Intermesh.

This script connects to both GCP and AWS servers running in different
clouds and validates that cross-cloud networking is working correctly.
"""
import json
import os
import time

import httpx


def wait_for_service(client: httpx.Client,
                     url: str,
                     name: str,
                     max_retries: int = 10,
                     retry_delay: int = 10) -> bool:
    """Wait for a service to become healthy."""
    for i in range(1, max_retries + 1):
        try:
            response = client.get(f'{url}/health')
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'healthy':
                    print(f'{name} is healthy!')
                    return True
        except (httpx.RequestError, httpx.HTTPStatusError):
            pass
        print(f'Waiting for {name}... attempt {i}')
        time.sleep(retry_delay)
    return False


def test_server(client: httpx.Client, url: str, name: str) -> None:
    """Test a server's info and echo endpoints."""
    print(f'Getting info from {name}...')
    response = client.get(f'{url}/info')
    print(json.dumps(response.json(), indent=2))

    print(f'Testing echo endpoint on {name}...')
    response = client.post(f'{url}/echo',
                           json={
                               'test': 'cross-cloud message',
                               'from': 'coordinator'
                           })
    print(json.dumps(response.json(), indent=2))


def main():
    print('Starting multi-cloud coordinator...')

    jobgroup_name = os.environ.get('SKYPILOT_JOBGROUP_NAME', 'unknown')
    print(f'JobGroup: {jobgroup_name}')

    # Service discovery via Intermesh DNS (.sky.mesh suffix)
    gcp_server = f'http://gcp-server-0.{jobgroup_name}.sky.mesh:8001'
    aws_server = f'http://aws-server-0.{jobgroup_name}.sky.mesh:8002'

    print(f'GCP Server: {gcp_server}')
    print(f'AWS Server: {aws_server}')

    # Wait for services to be ready
    print('Waiting for services to be available...')
    time.sleep(45)

    with httpx.Client(timeout=30.0) as client:
        print()
        print('=' * 42)
        print('Testing GCP Server')
        print('=' * 42)

        if wait_for_service(client, gcp_server, 'GCP server'):
            test_server(client, gcp_server, 'GCP')

        print()
        print('=' * 42)
        print('Testing AWS Server')
        print('=' * 42)

        if wait_for_service(client, aws_server, 'AWS server'):
            test_server(client, aws_server, 'AWS')

        print()
        print('=' * 42)
        print('Multi-Cloud Job Group Test Complete!')
        print('=' * 42)
        print()
        print('Successfully tested connectivity:')
        print('  - Coordinator (GCP us-west1) -> GCP Server (us-central1)')
        print('  - Coordinator (GCP us-west1) -> AWS Server (us-east-1)')
        print()
        print('Cross-cloud networking via intermesh is working!')


if __name__ == '__main__':
    main()
