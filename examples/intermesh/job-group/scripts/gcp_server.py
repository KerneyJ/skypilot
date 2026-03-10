"""GCP Server: HTTP server running on Google Cloud.

This server exposes health, info, and echo endpoints for testing
cross-cloud connectivity via Intermesh.
"""
from fastapi import FastAPI
import uvicorn

app = FastAPI()


@app.get('/health')
def health():
    return {'status': 'healthy', 'service': 'gcp-server', 'cloud': 'gcp'}


@app.get('/info')
def info():
    return {
        'message': 'Hello from GCP!',
        'cloud': 'gcp',
        'region': 'us-central1',
        'port': 8001
    }


@app.post('/echo')
def echo(data: dict):
    return {'received': data, 'from': 'gcp-server'}


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8001)
