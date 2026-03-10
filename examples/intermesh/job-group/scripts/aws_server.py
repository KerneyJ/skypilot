"""AWS Server: HTTP server running on Amazon Web Services.

This server exposes health, info, and echo endpoints for testing
cross-cloud connectivity via Intermesh.
"""
from fastapi import FastAPI
import uvicorn

app = FastAPI()


@app.get('/health')
def health():
    return {'status': 'healthy', 'service': 'aws-server', 'cloud': 'aws'}


@app.get('/info')
def info():
    return {
        'message': 'Hello from AWS!',
        'cloud': 'aws',
        'region': 'us-east-1',
        'port': 8002
    }


@app.post('/echo')
def echo(data: dict):
    return {'received': data, 'from': 'aws-server'}


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8002)
