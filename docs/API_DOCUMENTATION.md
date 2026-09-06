# Dataset Pipeline API Documentation

## Overview

REST API for triggering and monitoring the dataset building pipeline.

**Base URL**: `http://localhost:8000` (default)

## Quick Start

### 1. Start the Server

```bash
# Development mode
./run.sh dev

# Production mode
./run.sh prod

# With Docker
./run.sh docker
```

### 2. Test the API

```bash
# Health check
curl http://localhost:8000/health

# Trigger pipeline
curl -X POST http://localhost:8000/pipeline/run \
  -H "Content-Type: application/json" \
  -d '{"config_path": "data_pipeline/configs/pipeline_config.yaml"}'

# Check job status
curl http://localhost:8000/pipeline/jobs/{job_id}
```

---

## Endpoints

### 1. Root / Service Info

**GET** `/`

Returns API service information and available endpoints.

**Response**:
```json
{
  "service": "Dataset Pipeline API",
  "version": "2.0",
  "status": "running",
  "endpoints": {
    "health": "GET /health",
    "trigger": "POST /pipeline/run",
    "status": "GET /pipeline/jobs/{job_id}",
    "list": "GET /pipeline/jobs",
    "manifest": "GET /pipeline/jobs/{job_id}/manifest",
    "logs": "GET /pipeline/jobs/{job_id}/logs"
  }
}
```

---

### 2. Health Check

**GET** `/health`

Health check endpoint for monitoring.

**Response**:
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:00Z"
}
```

**Status Codes**:
* `200 OK` - Service is healthy

---

### 3. Trigger Pipeline

**POST** `/pipeline/run`

Start a new pipeline execution.

**Request Body**:
```json
{
  "config_path": "path/to/config.yaml"  // Optional, defaults to standard path
}
```

**Response** (202 Accepted):
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "pending",
  "message": "Pipeline job created and started",
  "created_at": "2024-01-15T10:30:00Z"
}
```

**Status Codes**:
* `202 Accepted` - Job created successfully
* `404 Not Found` - Config file not found
* `400 Bad Request` - Invalid request

**Example**:
```bash
# With default config
curl -X POST http://localhost:8000/pipeline/run \
  -H "Content-Type: application/json" \
  -d '{}'

# With custom config
curl -X POST http://localhost:8000/pipeline/run \
  -H "Content-Type: application/json" \
  -d '{"config_path": "configs/experiment_a.yaml"}'
```

---

### 4. Get Job Status

**GET** `/pipeline/jobs/{job_id}`

Retrieve status and metadata for a specific job.

**Path Parameters**:
* `job_id` (string) - UUID of the pipeline job

**Response**:
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "running",  // pending | running | completed | failed
  "config_path": "data_pipeline/configs/pipeline_config.yaml",
  "created_at": "2024-01-15T10:30:00Z",
  "started_at": "2024-01-15T10:30:05Z",
  "completed_at": null,
  "error": null,
  "has_manifest": false,
  "log_file": "logs/pipeline_550e8400-e29b-41d4-a716-446655440000.log"
}
```

**Status Codes**:
* `200 OK` - Job found
* `404 Not Found` - Job not found

**Example**:
```bash
curl http://localhost:8000/pipeline/jobs/550e8400-e29b-41d4-a716-446655440000
```

---

### 5. List All Jobs

**GET** `/pipeline/jobs`

List all pipeline jobs (most recent first).

**Response**:
```json
{
  "jobs": [
    {
      "job_id": "550e8400-e29b-41d4-a716-446655440000",
      "status": "completed",
      "created_at": "2024-01-15T10:30:00Z",
      "completed_at": "2024-01-15T10:35:00Z"
    },
    {
      "job_id": "660e8400-e29b-41d4-a716-446655440001",
      "status": "running",
      "created_at": "2024-01-15T10:25:00Z",
      "completed_at": null
    }
  ],
  "total": 2
}
```

**Status Codes**:
* `200 OK` - Always

**Example**:
```bash
curl http://localhost:8000/pipeline/jobs
```

---

### 6. Get Job Manifest

**GET** `/pipeline/jobs/{job_id}/manifest`

Retrieve the build manifest for a completed job.

**Path Parameters**:
* `job_id` (string) - UUID of the pipeline job

**Response** (when completed):
```json
{
  "pipeline": {
    "version": "2.0",
    "architecture": "class-based with joblib optimization",
    "seed": 42,
    "n_jobs": -1
  },
  "metrics": {
    "n_events_raw": 1500000,
    "n_events_train": 1200000,
    "n_events_test": 300000,
    "n_users_train": 25000,
    "n_users_test": 6250,
    "n_items_catalog": 15000,
    "n_cold_items": 750,
    "n_null_items": 150
  },
  "timing": {
    "stage_durations_seconds": {
      "Data Extraction": 3.21,
      "Telemetry Augmentation": 5.67,
      "Noise Injection": 8.43,
      "Train/Test Split": 2.15,
      "Implicit Matrix": 12.89,
      "Feature Engineering": 7.34
    },
    "total_duration_seconds": 39.69,
    "total_duration_formatted": "39.69s"
  },
  "outputs": {
    "train_interactions": "data/processed/train_interactions.parquet",
    "test_interactions": "data/processed/test_interactions.parquet",
    "item_features": "data/processed/item_features.parquet",
    "user_features": "data/processed/user_features.parquet",
    "implicit_matrix": "data/processed/implicit_matrix.parquet"
  },
  "built_at": "2024-01-15T10:35:00Z"
}
```

**Status Codes**:
* `200 OK` - Manifest available
* `404 Not Found` - Job not found or manifest not ready yet

**Example**:
```bash
curl http://localhost:8000/pipeline/jobs/550e8400-e29b-41d4-a716-446655440000/manifest
```

---

### 7. Get Job Logs

**GET** `/pipeline/jobs/{job_id}/logs`

Retrieve the last 100 lines of logs for a job.

**Path Parameters**:
* `job_id` (string) - UUID of the pipeline job

**Response**:
```json
{
  "logs": [
    "2024-01-15 10:30:00 | INFO     | Starting pipeline...",
    "2024-01-15 10:30:05 | INFO     | Loading data sources...",
    "2024-01-15 10:35:00 | INFO     | Pipeline completed successfully"
  ],
  "total_lines": 523,
  "showing": 100
}
```

**Status Codes**:
* `200 OK` - Logs available (may be empty if not ready)
* `404 Not Found` - Job not found

**Example**:
```bash
curl http://localhost:8000/pipeline/jobs/550e8400-e29b-41d4-a716-446655440000/logs
```

---

## Workflow Examples

### Example 1: Basic Pipeline Execution

```bash
# 1. Trigger pipeline
RESPONSE=$(curl -s -X POST http://localhost:8000/pipeline/run \
  -H "Content-Type: application/json" \
  -d '{}')

JOB_ID=$(echo $RESPONSE | jq -r '.job_id')
echo "Job ID: $JOB_ID"

# 2. Poll for status
while true; do
  STATUS=$(curl -s http://localhost:8000/pipeline/jobs/$JOB_ID | jq -r '.status')
  echo "Status: $STATUS"
  
  if [ "$STATUS" = "completed" ] || [ "$STATUS" = "failed" ]; then
    break
  fi
  
  sleep 5
done

# 3. Get manifest
if [ "$STATUS" = "completed" ]; then
  curl -s http://localhost:8000/pipeline/jobs/$JOB_ID/manifest | jq .
else
  echo "Pipeline failed!"
  curl -s http://localhost:8000/pipeline/jobs/$JOB_ID/logs | jq -r '.logs[]'
fi
```

### Example 2: Python Client

```python
import requests
import time

BASE_URL = "http://localhost:8000"

# 1. Trigger pipeline
response = requests.post(f"{BASE_URL}/pipeline/run", json={})
data = response.json()
job_id = data["job_id"]
print(f"Job started: {job_id}")

# 2. Wait for completion
while True:
    status_response = requests.get(f"{BASE_URL}/pipeline/jobs/{job_id}")
    status_data = status_response.json()
    status = status_data["status"]
    
    print(f"Status: {status}")
    
    if status in ["completed", "failed"]:
        break
    
    time.sleep(5)

# 3. Get results
if status == "completed":
    manifest = requests.get(f"{BASE_URL}/pipeline/jobs/{job_id}/manifest").json()
    print(f"Pipeline completed in {manifest['timing']['total_duration_formatted']}")
    print(f"Output files: {manifest['outputs']}")
else:
    logs = requests.get(f"{BASE_URL}/pipeline/jobs/{job_id}/logs").json()
    print("Pipeline failed. Last logs:")
    for log in logs["logs"][-10:]:
        print(log)
```

---

## Error Responses

All errors follow this format:

```json
{
  "error": "Error message describing what went wrong",
  "status": "failed",
  "detail": "Optional additional details"
}
```

**Common HTTP Status Codes**:
* `200 OK` - Success
* `202 Accepted` - Async operation started
* `400 Bad Request` - Invalid input
* `404 Not Found` - Resource not found
* `500 Internal Server Error` - Server error

---

## Configuration

### Environment Variables

Set these in `.env` file or export directly:

```bash
# Server configuration
API_HOST=0.0.0.0          # Host to bind
API_PORT=8000             # Port to bind
API_WORKERS=4             # Number of worker processes
DEBUG=false               # Enable debug mode

# Pipeline configuration
DEFAULT_CONFIG_PATH=data_pipeline/configs/pipeline_config.yaml
LOG_LEVEL=INFO
LOG_DIR=logs
```

### CORS Configuration

By default, CORS is enabled for all origins (`*`). For production, restrict in `api_server.py`:

```python
cors_config = CORSConfig(
    allow_origins=["https://yourdomain.com"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)
```

---

## Deployment

### Local Development

```bash
./run.sh setup    # First time only
./run.sh dev      # Start dev server
```

### Production (VPS)

```bash
./run.sh setup
./run.sh prod     # Starts in daemon mode with Gunicorn
```

### Docker

```bash
./run.sh docker   # Build and run with Docker Compose
```

### Systemd Service (VPS)

Create `/etc/systemd/system/pipeline-api.service`:

```ini
[Unit]
Description=Dataset Pipeline API
After=network.target

[Service]
Type=simple
User=your-user
WorkingDirectory=/path/to/project
Environment="PATH=/path/to/project/venv/bin"
ExecStart=/path/to/project/venv/bin/python src/api_server.py
Restart=always

[Install]
WantedBy=multi-user.target
```

Then:
```bash
sudo systemctl enable pipeline-api
sudo systemctl start pipeline-api
sudo systemctl status pipeline-api
```

---

## Monitoring

### Health Check

Use `/health` endpoint for load balancer health checks:

```bash
curl http://localhost:8000/health
```

### Logs

**API Server Logs**:
* Console: stdout/stderr
* File: `logs/api_*.log` (production mode)

**Pipeline Job Logs**:
* Per-job: `logs/pipeline_{job_id}.log`

### Metrics

Consider adding Prometheus metrics for production:
* Request count by endpoint
* Request duration
* Active jobs count
* Job success/failure rates

---

## Security Considerations

### Production Checklist

* [ ] Set `DEBUG=false`
* [ ] Restrict CORS origins
* [ ] Add authentication (API keys, OAuth, etc.)
* [ ] Enable HTTPS (use Nginx reverse proxy)
* [ ] Set up rate limiting
* [ ] Validate all inputs
* [ ] Use environment variables for secrets
* [ ] Enable audit logging
* [ ] Regular security updates

### Example: API Key Authentication

Add to `api_server.py`:

```python
from litestar.middleware import DefineMiddleware

def api_key_middleware(app, request):
    api_key = request.headers.get("X-API-Key")
    if api_key != os.getenv("API_KEY"):
        raise NotAuthorizedException("Invalid API key")

app = Litestar(
    route_handlers=[...],
    middleware=[DefineMiddleware(api_key_middleware)]
)
```

---

## Troubleshooting

### Issue: "Connection refused"

**Solution**: Check if server is running:
```bash
curl http://localhost:8000/health
```

### Issue: Job stuck in "pending"

**Solution**: Check server logs and ensure pipeline dependencies are installed

### Issue: "Config file not found"

**Solution**: Ensure config path is relative to project root:
```bash
ls -la data_pipeline/configs/pipeline_config.yaml
```

### Issue: Out of memory

**Solution**: Reduce `API_WORKERS` or pipeline `n_jobs` in config

---

## Support

For issues, check:
1. API server logs: `logs/api_*.log`
2. Job logs: `logs/pipeline_{job_id}.log`
3. Health endpoint: `/health`

---

## Changelog

### v2.0 (Current)
* Litestar-based API server
* Async job processing
* Comprehensive logging
* Docker support
* Production-ready deployment scripts
