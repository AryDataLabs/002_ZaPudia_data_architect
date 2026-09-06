# Dataset Pipeline API - Deployment Guide

## 🚀 Quick Start (3 Steps)

```bash
# 1. Setup
./run.sh setup

# 2. Configure (edit .env with your settings)
cp .env.example .env
nano .env

# 3. Run
./run.sh dev     # Development mode
# OR
./run.sh prod    # Production mode
```

API will be available at `http://localhost:8000`

---

## 📁 Project Structure

```
.
├── run.sh                          # Main deployment script
├── requirements.txt                # Python dependencies
├── .env.example                    # Environment template
├── docker-compose.yml              # Docker configuration
├── Dockerfile                      # Docker image
│
├── src/
│   ├── api_server.py              # Litestar API server
│   ├── pipeline_orchestrator.py   # Main pipeline class
│   ├── pipeline_config.py         # Configuration management
│   ├── pipeline_stages.py         # Stage implementations
│   ├── dataset_builder.py         # Original script (legacy)
│   └── run_pipeline_example.py    # Usage examples
│
├── data_pipeline/
│   └── configs/
│       └── pipeline_config.yaml   # Pipeline configuration
│
├── data/
│   ├── raw/                       # Input data
│   ├── processed/                 # Output data
│   └── intermediate/              # Temp data
│
├── logs/                          # All logs
│
├── API_DOCUMENTATION.md           # API reference
├── DEPLOYMENT_GUIDE.md            # This file
└── test_api_client.py             # API test client
```

---

## 🛠️ Installation

### Prerequisites

* **Python 3.8+** (3.11 recommended)
* **pip** (latest version)
* **Virtual environment** (venv)
* Optional: **Docker** & **Docker Compose** (for containerized deployment)

### System Dependencies

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install -y python3 python3-pip python3-venv build-essential
```

**CentOS/RHEL:**
```bash
sudo yum install -y python3 python3-pip python3-devel gcc
```

**macOS:**
```bash
brew install python@3.11
```

### Setup

```bash
# Clone/navigate to project
cd /path/to/project

# Run setup script (installs everything)
./run.sh setup

# OR manual setup:
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt

# Create directories
mkdir -p logs data/{raw,processed,intermediate} data_pipeline/configs

# Copy and configure environment
cp .env.example .env
```

---

## ⚙️ Configuration

### 1. Environment Variables

Edit `.env` file:

```bash
# API Server
API_HOST=0.0.0.0        # Bind to all interfaces
API_PORT=8000           # API port
API_WORKERS=4           # Worker processes (CPU count recommended)
DEBUG=false             # Debug mode (true for dev)

# Pipeline
DEFAULT_CONFIG_PATH=data_pipeline/configs/pipeline_config.yaml
LOG_LEVEL=INFO
LOG_DIR=logs
```

### 2. Pipeline Configuration

Edit `data_pipeline/configs/pipeline_config.yaml`:

```yaml
pipeline:
  seed: 42
  output_dir: "data/processed"
  intermediate_dir: "data/intermediate"

sources:
  kaggle_behavior:
    path: "data/raw/behavior.csv"
    event_type_map:
      view: "view"
      cart: "add_to_cart"
      purchase: "purchase"
  kaggle_orders:
    path: "data/raw/orders.csv"

# ... rest of config
```

---

## 🚀 Deployment Options

### Option 1: Local Development

**Best for**: Testing, development

```bash
./run.sh dev
```

* Single worker
* Auto-reload on code changes
* Debug mode enabled
* Console logging only

**Access**: `http://localhost:8000`

---

### Option 2: Production VPS

**Best for**: AWS EC2, DigitalOcean, Linode, dedicated servers

#### A. Quick Start (Foreground)

```bash
./run.sh prod
```

#### B. Background (Daemon)

Modify `run.sh` or run manually:

```bash
source venv/bin/activate
export PYTHONPATH="$(pwd)/src:$PYTHONPATH"

cd src && gunicorn api_server:app \
  --workers 4 \
  --worker-class uvicorn.workers.UvicornWorker \
  --bind 0.0.0.0:8000 \
  --access-logfile ../logs/api_access.log \
  --error-logfile ../logs/api_error.log \
  --daemon
```

#### C. Systemd Service (Recommended)

Create `/etc/systemd/system/pipeline-api.service`:

```ini
[Unit]
Description=Dataset Pipeline API
After=network.target

[Service]
Type=simple
User=your-username
WorkingDirectory=/path/to/project
Environment="PATH=/path/to/project/venv/bin"
Environment="PYTHONPATH=/path/to/project/src"
ExecStart=/path/to/project/venv/bin/gunicorn api_server:app \
  --workers 4 \
  --worker-class uvicorn.workers.UvicornWorker \
  --bind 0.0.0.0:8000 \
  --access-logfile /path/to/project/logs/api_access.log \
  --error-logfile /path/to/project/logs/api_error.log
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable pipeline-api
sudo systemctl start pipeline-api
sudo systemctl status pipeline-api

# View logs
journalctl -u pipeline-api -f

# Stop/restart
sudo systemctl stop pipeline-api
sudo systemctl restart pipeline-api
```

#### D. Nginx Reverse Proxy (Production)

Create `/etc/nginx/sites-available/pipeline-api`:

```nginx
upstream pipeline_api {
    server 127.0.0.1:8000 fail_timeout=0;
}

server {
    listen 80;
    server_name api.yourdomain.com;
    
    # Redirect to HTTPS
    return 301 https://$server_name$request_uri;
}

server {
    listen 443 ssl http2;
    server_name api.yourdomain.com;
    
    # SSL certificates (use certbot)
    ssl_certificate /etc/letsencrypt/live/api.yourdomain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/api.yourdomain.com/privkey.pem;
    
    # Security headers
    add_header X-Frame-Options "SAMEORIGIN" always;
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-XSS-Protection "1; mode=block" always;
    
    # Logging
    access_log /var/log/nginx/pipeline-api-access.log;
    error_log /var/log/nginx/pipeline-api-error.log;
    
    # Proxy settings
    location / {
        proxy_pass http://pipeline_api;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # Timeouts for long-running pipelines
        proxy_connect_timeout 600;
        proxy_send_timeout 600;
        proxy_read_timeout 600;
        send_timeout 600;
    }
    
    # Health check endpoint
    location /health {
        proxy_pass http://pipeline_api/health;
        access_log off;
    }
}
```

Enable and test:

```bash
sudo ln -s /etc/nginx/sites-available/pipeline-api /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx

# Get SSL certificate
sudo certbot --nginx -d api.yourdomain.com
```

---

### Option 3: Docker

**Best for**: Consistent environments, easy scaling

#### A. Docker Compose (Recommended)

```bash
# Build and start
./run.sh docker

# OR manually
docker-compose up -d --build

# View logs
docker-compose logs -f

# Stop
docker-compose down

# Restart
docker-compose restart
```

**Access**: `http://localhost:8000`

#### B. Docker Only

```bash
# Build
docker build -t pipeline-api .

# Run
docker run -d \
  --name pipeline-api \
  -p 8000:8000 \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/logs:/app/logs \
  -e API_WORKERS=4 \
  pipeline-api

# View logs
docker logs -f pipeline-api

# Stop
docker stop pipeline-api
docker rm pipeline-api
```

---

### Option 4: Serverless (AWS Lambda + API Gateway)

**Best for**: Low traffic, cost optimization, auto-scaling

#### Using Zappa

1. **Install Zappa**:
   ```bash
   pip install zappa
   ```

2. **Initialize**:
   ```bash
   zappa init
   ```

3. **Configure `zappa_settings.json`**:
   ```json
   {
       "production": {
           "app_function": "src.api_server.app",
           "aws_region": "us-east-1",
           "runtime": "python3.11",
           "s3_bucket": "your-zappa-bucket",
           "timeout_seconds": 900,
           "memory_size": 3008,
           "environment_variables": {
               "DEBUG": "false",
               "API_WORKERS": "1"
           }
       }
   }
   ```

4. **Deploy**:
   ```bash
   zappa deploy production
   # OR update existing
   zappa update production
   ```

5. **Get URL**:
   ```bash
   zappa status production
   ```

**Note**: Lambda has limitations:
- 15-minute timeout (pipelines must complete faster)
- 10GB memory max
- Read-only filesystem (except `/tmp`)

Consider using Step Functions for long-running pipelines.

---

### Option 5: Kubernetes

**Best for**: Large scale, high availability, microservices

#### Deployment YAML

`k8s/deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: pipeline-api
spec:
  replicas: 3
  selector:
    matchLabels:
      app: pipeline-api
  template:
    metadata:
      labels:
        app: pipeline-api
    spec:
      containers:
      - name: api
        image: your-registry/pipeline-api:latest
        ports:
        - containerPort: 8000
        env:
        - name: API_WORKERS
          value: "4"
        - name: DEBUG
          value: "false"
        resources:
          requests:
            memory: "2Gi"
            cpu: "1000m"
          limits:
            memory: "4Gi"
            cpu: "2000m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 10
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 5
          periodSeconds: 10
---
apiVersion: v1
kind: Service
metadata:
  name: pipeline-api
spec:
  selector:
    app: pipeline-api
  ports:
  - port: 80
    targetPort: 8000
  type: LoadBalancer
```

Deploy:

```bash
kubectl apply -f k8s/deployment.yaml
kubectl get services
```

---

## 🧪 Testing

### 1. Health Check

```bash
curl http://localhost:8000/health
```

Expected:
```json
{"status": "healthy", "timestamp": "..."}
```

### 2. Run Test Suite

```bash
# Quick tests (no pipeline execution)
python test_api_client.py

# Full test with pipeline
python test_api_client.py --pipeline

# Custom URL
python test_api_client.py --url http://api.example.com
```

### 3. Manual API Test

```bash
# Trigger pipeline
curl -X POST http://localhost:8000/pipeline/run \
  -H "Content-Type: application/json" \
  -d '{}'

# Get job status (replace JOB_ID)
curl http://localhost:8000/pipeline/jobs/JOB_ID

# List all jobs
curl http://localhost:8000/pipeline/jobs
```

### 4. Load Testing

Using Apache Bench:

```bash
# 1000 requests, 10 concurrent
ab -n 1000 -c 10 http://localhost:8000/health
```

Using Locust:

```python
# locustfile.py
from locust import HttpUser, task

class PipelineUser(HttpUser):
    @task
    def health_check(self):
        self.client.get("/health")
```

Run:
```bash
locust -f locustfile.py --host=http://localhost:8000
```

---

## 📊 Monitoring

### Logs

**API Logs**:
```bash
# Development (console)
tail -f logs/api_error.log

# Production (file)
tail -f logs/api_access.log
tail -f logs/api_error.log

# Systemd
journalctl -u pipeline-api -f

# Docker
docker-compose logs -f api
```

**Pipeline Job Logs**:
```bash
# Via API
curl http://localhost:8000/pipeline/jobs/JOB_ID/logs

# Direct file
tail -f logs/pipeline_JOB_ID.log
```

### Metrics (Optional)

Add Prometheus metrics to `api_server.py`:

```python
from prometheus_client import Counter, Histogram, generate_latest

REQUEST_COUNT = Counter('api_requests_total', 'Total requests')
REQUEST_DURATION = Histogram('api_request_duration_seconds', 'Request duration')

@get("/metrics")
def metrics():
    return Response(generate_latest(), media_type="text/plain")
```

---

## 🔒 Security

### Production Checklist

* [ ] Set `DEBUG=false`
* [ ] Use HTTPS (Nginx + Let's Encrypt)
* [ ] Add authentication (API keys, JWT, OAuth)
* [ ] Restrict CORS origins
* [ ] Enable rate limiting
* [ ] Use environment variables for secrets
* [ ] Regular security updates
* [ ] Firewall configuration
* [ ] Log rotation
* [ ] Backup strategy

### API Key Authentication

Add to `api_server.py`:

```python
import os
from litestar import Request
from litestar.exceptions import NotAuthorizedException

async def auth_middleware(request: Request, handler):
    api_key = request.headers.get("X-API-Key")
    if api_key != os.getenv("API_KEY"):
        raise NotAuthorizedException("Invalid API key")
    return await handler(request)

app = Litestar(
    route_handlers=[...],
    middleware=[auth_middleware]
)
```

Usage:
```bash
curl -H "X-API-Key: your-secret-key" http://localhost:8000/pipeline/run
```

### Rate Limiting

Use Nginx:

```nginx
limit_req_zone $binary_remote_addr zone=api_limit:10m rate=10r/s;

location / {
    limit_req zone=api_limit burst=20 nodelay;
    proxy_pass http://pipeline_api;
}
```

---

## 🐛 Troubleshooting

### Issue: "Connection refused"

**Cause**: Server not running

**Solution**:
```bash
# Check if running
curl http://localhost:8000/health

# Check processes
ps aux | grep "api_server\|uvicorn\|gunicorn"

# Restart
./run.sh stop
./run.sh prod
```

### Issue: "Module not found"

**Cause**: Dependencies not installed or PYTHONPATH not set

**Solution**:
```bash
# Reinstall dependencies
source venv/bin/activate
pip install -r requirements.txt

# Set PYTHONPATH
export PYTHONPATH="$(pwd)/src:$PYTHONPATH"
```

### Issue: Pipeline job stuck in "pending"

**Cause**: Worker crashed or config error

**Solution**:
```bash
# Check logs
tail -f logs/api_error.log

# Check job logs
curl http://localhost:8000/pipeline/jobs/JOB_ID/logs

# Restart server
./run.sh stop
./run.sh prod
```

### Issue: Out of memory

**Cause**: Too many workers or large dataset

**Solution**:
```bash
# Reduce workers
export API_WORKERS=2

# Reduce pipeline parallelism
# Edit pipeline_config.yaml: n_jobs: 2

# Increase system memory or use swap
```

### Issue: Slow pipeline execution

**Solution**:
```bash
# Check system resources
top
df -h

# Increase parallel jobs
# Edit pipeline_config.yaml: n_jobs: -1  # Use all CPUs

# Use SSD for data directories
# Check logs for bottlenecks
```

---

## 📚 Additional Resources

* [API Documentation](API_DOCUMENTATION.md) - Complete API reference
* [Pipeline README](src/PIPELINE_README.md) - Pipeline architecture
* [Litestar Docs](https://litestar.dev/) - Web framework documentation
* [Uvicorn Docs](https://www.uvicorn.org/) - ASGI server documentation
* [Gunicorn Docs](https://docs.gunicorn.org/) - Production server
* [Docker Docs](https://docs.docker.com/) - Containerization

---

## 🆘 Support

For issues:

1. Check logs: `logs/api_*.log`
2. Check job logs: `logs/pipeline_*.log`
3. Test health: `curl http://localhost:8000/health`
4. Run test suite: `python test_api_client.py`

---

## 📝 License

Same as original project.
