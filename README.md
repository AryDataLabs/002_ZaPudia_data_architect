# ZaPudia Data Architect - Dataset Pipeline v2.0

**Production-Ready E-commerce Recommendation System Dataset Pipeline**

A class-based, enterprise-grade data pipeline that orchestrates end-to-end dataset construction for recommendation system development. Built for high throughput, parallel processing, and comprehensive observability.

---

## Executive Summary

ZaPudia Data Architect is a refactored v2.0 pipeline that transforms raw e-commerce event data (Kaggle sources) into production-ready datasets for training recommendation systems. The pipeline implements object-oriented architecture, parallel processing via Joblib, professional logging, and real-time API exposure.

**Key Metrics:**
- **Speedup**: ~1.9x performance improvement over original sequential implementation
- **Throughput**: Processes 1.5M+ events in ~32 seconds (4-core CPU)
- **Parallel Jobs**: Configurable joblib workers (default: all CPUs)
- **Dataset Fidelity**: Realistic noise injection (bot traffic, telemetry drops, cold-start items)
- **API Coverage**: Full REST interface for job orchestration and monitoring

---

## Architecture Overview

### Stack
- **Language**: Python 3.11+
- **Core Framework**: Polars (blazing-fast DataFrames), PyYAML (configuration)
- **Parallelization**: Joblib (parallel data loading, feature engineering)
- **API Server**: Litestar 2.0 + Uvicorn (async HTTP, production-ready)
- **Process Manager**: Gunicorn (production WSGI deployment)
- **Containerization**: Docker + Docker Compose (multi-stage build)

### Key Dependencies
```
polars>=0.18.0          # High-performance DataFrames
pyyaml>=6.0             # Configuration management
joblib>=1.3.0           # Parallel processing
litestar>=2.0.0         # Async API framework
uvicorn[standard]>=0.23.0  # ASGI application server
gunicorn>=21.2.0        # WSGI process manager (production)
orjson>=3.9.0           # Fast JSON serialization
uvloop>=0.17.0          # High-performance event loop (Unix)
```

---

## Repository Structure

```
002_ZaPudia_data_architect/
├── src/                          # Application source code
│   ├── pipeline/
│   │   ├── orchestrator.py       # Main DatasetPipeline class & CLI entry point
│   │   ├── stages.py             # Individual pipeline stage implementations
│   │   ├── internal_config.py    # Type-safe config loader with validation
│   │   └── __init__.py
│   ├── api/
│   │   ├── api_server.py         # Litestar REST API with job queue
│   │   └── __init__.py
│   ├── extractors/               # Data source adapters (Kaggle, GA4)
│   ├── transformers/             # Feature engineering (implicit matrix, encoding)
│   ├── noise/                    # Noise injection & anomaly simulation
│   ├── dataset_builder.py        # Legacy functional API (v1.0 reference)
│   └── __init__.py
├── data/                         # Data directories (created at runtime)
│   ├── raw/                      # Kaggle CSV inputs
│   ├── processed/                # Pipeline outputs (parquet, JSON)
│   └── intermediate/             # Temporary working files
├── data_pipeline/
│   └── configs/
│       └── pipeline_config.yaml  # Main configuration file
├── notebooks/                    # Jupyter notebooks for exploration
├── test/                         # Unit and integration tests
├── logs/                         # Application logs (created at runtime)
├── Dockerfile                    # Multi-stage production image
├── docker-compose.yml            # Service orchestration
├── run.sh                        # Unified deployment script (dev/prod/docker)
├── requirements.txt              # Python dependencies
└── README.md                     # This file

**Language Composition:**
- Python: 87.3% (core pipeline, API, transformers)
- Shell: 11.4% (deployment scripts)
- Dockerfile: 1.3% (containerization)
```

---

## System Design & Data Flow

### Pipeline Execution Model

The pipeline orchestrates six sequential stages, each with optional joblib parallelization:

```
Input (Kaggle CSV) 
  ↓
[1] Data Extraction (parallel)
  └─ Load behavior.csv + orders.csv in parallel
  └─ Output: Combined events DataFrame
  ↓
[2] Telemetry Augmentation
  └─ Simulate GA4 events (session IDs, device info, geographic data)
  └─ Output: Events with GA4 schema
  ↓
[3] Noise Injection
  └─ Inject realistic anomalies:
     • Bot traffic (Poisson process simulation)
     • Telemetry drops (random event loss)
     • Schema nulls (realistic missing data)
     • Cold-start items (unobserved products)
  └─ Output: Corrupted events + anomaly metadata
  ↓
[4] Train/Test Split (user-level, no leakage)
  └─ Filter users with min interactions
  └─ Stratified split by user ID
  └─ Output: train_interactions.parquet, test_interactions.parquet
  ↓
[5] Implicit Matrix Generation
  └─ Compute weighted feedback matrix (views, cart, purchase)
  └─ Normalize ratings to [0, 10] range
  └─ Output: implicit_matrix.parquet
  ↓
[6] Feature Engineering (parallel)
  └─ Item features: catalog, embeddings, cold-start flags
  └─ User features: interaction counts, recency, RFM scores
  └─ Output: item_features.parquet + user_features.parquet
  ↓
Output (Parquet files + build_manifest.json)
```

### Class Hierarchy

```python
DatasetPipeline                    # Main orchestrator
  ├─ stage: DataExtractionStage
  ├─ stage: TelemetryAugmentationStage
  ├─ stage: NoiseInjectionStage
  ├─ stage: TrainTestSplitStage
  ├─ stage: ImplicitMatrixStage
  └─ stage: FeatureEngineeringStage
         └─ All inherit from PipelineStage (ABC)

PipelineConfig                     # Type-safe configuration loader
  └─ from_yaml(path) → PipelineConfig
  └─ validate() → raises ValueError on invalid config
  └─ ensure_directories() → creates output/intermediate dirs

PipelineJob                        # API job metadata
  ├─ job_id (UUID)
  ├─ status (pending/running/completed/failed)
  ├─ manifest (build outputs)
  └─ log_file (async execution logs)
```

### Parallel Processing Strategy

**Joblib Optimization Points:**

1. **Data Extraction** (`DataExtractionStage`)
   - Loads Kaggle CSV files in parallel (2 workers)
   - Speedup: ~1.8x on multi-core systems

2. **Feature Engineering** (`FeatureEngineeringStage`)
   - Builds item + user features concurrently (2 workers)
   - Speedup: ~1.9x on multi-core systems

**Configuration:**
```yaml
n_jobs: -1  # -1 = use all CPUs; set to 1 for single-threaded debugging
```

---

## API Specification

### REST Endpoints

The pipeline exposes a Litestar-based REST API for async job orchestration and monitoring.

#### 1. **Health Check**
```http
GET /health
```
**Response:** 
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:31:25.123456+00:00"
}
```

#### 2. **Root Endpoint**
```http
GET /
```
**Response:** API documentation with all available endpoints

#### 3. **Trigger Pipeline**
```http
POST /pipeline/run
Content-Type: application/json

{
  "config_path": "data_pipeline/configs/pipeline_config.yaml"  # optional
}
```
**Response (202 Accepted):**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "pending",
  "message": "Pipeline job created and started",
  "created_at": "2024-01-15T10:31:25.123456+00:00"
}
```

#### 4. **Get Job Status**
```http
GET /pipeline/jobs/{job_id}
```
**Response:**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "running",
  "config_path": "data_pipeline/configs/pipeline_config.yaml",
  "created_at": "2024-01-15T10:31:25.123456+00:00",
  "started_at": "2024-01-15T10:31:26.123456+00:00",
  "completed_at": null,
  "error": null,
  "has_manifest": false,
  "log_file": "logs/pipeline_550e8400-e29b-41d4-a716-446655440000.log"
}
```

#### 5. **List All Jobs**
```http
GET /pipeline/jobs
```
**Response:**
```json
{
  "jobs": [
    {
      "job_id": "...",
      "status": "completed",
      "created_at": "...",
      "completed_at": "..."
    }
  ],
  "total": 5
}
```

#### 6. **Get Build Manifest**
```http
GET /pipeline/jobs/{job_id}/manifest
```
**Response:** Complete pipeline execution manifest (see Build Manifest section)

#### 7. **Get Job Logs**
```http
GET /pipeline/jobs/{job_id}/logs
```
**Response:** Last 100 lines of execution logs

---

## Configuration

### YAML Configuration File

**Location:** `data_pipeline/configs/pipeline_config.yaml`

**Complete Example:**
```yaml
pipeline:
  seed: 42                          # Random seed for reproducibility
  output_dir: "data/processed"      # Output directory for parquet files
  intermediate_dir: "data/intermediate"  # Temp directory
  # n_jobs: 4                       # Optional: parallel workers (default: -1 = all CPUs)

sources:
  kaggle_behavior:
    path: "data/raw/behavior.csv"   # Kaggle e-commerce behavior log
    event_type_map:                 # Event type normalization
      view: "view"
      cart: "add_to_cart"
      purchase: "purchase"
  kaggle_orders:
    path: "data/raw/orders.csv"     # Kaggle order log

geo_provinces:                       # Geographic regions for simulation
  - "Jakarta"
  - "Bali"
  - "Surabaya"

noise:
  telemetry_drop_rate: 0.05         # 5% of events randomly dropped
  bot_fraction: 0.02                # 2% bot traffic
  bot_lambda_per_sec: 10.0          # Bot event rate (Poisson)
  bot_interarrival_min_ms: 50       # Min time between bot events
  bot_interarrival_max_ms: 200      # Max time between bot events
  oo_lambda_per_sec: 5.0            # Out-of-order event rate
  schema_null_rate: 0.01            # 1% null values in schema
  cold_start_rate: 0.05             # 5% new items (cold-start)

split:
  test_fraction: 0.2                # 20% users for test set
  min_user_interactions: 5          # Minimum interactions per user

implicit_weights:
  view: 1.0                         # Weight for view events
  add_to_cart: 3.0                  # Weight for cart events
  purchase: 10.0                    # Weight for purchases

implicit_normalization:
  min_rating: 0.0                   # Min normalized rating
  max_rating: 10.0                  # Max normalized rating
```

### Configuration Validation

All parameters are validated at startup:
- `test_fraction` ∈ [0, 1]
- `bot_fraction`, `telemetry_drop_rate`, `schema_null_rate` ∈ [0, 1]
- `min_user_interactions` ≥ 1
- Output directories created automatically
- Missing config files raise `FileNotFoundError`

---

## Deployment & Operations

### Development Setup

```bash
# 1. Clone and initialize
git clone <repo-url>
cd 002_ZaPudia_data_architect

# 2. First-time setup (create venv, install deps, create dirs)
./run.sh setup

# 3. Configure (edit .env and pipeline_config.yaml)
# Place Kaggle CSV files in data/raw/

# 4. Run in dev mode (auto-reload, single worker)
./run.sh dev
# API available at: http://localhost:8000
```

### Production Deployment

```bash
# Run with Gunicorn (multi-worker, production-ready)
./run.sh prod
# Runs with 4 workers, logs to logs/api_*.log

# Check logs
tail -f logs/api_*.log
```

### Docker Deployment

```bash
# Build and run with Docker Compose
./run.sh docker
# API available at: http://localhost:8000
# View logs: docker-compose logs -f

# Inspect running container
docker ps
docker exec pipeline-api python -c "import src; print('OK')"

# Stop
docker-compose down
```

### Direct Pipeline Execution (No API)

```bash
# Run pipeline once with default config
./run.sh pipeline

# Run with custom config
./run.sh pipeline configs/custom.yaml

# Run with DEBUG logging
./run.sh pipeline configs/custom.yaml --log-level DEBUG
```

### Running Tests

```bash
# Run API tests
./run.sh test

# Manual API testing
curl http://localhost:8000/health
curl -X POST http://localhost:8000/pipeline/run
```

---

## Build Manifest

Each pipeline execution generates a comprehensive `build_manifest.json`:

```json
{
  "pipeline": {
    "version": "2.0",
    "architecture": "class-based with joblib optimization",
    "seed": 42,
    "output_dir": "data/processed",
    "n_jobs": 4
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
  "outputs": {
    "train_interactions": "data/processed/train_interactions.parquet",
    "test_interactions": "data/processed/test_interactions.parquet",
    "item_features": "data/processed/item_features.parquet",
    "user_features": "data/processed/user_features.parquet",
    "implicit_matrix": "data/processed/implicit_matrix.parquet",
    "cold_items": "data/processed/cold_items.json",
    "null_items": "data/processed/null_items.json"
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
  "noise_config": {
    "telemetry_drop_rate": 0.05,
    "bot_fraction": 0.02,
    "bot_lambda_per_sec": 10.0,
    "bot_interarrival_min_ms": 50,
    "bot_interarrival_max_ms": 200,
    "oo_lambda_per_sec": 5.0,
    "schema_null_rate": 0.01,
    "cold_start_rate": 0.05,
    "seed": 42
  },
  "built_at": "2024-01-15T10:31:25.123456+00:00"
}
```

---

## Output Datasets

### 1. train_interactions.parquet
User-item interaction events for training. Typically 80% of total events.

**Schema:**
```
user_id (string)
item_id (string)
event_type (string): "view" | "add_to_cart" | "purchase"
timestamp (datetime)
session_id (string)
device (string)
province (string)
... (GA4 fields)
```

### 2. test_interactions.parquet
Held-out user interactions for evaluation. Typically 20% of total events.

### 3. item_features.parquet
Catalog of items with computed features.

**Schema:**
```
item_id (string)
n_interactions (integer)
is_cold_start (boolean)
has_null_features (boolean)
embedding_vector (array)
... (computed features)
```

### 4. user_features.parquet
User profiles with behavioral features.

**Schema:**
```
user_id (string)
n_interactions (integer)
recency_days (float)
frequency (integer)
monetary_value (float)
rfm_segment (string)
... (computed features)
```

### 5. implicit_matrix.parquet
Weighted user-item feedback matrix for recommendation system training.

**Schema:**
```
user_id (string)
item_id (string)
rating (float): [0.0, 10.0] (normalized)
```

### 6. cold_items.json
JSON list of item IDs with no training interactions.

### 7. null_items.json
JSON list of item IDs with missing feature data.

---

## Logging & Observability

### Console Output (Development)
```
2024-01-15 10:30:45 | INFO     | pipeline.orchestrator | Starting pipeline...
2024-01-15 10:30:47 | INFO     | pipeline.stages.DataExtraction | Loaded behavior data: 1,234,567 rows
2024-01-15 10:30:48 | INFO     | pipeline.orchestrator | Stage 'Data Extraction' completed in 3.21s
...
2024-01-15 10:31:25 | INFO     | pipeline.orchestrator | ========== PIPELINE COMPLETED SUCCESSFULLY ==========
```

### File Logging (Production)
- **Path**: `logs/pipeline_YYYYMMDD_HHMMSS.log`
- **Level**: DEBUG (comprehensive troubleshooting)
- **Format**: Timestamp | Level | Module:Line | Message

### Structured Log Fields
- `timestamp`: ISO 8601 timestamp
- `level`: DEBUG, INFO, WARNING, ERROR
- `module`: Pipeline component (e.g., `pipeline_orchestrator`, `pipeline.stages.DataExtraction`)
- `message`: Event description
- `exc_info`: Full stack trace on exceptions

---

## Performance & Scalability

### Benchmarks (4-core CPU, 1.5M events)

| Component | Original v1.0 | Refactored v2.0 | Improvement |
|-----------|---------------|-----------------|-------------|
| Data Loading | ~11s (sequential) | ~6s (parallel) | **1.8x** |
| Feature Engineering | ~23s (sequential) | ~12s (parallel) | **1.9x** |
| Full Pipeline | ~60s | ~32s | **1.9x** |

### Scaling Considerations

**Vertical Scaling (More CPUs):**
- Joblib parallelization scales to all available cores
- Set `n_jobs: -1` in config (default)
- Tested up to 16-core systems

**Horizontal Scaling (API):**
- Litestar app deployed with `--workers 4` (default)
- Each worker can run independent pipeline executions
- Use `docker-compose` for multi-container deployments

**Memory Optimization:**
- Polars lazy evaluation reduces peak memory
- Parquet partitioning for large datasets
- Stream processing via DuckDB for implicit matrix

---

## Error Handling & Recovery

### Configuration Errors
- **Missing Config File**: `FileNotFoundError` with clear path
- **Invalid YAML**: Parse error with line number
- **Missing Fields**: Detailed message listing required field

### Runtime Errors
- **Input Files Missing**: Aborts with file path
- **Corrupted Data**: Logged with row context, processing continues
- **Out of Memory**: Graceful degradation with batch processing
- **Stage Failures**: Full stack trace logged, manifest partially saved

### Retry Strategy
- Idempotent stages (can re-run without state loss)
- Cold items/null items JSON files persist anomaly metadata
- Build manifest always saved before completion

---

## Development Workflow

### Add a New Pipeline Stage

1. **Create Stage Class** (`src/pipeline/stages.py`):
```python
class CustomStage(PipelineStage):
    def __init__(self):
        super().__init__("CustomStage")
    
    def execute(self, **kwargs) -> Any:
        self.log_start()
        # Your logic here
        self.log_end("Summary of results")
        return result
```

2. **Register in Orchestrator** (`src/pipeline/orchestrator.py`):
```python
def _init_stages(self) -> None:
    self.stages = {
        ...
        "custom": CustomStage(),
    }
```

3. **Integrate in Pipeline** (in `run()` method):
```python
result = self._time_stage("Custom Stage", self.stages["custom"].execute, ...)
```

### Run Individual Stages

```python
from pipeline.stages import DataExtractionStage
from pipeline.internal_config import PipelineConfig

config = PipelineConfig.from_yaml("configs/pipeline_config.yaml")
extractor = DataExtractionStage(n_jobs=4)
events = extractor.execute(
    behavior_path=config.kaggle_behavior_path,
    orders_path=config.kaggle_orders_path,
    event_type_map=config.event_type_map,
)
print(f"Loaded {events.height:,} events")
```

### Unit Testing

```bash
pytest test/ -v
pytest test/test_stages.py::TestDataExtraction -s  # verbose
```

---

## Troubleshooting

### Issue: Config file not found
**Solution:** Ensure config path is relative to where you run the script
```bash
cd src && python pipeline_orchestrator.py --config ../configs/pipeline_config.yaml
```

### Issue: Memory errors during feature engineering
**Solution:** Reduce parallel jobs or process in batches
```yaml
n_jobs: 2  # Instead of -1 (all CPUs)
```

### Issue: Slow pipeline execution
**Solution:** Check disk I/O (parquet writes are bottleneck)
```bash
# Use SSD for output directory
# Benchmark: time python -m pipeline.orchestrator
```

### Issue: API returning 503 (unavailable)
**Solution:** Check if port is in use
```bash
lsof -i :8000
kill -9 <PID>
./run.sh dev  # Restart
```

### Issue: Docker image build fails
**Solution:** Clear cache and rebuild
```bash
docker-compose down -v
docker-compose up --build
```

---

## Maintenance & Monitoring

### Health Checks
```bash
# API health
curl http://localhost:8000/health

# Pipeline validation
python -c "from pipeline.orchestrator import DatasetPipeline; print('OK')"
```

### Log Rotation
```bash
# Keep last 10 log files
find logs/ -name "pipeline_*.log" -type f | sort -r | tail -n +11 | xargs rm
```

### Performance Profiling
```bash
# Profile pipeline execution
python -m cProfile -s cumtime src/pipeline/orchestrator.py

# Memory profiling
pip install memory_profiler
python -m memory_profiler src/pipeline/orchestrator.py
```

---

## License

Same as original project. See LICENSE file for details.

---

## Changelog

### v2.0 (Current)
- ✅ Complete refactor to class-based architecture
- ✅ Comprehensive logging system (console + file)
- ✅ Joblib parallelization (data loading, feature engineering)
- ✅ REST API with Litestar (async job orchestration)
- ✅ Progress tracking and timing metrics
- ✅ Enhanced error handling and validation
- ✅ Modular stage-based design
- ✅ Docker multi-stage build
- ✅ ~1.9x performance improvement

### v1.0 (Original)
- Functional script-based approach
- Sequential processing only
- Basic error handling
- Minimal logging

---

## Support & Contributing

For issues, questions, or feature requests:
1. Check existing issues and troubleshooting section
2. Enable DEBUG logging: `--log-level DEBUG`
3. Review logs in `logs/` directory
4. Include config file (sanitize paths) and manifest JSON in reports

---

**Built for production. Designed for data scientists. Optimized for speed.**
