"""Litestar API Server for Dataset Pipeline.

Provides REST API endpoints to:
- Trigger pipeline execution
- Check pipeline status
- Retrieve build manifests
- Health checks

Run with: python api_server.py
Or with uvicorn: uvicorn api_server:app --host 0.0.0.0 --port 8000
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from litestar import Litestar, Request, Response, get, post
from litestar.config.cors import CORSConfig
from litestar.datastructures import State
from litestar.exceptions import NotFoundException
from litestar.logging import LoggingConfig
from litestar.status_codes import HTTP_200_OK, HTTP_202_ACCEPTED, HTTP_404_NOT_FOUND

from pipeline_orchestrator import DatasetPipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


class JobStatus(str, Enum):
    """Pipeline job status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class PipelineJob:
    """Pipeline job metadata."""
    job_id: str
    status: JobStatus
    config_path: str
    created_at: str
    started_at: str | None = None
    completed_at: str | None = None
    error: str | None = None
    manifest: dict[str, Any] | None = None
    log_file: str | None = None


class JobStore:
    """In-memory job store (use Redis/DB for production)."""
    
    def __init__(self):
        self.jobs: dict[str, PipelineJob] = {}
        self._lock = asyncio.Lock()
    
    async def create_job(self, config_path: str) -> PipelineJob:
        """Create a new pending job."""
        async with self._lock:
            job_id = str(uuid.uuid4())
            job = PipelineJob(
                job_id=job_id,
                status=JobStatus.PENDING,
                config_path=config_path,
                created_at=datetime.now(timezone.utc).isoformat(),
            )
            self.jobs[job_id] = job
            logger.info(f"Created job {job_id}")
            return job
    
    async def get_job(self, job_id: str) -> PipelineJob | None:
        """Get job by ID."""
        return self.jobs.get(job_id)
    
    async def update_job(self, job_id: str, **updates) -> None:
        """Update job fields."""
        async with self._lock:
            if job_id in self.jobs:
                job = self.jobs[job_id]
                for key, value in updates.items():
                    setattr(job, key, value)
                logger.debug(f"Updated job {job_id}: {updates}")
    
    async def list_jobs(self) -> list[PipelineJob]:
        """List all jobs."""
        return list(self.jobs.values())


# Global job store
job_store = JobStore()


async def run_pipeline_async(job_id: str, config_path: str) -> None:
    """Run pipeline in background."""
    try:
        # Update to running
        await job_store.update_job(
            job_id,
            status=JobStatus.RUNNING,
            started_at=datetime.now(timezone.utc).isoformat(),
        )
        
        # Setup log file
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        log_file = log_dir / f"pipeline_{job_id}.log"
        
        await job_store.update_job(job_id, log_file=str(log_file))
        
        # Run pipeline (synchronous, in thread pool)
        logger.info(f"Starting pipeline for job {job_id}")
        
        def run_sync():
            pipeline = DatasetPipeline(
                config_path=config_path,
                log_level="INFO",
                log_file=log_file,
            )
            return pipeline.run()
        
        # Run in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        manifest = await loop.run_in_executor(None, run_sync)
        
        # Update to completed
        await job_store.update_job(
            job_id,
            status=JobStatus.COMPLETED,
            completed_at=datetime.now(timezone.utc).isoformat(),
            manifest=manifest,
        )
        
        logger.info(f"Pipeline completed for job {job_id}")
        
    except Exception as e:
        logger.error(f"Pipeline failed for job {job_id}: {e}", exc_info=True)
        await job_store.update_job(
            job_id,
            status=JobStatus.FAILED,
            completed_at=datetime.now(timezone.utc).isoformat(),
            error=str(e),
        )


# ============================================================================
# API ENDPOINTS
# ============================================================================

@get("/")
async def root() -> dict[str, str]:
    """Root endpoint with API info."""
    return {
        "service": "Dataset Pipeline API",
        "version": "2.0",
        "status": "running",
        "endpoints": {
            "health": "GET /health",
            "trigger": "POST /pipeline/run",
            "status": "GET /pipeline/jobs/{job_id}",
            "list": "GET /pipeline/jobs",
            "manifest": "GET /pipeline/jobs/{job_id}/manifest",
            "logs": "GET /pipeline/jobs/{job_id}/logs",
        },
    }


@get("/health")
async def health_check() -> dict[str, str]:
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@post("/pipeline/run")
async def trigger_pipeline(
    data: dict[str, Any],
) -> Response[dict[str, Any]]:
    """Trigger a new pipeline run.
    
    Request body:
    {
        "config_path": "path/to/config.yaml"  # optional, defaults to standard path
    }
    
    Returns:
    {
        "job_id": "uuid",
        "status": "pending",
        "message": "Pipeline job created"
    }
    """
    # Get config path from request or use default
    config_path = data.get(
        "config_path",
        "data_pipeline/configs/pipeline_config.yaml"
    )
    
    # Validate config exists
    if not Path(config_path).exists():
        return Response(
            content={
                "error": f"Config file not found: {config_path}",
                "status": "failed",
            },
            status_code=HTTP_404_NOT_FOUND,
        )
    
    # Create job
    job = await job_store.create_job(config_path)
    
    # Start pipeline in background
    asyncio.create_task(run_pipeline_async(job.job_id, config_path))
    
    return Response(
        content={
            "job_id": job.job_id,
            "status": job.status.value,
            "message": "Pipeline job created and started",
            "created_at": job.created_at,
        },
        status_code=HTTP_202_ACCEPTED,
    )


@get("/pipeline/jobs/{job_id:str}")
async def get_job_status(job_id: str) -> dict[str, Any]:
    """Get job status.
    
    Returns:
    {
        "job_id": "uuid",
        "status": "running|completed|failed",
        "created_at": "ISO timestamp",
        "started_at": "ISO timestamp",
        "completed_at": "ISO timestamp",
        "error": "error message if failed"
    }
    """
    job = await job_store.get_job(job_id)
    
    if not job:
        raise NotFoundException(f"Job {job_id} not found")
    
    return {
        "job_id": job.job_id,
        "status": job.status.value,
        "config_path": job.config_path,
        "created_at": job.created_at,
        "started_at": job.started_at,
        "completed_at": job.completed_at,
        "error": job.error,
        "has_manifest": job.manifest is not None,
        "log_file": job.log_file,
    }


@get("/pipeline/jobs")
async def list_jobs() -> dict[str, Any]:
    """List all pipeline jobs.
    
    Returns:
    {
        "jobs": [
            {"job_id": "...", "status": "...", ...},
            ...
        ],
        "total": 5
    }
    """
    jobs = await job_store.list_jobs()
    
    return {
        "jobs": [
            {
                "job_id": job.job_id,
                "status": job.status.value,
                "created_at": job.created_at,
                "completed_at": job.completed_at,
            }
            for job in sorted(jobs, key=lambda j: j.created_at, reverse=True)
        ],
        "total": len(jobs),
    }


@get("/pipeline/jobs/{job_id:str}/manifest")
async def get_job_manifest(job_id: str) -> dict[str, Any]:
    """Get job build manifest.
    
    Returns the full pipeline manifest with metrics, outputs, timing, etc.
    """
    job = await job_store.get_job(job_id)
    
    if not job:
        raise NotFoundException(f"Job {job_id} not found")
    
    if not job.manifest:
        return Response(
            content={
                "error": "Manifest not available yet",
                "status": job.status.value,
            },
            status_code=HTTP_404_NOT_FOUND,
        )
    
    return job.manifest


@get("/pipeline/jobs/{job_id:str}/logs")
async def get_job_logs(job_id: str) -> dict[str, Any]:
    """Get job logs.
    
    Returns the last N lines of the log file.
    """
    job = await job_store.get_job(job_id)
    
    if not job:
        raise NotFoundException(f"Job {job_id} not found")
    
    if not job.log_file or not Path(job.log_file).exists():
        return {
            "logs": [],
            "message": "Log file not available yet",
        }
    
    # Read last 100 lines
    try:
        with open(job.log_file, "r") as f:
            lines = f.readlines()
            last_lines = lines[-100:] if len(lines) > 100 else lines
        
        return {
            "logs": [line.rstrip() for line in last_lines],
            "total_lines": len(lines),
            "showing": len(last_lines),
        }
    except Exception as e:
        return {
            "error": f"Failed to read logs: {e}",
            "logs": [],
        }


# ============================================================================
# APP CONFIGURATION
# ============================================================================

cors_config = CORSConfig(
    allow_origins=["*"],  # In production, restrict this
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

logging_config = LoggingConfig(
    root={"level": "INFO", "handlers": ["console"]},
    formatters={
        "standard": {
            "format": "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
        }
    },
)

app = Litestar(
    route_handlers=[
        root,
        health_check,
        trigger_pipeline,
        get_job_status,
        list_jobs,
        get_job_manifest,
        get_job_logs,
    ],
    cors_config=cors_config,
    logging_config=logging_config,
    debug=os.getenv("DEBUG", "false").lower() == "true",
)


if __name__ == "__main__":
    import uvicorn
    
    # Get config from environment
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8000"))
    workers = int(os.getenv("API_WORKERS", "1"))
    
    logger.info(f"Starting API server on {host}:{port}")
    
    uvicorn.run(
        "api_server:app",
        host=host,
        port=port,
        workers=workers,
        reload=os.getenv("DEBUG", "false").lower() == "true",
        log_level="info",
    )
