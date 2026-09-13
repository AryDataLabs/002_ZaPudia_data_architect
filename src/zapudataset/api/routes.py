#!/usr/bin/env python3

__author__     = "Aryanto"
__copyright__  = "Copyright 2026, AryDataLabs/ZaPuDia Series"
__credits__    = ["aryanto"]
__license__    = "GNU_Public"
__version__    = "0.0.1"
__maintainer__ = "Aryanto, M.Si"
__email__      = "aryanto.dandan@gmail.com"
__created__    = "2026-08-31"
__modified__   = "2026-09-13"

import asyncio
from   pathlib               import Path
from   typing                import Any
from   datetime              import datetime, timezone
from   litestar              import Response, get, post
from   litestar.exceptions   import NotFoundException
from   litestar.status_codes import HTTP_202_ACCEPTED, HTTP_404_NOT_FOUND

from ..configs  import logger
from ..pipeline import DatasetPipeline
from .apimod    import JobStatus, job_store


async def run_pipeline_async(job_id: str, config_path: str) -> None:
    """Run pipeline in background task worker."""
    try:
        await job_store.update_job(
            job_id,
            status=JobStatus.RUNNING,
            started_at=datetime.now(timezone.utc).isoformat(),
        )

        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        log_file = log_dir / f"pipeline_{job_id}.log"

        await job_store.update_job(job_id, log_file=str(log_file))

        logger.info(f"Starting pipeline for job {job_id}")

        def run_sync():
            pipeline = DatasetPipeline(
                config_path=config_path,
                log_level="INFO",
                log_file=log_file,
            )
            return pipeline.run()

        loop = asyncio.get_event_loop()
        manifest = await loop.run_in_executor(None, run_sync)

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


@get("/")
async def root() -> dict[str, Any]:
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
async def trigger_pipeline(data: dict[str, Any]) -> Response[dict[str, Any]]:
    """Trigger a new pipeline run."""
    config_path = data.get(
        "config_path",
        "data_pipeline/configs/pipeconf.yaml"
    )

    if not Path(config_path).exists():
        return Response(
            content={
                "error": f"Config file not found: {config_path}",
                "status": "failed",
            },
            status_code=HTTP_404_NOT_FOUND,
        )

    job = await job_store.create_job(config_path)
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
    """Get status details for a specific job."""
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
    """List all registered pipeline jobs."""
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
    """Retrieve full pipeline build manifest."""
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
    """Retrieve tail of execution logs."""
    job = await job_store.get_job(job_id)
    if not job:
        raise NotFoundException(f"Job {job_id} not found")

    if not job.log_file or not Path(job.log_file).exists():
        return {
            "logs": [],
            "message": "Log file not available yet",
        }

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