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

import uuid
import asyncio
from   enum        import Enum
from   typing      import Any
from   dataclasses import dataclass
from   datetime    import datetime, timezone
from ..configs     import logger


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
    """In-memory job store."""

    def __init__(self) -> None:
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

    async def update_job(self, job_id: str, **updates: Any) -> None:
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


# Global job store instance
job_store = JobStore()