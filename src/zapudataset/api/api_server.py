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

import os
import uvicorn
from   litestar             import Litestar
from   litestar.logging     import LoggingConfig
from   litestar.config.cors import CORSConfig

from ..configs import logger
from .routes   import (get_job_logs,
                       get_job_manifest,
                       get_job_status,
                       health_check,
                       list_jobs,
                       root,
                       trigger_pipeline,)

cors_config     = CORSConfig(
                  allow_origins=["*"],
                  allow_methods=["GET", "POST"],
                  allow_headers=["*"],)
logging_config  = LoggingConfig(
                  root       = {"level": "INFO", 
                                "handlers": ["console"]},
                  formatters = {"standard": {
                  "format"   : "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"}
                  })
app             = Litestar(route_handlers = [
                  root,
                  health_check,
                  trigger_pipeline,
                  get_job_status,
                  list_jobs,
                  get_job_manifest,
                  get_job_logs,],
                  cors_config    = cors_config,
                  logging_config = logging_config,
                  debug          = os.getenv("DEBUG", "false").lower() == "true",)


if __name__ == "__main__":
    host    = os.getenv("API_HOST", "0.0.0.0")
    port    = int(os.getenv("API_PORT", "8000"))
    workers = int(os.getenv("API_WORKERS", "1"))
    logger.info(f"Starting API server on {host}:{port}")
    uvicorn.run(
        "app:app",
        host     = host,
        port     = port,
        workers  = workers,
        reload   = os.getenv("DEBUG", "false").lower() == "true",
        log_level= "info",)
