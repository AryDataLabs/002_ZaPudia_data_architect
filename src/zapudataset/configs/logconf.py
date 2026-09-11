#!/usr/bin/env python3

__author__     = "Aryanto"
__copyright__  = "Copyright 2026, AryDataLabs/ZaPuDia Series"
__credits__    = ["aryanto"]
__license__    = "GNU_Public"
__version__    = "0.0.1"
__maintainer__ = "Aryanto, M.Si"
__email__      = "aryanto.dandan@gmail.com"
__created__    = "2026-08-31"
__modified__   = "2026-09-07"

'''
Module Name : Logging General usage
Description : Handles the logging area and configs.
compiler    : python 3.13
'''

import os
import sys
import logging
from pathlib     import Path
from datetime    import datetime
from .confreader import load_config, ConfigError
from .schemaread import PipeConfig

base_dir = Path(__file__).resolve().parents[0]

def logconfig(config_path: str = None):
    """
    Configure logging using schema-validated YAML configuration.
    config_path: Path to YAML config file. Defaults to 'pipeconf.yaml' in same directory.
    """
    if config_path is None:
        config_path = base_dir / 'pipeconf.yaml'
    try:
        cfg: PipeConfig = load_config(str(config_path), schema=PipeConfig)
        log_config = cfg.logging
    except ConfigError as e:
        print(f"WARNING: Failed to load config from {config_path}: {e}")
        print("Using default logging configuration.")
        log_config = None
    date_str    = datetime.now().strftime('%Y%m%d')
    log_dir     = str(base_dir.parents[2]/'artifacts'/'logs'/date_str)
    Path(log_dir).mkdir(parents = True, exist_ok = True)
    log_name    = log_config.log_file if log_config else 'app.log'
    log_level   = log_config.level if log_config else 'INFO'
    log_path    = str(Path(log_dir) / log_name)
    log_format  = '%(asctime)s - %(filename)s - %(funcName)s - %(message)s'
    logging.basicConfig(
        level   = getattr(logging, log_level, logging.INFO),
        format  = log_format,
        handlers= [logging.FileHandler(log_path),
                   logging.StreamHandler(sys.stdout)])
    logging.getLogger('matplotlib').setLevel(logging.WARNING)
    logging.getLogger('graphviz').setLevel(logging.WARNING)
    logger = logging.getLogger(__name__)
    logger.info(f"Logging configured: level = {log_level}, file = {log_path}")
    return logger

if __name__ == "__main__":
    pass