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
from pathlib import Path
from datetime import datetime
from configparser import ConfigParser

def logconfig():
    # 1. Resolve Paths
    base_dir    = Path(__file__).resolve().parents[0]
    date_str    = datetime.now().strftime('%Y%m%d')
    log_dir     = str(base_dir.parents[1]/'artifacts'/'logs'/date_str)
    os.makedirs(log_dir, exist_ok=True)
    config      = ConfigParser()
    config.read(os.path.join(base_dir, 'configuration.ini'))
    log_name    = config.get('DEFAULT', 'LOG_FILE', fallback='app.log')
    log_path    = os.path.join(log_dir, log_name)
    #log_format = '%(asctime)s - %(filename)s - %(levelname)s - %(message)s'
    log_format  = '%(asctime)s - %(filename)s - %(funcName)s - %(message)s'
    _log_level  = config.get("logging", "level", fallback="DEBUG")
    logging.basicConfig(
        level   = getattr(logging, _log_level, logging.DEBUG),
        format  = log_format,
        handlers= [logging.FileHandler(log_path),
                   logging.StreamHandler(sys.stdout)])
    logging.getLogger('matplotlib').setLevel(logging.WARNING)
    logging.getLogger('graphviz').setLevel(logging.WARNING)
    return logging.getLogger(__name__)

if __name__ == "__main__":
    pass