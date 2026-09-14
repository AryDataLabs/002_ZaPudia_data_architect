#!/usr/bin/env python3

__author__     = "Aryanto"
__copyright__  = "Copyright 2026, AryDataLabs/ZaPuDia Series"
__credits__    = ["aryanto"]
__license__    = "GNU_Public"
__version__    = "0.1.1"
__maintainer__ = "Aryanto, M.Si"
__email__      = "aryanto.dandan@gmail.com"
__created__    = "2026-08-31"
__modified__   = "2026-09-13"

from pathlib import Path
BASE_DIR = Path(__file__).resolve().parents[2]
ENV_PATH = BASE_DIR / '.env'
print(ENV_PATH)
try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path = ENV_PATH)
    print('success load dotenv')
except Exception:
    pass


def get_api():
    """Lazy import of API module (requires litestar, uvicorn)."""
    from . import api
    return api

from . import blueprint
from . import configs
from . import extractors
from . import noise
from . import pipeline
from . import transformers
from .dataset_builder import build_dataset

__all__ = ['build_dataset',
           'blueprint',
           'configs',
           'extractors',
           'noise',
           'pipeline',
           'transformers',
           'get_api',]