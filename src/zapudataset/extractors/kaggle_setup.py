#!/usr/bin/env python3

__author__     = "Aryanto"
__copyright__  = "Copyright 2026, AryDataLabs/ZaPuDia Series"
__credits__    = ["aryanto"]
__license__    = "GNU_Public"
__version__    = "0.0.1"
__maintainer__ = "Aryanto, M.Si"
__email__      = "aryanto.dandan@gmail.com"
__created__    = "2026-08-31"
__modified__   = "2026-09-12"

import os
import json
from pathlib import Path
from ..configs import logger

def KaggleSetup(username, Token):
    Success = False
    try :
        auth = {'username': username, 'key': Token}
        ConfDir = Path.home() / '.config' / 'kaggle'
        ConfDir.mkdir(parents=True, exist_ok=True)
        Dest = ConfDir / 'kaggle.json'
        with open(Dest, 'w') as thefile:
            json.dump(auth, thefile)
        os.chmod(ConfDir, 0o600)
        logger.debug(f'Kaggle API Dir: {ConfDir}')
        Success = True
    except Exception as Arr:
        logger.error(f'Failed : {Arr}')
    finally:
        return Success

def KaggleDown(address : str = 'andrexibiza/grocery-sales-dataset'):
    Success = False
    try :
        from kaggle.api.kaggle_api_extended import KaggleApi
        api = KaggleApi()
        api.authenticate()
        logger.debug(f'Downloading {address} ...', end = '\r')
        api.dataset_download_files(address, path='.', unzip=True)
        logger.debug('Download Success')
        Success = True
    except Exception as Arr:
        logger.error(f'Failed : {Arr}')
    finally:
        logger.info('Files:', os.listdir('.'))
        return Success

if __name__ == '__main__':
    pass
