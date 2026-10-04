#!/usr/bin/env python3

__author__     = "Aryanto"
__copyright__  = "Copyright 2026, AryDataLabs/ZaPuDia Series"
__credits__    = ["aryanto"]
__license__    = "GNU_Public"
__version__    = "0.0.4"
__maintainer__ = "Aryanto, M.Si"
__email__      = "aryanto.dandan@gmail.com"
__created__    = "2026-08-31"
__modified__   = "2026-10-04"

import os
import json
from pathlib   import Path
from typing    import Optional
import polars  as pl

from ..configs import logger
from kaggle.api.kaggle_api_extended import KaggleApi

kaggledir = Path(__file__).resolve().parent


def _convert_csv_to_parquet_if_needed(target_path: Path) -> None:
    """Convert any downloaded .csv files in target_path to .parquet format."""
    csv_files = list(target_path.glob("*.csv"))
    if csv_files:
        logger.info(f"Converting {len(csv_files)} CSV file(s) to Parquet in {target_path}...")
        for csv_file in csv_files:
            parquet_file = csv_file.with_suffix(".parquet")
            if not parquet_file.exists():
                try:
                    df = pl.read_csv(csv_file, infer_schema_length=10000, ignore_errors=True)
                    df.write_parquet(parquet_file)
                    logger.info(f"Converted: {csv_file.name} -> {parquet_file.name}")
                except Exception as e:
                    logger.error(f"Failed converting {csv_file.name} to parquet: {e}")


def KaggleSetup(
        username : Optional[str] = None, 
        token    : Optional[str] = None,
    ) -> bool:
    """Inject Kaggle creds straight into runtime env vars. No disk saving."""
    try:
        username = username or os.getenv('KaggleUsername') or os.getenv('KAGGLE_USERNAME')
        token    = token or os.getenv('KaggleAPItoken') or os.getenv('KAGGLE_KEY')
        if not username or not token:
            logger.error("Kaggle creds missing! Make sure "
            "KAGGLE_USERNAME and KAGGLE_KEY exist in environment variables.")
            return False

        os.environ['KAGGLE_USERNAME'] = username
        os.environ['KAGGLE_KEY'] = token
        logger.info(f"Kaggle API authenticated in-memory for user: {username}")
        return True
    except Exception as Arr:
        logger.error(f"Failed setting up Kaggle env vars: {Arr}")
        return False


def KaggleDown(
        address    : str = 'andrexibiza/grocery-sales-dataset',
        target_dir : str | Path = '.',
        unzip      : bool = True,
    ) -> bool:
    """Download and extract Kaggle dataset using in-memory auth."""
    success     = False
    target_path = Path(target_dir)
    target_path.mkdir(parents = True, exist_ok = True)
    try:
        if 'KAGGLE_USERNAME' not in os.environ or 'KAGGLE_KEY' not in os.environ:
            if not KaggleSetup():
                return False
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files(
            address,
            path  = str(target_path), 
            unzip = unzip)
        logger.info(f"Done downloading '{address}'")
        
        # Otomatis konversi file CSV ke Parquet untuk disesuaikan dengan kaggle_loader
        _convert_csv_to_parquet_if_needed(target_path)
        
        success = True
    except Exception as Arr:
        logger.error(f"Failed pulling '{address}': {Arr}")
        raise ValueError(f"Failed to download dataset {address}: {Arr}")
    finally:
        if target_path.exists():
            files = os.listdir(target_path)
            logger.info(f"Files inside {target_path}: {files}")
    return success


def KG_IDDown(
        dataset_ids : str | list[str],
        json_path   : str | Path = kaggledir / 'kaggle_datasets.json',
        base_dir    : str | Path = 'data/raw'
    ) -> dict[str, bool]:
    """
    Find and download Kaggle dataset(s) by ID from JSON manifest.
    Accepts single ID ('kgurl-01') or list of IDs (['kgurl-01', 'kgurl-02']).
    """
    json_file = Path(json_path)
    if not json_file.exists():
        logger.error(f"Config JSON not found: {json_file.resolve()}")
        return dict()
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except Exception as err:
        logger.error(f"Failed parsing {json_file.name}: {err}")
        return dict()

    ds_map     = {item['id']: item for item in config.get('kaggle_datasets', []) if 'id' in item}
    target_ids = [dataset_ids] if isinstance(dataset_ids, str) else dataset_ids
    results    = dict()
    for ds_id in target_ids:
        if ds_id not in ds_map:
            logger.warning(f"Dataset ID '{ds_id}' not found in {json_file.name}")
            results[ds_id] = False
            continue
        item       = ds_map[ds_id]
        address    = item['address']
        target_dir = Path(base_dir) / item['name']
        logger.info(f"Processing ID [{ds_id}] -> {item['name']}")
        success = KaggleDown(address=address, target_dir=target_dir)
        results[ds_id] = success
    return results


if __name__ == '__main__':
    if KaggleSetup():
        print("Kaggle env vars loaded into memory successfully!")