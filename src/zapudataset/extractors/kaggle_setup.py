#!/usr/bin/env python3

__author__     = "Aryanto"
__copyright__  = "Copyright 2026, AryDataLabs/ZaPuDia Series"
__credits__    = ["aryanto"]
__license__    = "GNU_Public"
__version__    = "0.0.5"
__maintainer__ = "Aryanto, M.Si"
__email__      = "aryanto.dandan@gmail.com"
__created__    = "2026-08-31"
__modified__   = "2026-10-04"

import os
import json
import polars  as pl
from pathlib   import Path
from typing    import Optional
from tqdm      import tqdm

from ..configs import logger
from kaggle.api.kaggle_api_extended import KaggleApi
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm.auto import tqdm

kaggledir = Path(__file__).resolve().parent


def _convert_single_csv(csv_file_path: str) -> str:
    """Worker function untuk multiprocessing (harus top-level/picklable)."""
    csv_file = Path(csv_file_path)
    parquet_file = csv_file.with_suffix(".parquet")
    
    if not parquet_file.exists():
        # Polars streaming engine sink_parquet
        pl.scan_csv(
            csv_file,
            infer_schema_length=10000,
            ignore_errors=True,
        ).sink_parquet(parquet_file, compression="zstd")
        
        return f"Converted: {csv_file.name} -> {parquet_file.name}"
    return f"Skipped (Already exists): {parquet_file.name}"


def _convert_csv_to_parquet_if_needed(target_path: Path) -> None:
    """Convert downloaded .csv files to .parquet using Multiprocessing + Polars Streaming."""
    csv_files = list(target_path.glob("*.csv"))
    if not csv_files:
        return

    # Batasi worker sesuai jumlah CPU atau jumlah file (misal maks 2 di Google Colab agar RAM tidak OOM)
    cpu_cores = os.cpu_count() or 1
    max_workers = min(len(csv_files), max(1, cpu_cores))

    logger.info(f"Starting parallel CSV conversion with {max_workers} worker process(es)...")

    pbar = tqdm(total=len(csv_files), desc=f"Converting CSV [{target_path.name}]", unit="file")

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit semua task konversi ke pool process
        future_to_file = {
            executor.submit(_convert_single_csv, str(csv_file)): csv_file
            for csv_file in csv_files
        }

        for future in as_completed(future_to_file):
            csv_file = future_to_file[future]
            try:
                msg = future.result()
                logger.info(msg)
            except Exception as e:
                logger.error(f"Failed converting {csv_file.name} to parquet: {e}")
            finally:
                pbar.update(1)

    pbar.close()


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
            "KAGGLE_USERNAME and KAGGLE_KEY exist in environment variables or .env file.")
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
    """Download and extract Kaggle dataset using in-memory auth with download progress bar."""
    success     = False
    target_path = Path(target_dir)
    target_path.mkdir(parents = True, exist_ok = True)
    try:
        if 'KAGGLE_USERNAME' not in os.environ or 'KAGGLE_KEY' not in os.environ:
            if not KaggleSetup():
                return False
        api = KaggleApi()
        api.authenticate()
        
        logger.info(f"Downloading dataset '{address}' to {target_path}...")
        api.dataset_download_files(
            address,
            path  = str(target_path), 
            unzip = unzip,
            quiet = False
        )
        logger.info(f"Done downloading '{address}'")
        
        # Konversi CSV ke Parquet secara otomatis dengan progress bar
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

    # Progress bar untuk memantau unduhan antar dataset ID
    id_pbar = tqdm(target_ids, desc="Kaggle Datasets Pipeline", unit="ds")
    for ds_id in id_pbar:
        if ds_id not in ds_map:
            logger.warning(f"Dataset ID '{ds_id}' not found in {json_file.name}")
            results[ds_id] = False
            continue
        item       = ds_map[ds_id]
        address    = item['address']
        target_dir = Path(base_dir) / item['name']
        
        id_pbar.set_postfix_str(f"ID: {ds_id} ({item['name']})")
        logger.info(f"Processing ID [{ds_id}] -> {item['name']}")
        
        success = KaggleDown(address=address, target_dir=target_dir)
        results[ds_id] = success
    return results


if __name__ == '__main__':
    if KaggleSetup():
        print("Kaggle env vars loaded into memory successfully!")