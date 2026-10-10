#!/usr/bin/env python3

__author__     = "Aryanto"
__copyright__  = "Copyright 2026, AryDataLabs/ZaPuDia Series"
__credits__    = ["aryanto"]
__license__    = "GNU_Public"
__version__    = "0.0.2"
__maintainer__ = "Aryanto, M.Si"
__email__      = "aryanto.dandan@gmail.com"
__created__    = "2026-08-31"
__modified__   = "2026-10-05"


"""
End-to-end dataset builder orchestrator with comprehensive logging.
________________________________________
Wires extractors -> noise -> implicit builder -> feature encoder -> split ->
partitioned parquet exports. Single entry point for the whole pipeline.
"""

import json
import yaml
import polars as pl
from   dataclasses import asdict
from   datetime    import datetime, timezone
from   pathlib     import Path
from   typing      import Any
from   copy        import deepcopy

from .noise        import NoiseConfig, inject_all
from .configs      import logger, configure
from .extractors   import (simulate_ga4_events,
                           load_kaggle_behavior,
                           load_kaggle_orders,
                           KaggleSetup,
                           KG_IDDown)
from .pipeline     import  ensure_nonempty_events, grow_split_to_size
from .transformers import (build_item_features, 
                           build_user_features, 
                           write_implicit_matrix)


def _ensure_dirs(*paths: str | Path) -> None:
    for p in paths:
        path_obj = Path(p)
        if not path_obj.exists():
            path_obj.mkdir(parents=True, exist_ok=True)
        else:
            logger.info(f"Dir already exists: {path_obj.resolve()}")


def _filechecker(path_str: str) -> bool:
    p = Path(path_str)
    if "*" in path_str or "?" in path_str:
        matches = list(Path().glob(path_str))
        exists = len(matches) > 0
        logger.debug(f"Wildcard pattern '{path_str}' "
                     f"matched {len(matches)} file(s).")
        return exists
    exists = p.exists()
    logger.debug(f"Path '{path_str}' exists: {exists}")
    return exists

def _resolvepath(ymlpath: str) -> Path:
    path_obj = Path(ymlpath).expanduser().resolve()
    if not path_obj.exists():
        raise argparse.ArgumentTypeError(f"Config File is not found: {path_obj}")
    return path_obj

def _auto_download_sources(cfg: dict[str, Any]) -> dict[str, Path]:
    """Download Kaggle sources if missing and return verified source paths."""
    logger.info("[Step 0] Verifying raw source datasets...")
    beh_path = Path(cfg["sources"]["kaggle_behavior"]["path"])
    ord_path = Path(cfg["sources"]["kaggle_orders"]["path"])

    logger.info(f"[Step 0] Behavior target pattern: {beh_path}")
    logger.info(f"[Step 0] Orders target pattern:   {ord_path}")
    has_beh = _filechecker(str(beh_path))
    has_ord = _filechecker(str(ord_path))
    if not has_beh or not has_ord:
        logger.warning("[Step 0] One or more source datasets missing. Triggering Kaggle downloader.")
        if KaggleSetup():
            ds_ids = cfg.get("sources", {}).get(
                             "kaggle_dataset_ids",
                             ["kgurl-01", "kgurl-02"],)
            download_status = KG_IDDown(dataset_ids=ds_ids)
            logger.info(f"[Step 0] Download status: {download_status}")
            if not all(download_status.values()):
                logger.warning(f"[Step 0] Some downloads reported non-success: {download_status}")
        else:
            logger.error("[Step 0] KaggleSetup failed. Unable to authenticate.")

    # Re-check post-download
    has_beh_after = _filechecker(str(beh_path))
    has_ord_after = _filechecker(str(ord_path))
    if not has_beh_after or not has_ord_after:
        logger.error(f"[Step 0] Validation failed! Behavior exists: {has_beh_after}, Orders exists: {has_ord_after}")
        raise FileNotFoundError(
            f"Source datasets missing after download process.\n"
            f"Expected behavior path: {beh_path}\n"
            f"Expected orders path: {ord_path}")
    logger.info("[Step 0] Raw source datasets verified successfully.")
    return {"behavior": beh_path,
            "orders"  : ord_path,}


def build_dataset(
    config_path: str | Path = (
        "src/zapudataset/configs/pipeconf.yaml"
    ),
) -> dict[str, Any]:
    """Run full dataset building pipeline."""
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("  STARTING DATASET BUILDER PIPELINE")
    logger.info("=" * 60)

    cfg = deepcopy(configure)
    seed = cfg["pipeline"]["seed"]
    out_dir = Path(cfg["pipeline"]["output_dir"])
    int_dir = Path(cfg["pipeline"]["intermediate_dir"])
    
    logger.info(f"[Init] Output Directory:       {out_dir.resolve()}")
    logger.info(f"[Init] Intermediate Directory: {int_dir.resolve()}")
    _ensure_dirs(out_dir, int_dir)

    # 0. Auto download raw datasets & get verified filepaths
    source_paths = _auto_download_sources(cfg)

    # 1. Extract & harmonize Kaggle sources
    logger.info("[Step 1] Extracting & harmonizing Kaggle sources...")
    beh_cfg = cfg["sources"]["kaggle_behavior"]
    
    logger.info(f"[Step 1] Loading behavior logs from: {source_paths['behavior']}")
    behavior = load_kaggle_behavior(
        str(source_paths["behavior"]),
        beh_cfg["event_type_map"],
    )
    
    logger.info(f"[Step 1] Loading order logs from:    {source_paths['orders']}")
    orders = load_kaggle_orders(str(source_paths["orders"]))

    logger.info("[Step 1] Concatenating LazyFrames & collecting to RAM...")
    events = pl.concat(
        [behavior, orders],
        how="vertical_relaxed",
    ).collect(streaming=True)
    
    logger.info(f"[Step 1] Raw concatenated events height: {events.height:,} rows | Schema columns: {len(events.columns)}")
    events = ensure_nonempty_events(events, cfg)
    logger.info(f"[Step 1] Non-empty events height:       {events.height:,} rows")

    # 2. Augment with GA4 telemetry
    logger.info("[Step 2] Augmenting dataset with GA4 telemetry simulation...")
    events = simulate_ga4_events(events, config=cfg)
    logger.info(f"[Step 2] Events height post GA4 augmentation: {events.height:,} rows")

    # 3. Noise injection
    logger.info("[Step 3] Injecting synthetic noise & anomalies...")
    nz = cfg["noise"]
    ncfg = NoiseConfig(
        telemetry_drop_rate=nz["telemetry_drop_rate"],
        bot_fraction=nz["bot_fraction"],
        bot_lambda_per_sec=nz["bot_lambda_per_sec"],
        bot_interarrival_min_ms=nz["bot_interarrival_min_ms"],
        bot_interarrival_max_ms=nz["bot_interarrival_max_ms"],
        oo_lambda_per_sec=nz["oo_lambda_per_sec"],
        schema_null_rate=nz["schema_null_rate"],
        cold_start_rate=nz["cold_start_rate"],
        seed=seed,
    )
    corrupted, cold_items, null_items = inject_all(
        events,
        ncfg,
        cfg["geo_provinces"],
    )
    logger.info(f"[Step 3] Corrupted events height: {corrupted.height:,} rows")
    logger.info(f"[Step 3] Cold start items count:  {len(cold_items)}")
    logger.info(f"[Step 3] Null items count:        {len(null_items)}")

    cold_items_path = out_dir / "cold_items.json"
    null_items_path = out_dir / "null_items.json"
    
    cold_items_path.write_text(json.dumps(sorted(cold_items)))
    null_items_path.write_text(json.dumps(sorted(null_items)))
    logger.info(f"[Step 3] Saved: {cold_items_path.name} & {null_items_path.name}")

    # 4. Train/test split
    logger.info("[Step 4] Executing Train/Test split...")
    min_int = cfg["split"]["min_user_interactions"]
    user_counts = (
        corrupted.group_by(pl.col("user_id"))
        .agg(pl.len().alias("n"))
        .filter(pl.col("n") >= min_int)
    )
    eligible = user_counts["user_id"]
    logger.info(f"[Step 4] Total eligible users (>= {min_int} interactions): {eligible.len():,}")

    n_test = int(cfg["split"]["test_fraction"] * eligible.len())
    test_users = set(
        eligible.sample(n=n_test, seed=seed).to_list()
    )
    logger.info(f"[Step 4] Sampled test users count: {len(test_users):,}")

    train = corrupted.filter(~pl.col("user_id").is_in(list(test_users)))
    test = corrupted.filter(pl.col("user_id").is_in(list(test_users)))
    
    train_path = out_dir / "train_interactions.parquet"
    test_path = out_dir / "test_interactions.parquet"
    
    logger.info(f"[Step 4] Growing/adjusting split sizes...")
    train, test = grow_split_to_size(
        train,
        test,
        train_path,
        test_path,
        cfg,
    )
    logger.info(f"[Step 4] Saved train interactions ({train.height:,} rows) -> {train_path.resolve()}")
    logger.info(f"[Step 4] Saved test interactions  ({test.height:,} rows)  -> {test_path.resolve()}")

    # 5. Implicit matrix (DuckDB)
    logger.info("[Step 5] Building implicit interaction matrix via DuckDB...")
    weights = cfg["implicit_weights"]
    imp_cfg = cfg["implicit_normalization"]
    implicit_path = write_implicit_matrix(
        train_path,
        out_dir / "implicit_matrix.parquet",
        weights,
        norm_min=imp_cfg["min_rating"],
        norm_max=imp_cfg["max_rating"],
    )
    logger.info(f"[Step 5] Saved implicit matrix -> {Path(implicit_path).resolve()}")

    # 6. Feature parquet exports
    logger.info("[Step 6] Extracting item & user feature tables...")
    full_cat = pl.concat(
        [train, test],
        how="vertical_relaxed",
    )
    item_path, item_maps = build_item_features(
        full_cat,
        cold_items,
        out_dir / "item_features.parquet",
    )
    logger.info(f"[Step 6] Saved item features -> {Path(item_path).resolve()}")

    user_path, user_maps = build_user_features(
        train,
        out_dir / "user_features.parquet",
    )
    logger.info(f"[Step 6] Saved user features -> {Path(user_path).resolve()}")

    # 7. Manifest
    logger.info("[Step 7] Generating pipeline manifest...")
    manifest = {
        "pipeline": cfg["pipeline"],
        "n_events_raw": int(events.height),
        "n_events_train": int(train.height),
        "n_events_test": int(test.height),
        "n_cold_items": len(cold_items),
        "n_null_items": len(null_items),
        "n_users_train": int(train["user_id"].n_unique()),
        "n_users_test": int(test["user_id"].n_unique()),
        "n_items_catalog": int(full_cat["item_id"].n_unique()),
        "outputs": {
            "train_interactions": str(train_path),
            "test_interactions": str(test_path),
            "item_features": str(item_path),
            "user_features": str(user_path),
            "implicit_matrix": str(implicit_path),
        },
        "noise_config": asdict(ncfg),
        "built_at": datetime.now(timezone.utc).isoformat(),
    }
    manifest_path = out_dir / "build_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    logger.info(f"[Step 7] Saved build manifest -> {manifest_path.resolve()}")

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info("=" * 60)
    logger.info(f"  PIPELINE COMPLETED SUCCESSFULLY IN {elapsed:.2f}s")
    logger.info("=" * 60)
    return manifest


class DatasetBuilder:
    """Backward-compatible facade around build_dataset."""

    def __init__(
        self,
        config_path: str | Path = (
            "src/zapudataset/configs/pipeconf.yaml"
        ),
    ) -> None:
        self.config_path = config_path

    def build(self) -> dict[str, Any]:
        return build_dataset(self.config_path)

    def run(self) -> dict[str, Any]:
        return self.build()


if __name__ == "__main__":
    import argparse
    
    parser  = argparse.ArgumentParser(
              description = "ZaPuDia End-to-End Dataset Builder Pipeline")
    cfgpath = Path("src/zapudataset/configs/pipeconf.yaml").resolve()
    parser.add_argument(
              "-c",
              "--config",
              type    = _resolvepath,
              default = cfgpath if cfgpath.exists() else "./src/zapudataset/configs/pipeconf.yaml",
              help    = "Path to configuration file tipe yaml",)
    args = parser.parse_args()

    # Cast to Path.resolve()
    config_realpath = (args.config
                       if isinstance(args.config, Path)
                       else Path(args.config).expanduser().resolve())
    logger.info(f"[CLI] Executing with resolved config path: {config_realpath}")
    build_dataset(config_path = config_realpath)
