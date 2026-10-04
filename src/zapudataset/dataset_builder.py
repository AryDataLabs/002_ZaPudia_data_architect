#!/usr/bin/env python3

__author__     = "Aryanto"
__copyright__  = "Copyright 2026, AryDataLabs/ZaPuDia Series"
__credits__    = ["aryanto"]
__license__    = "GNU_Public"
__version__    = "0.0.1"
__maintainer__ = "Aryanto, M.Si"
__email__      = "aryanto.dandan@gmail.com"
__created__    = "2026-08-31"
__modified__   = "2026-10-03"


"""
End-to-end dataset builder orchestrator.
________________________________________
Wires extractors -> noise -> implicit builder -> feature encoder -> split ->
partitioned parquet exports. Single entry point for the whole pipeline.
Exports (in cfg.output_dir):
  - train_interactions.parquet
  - test_interactions.parquet
  - item_features.parquet (+ .encoders.json)
  - user_features.parquet (+ .encoders.json)
  - implicit_matrix.parquet
  - cold_items.json, null_items.json, build_manifest.json
"""

import json
import yaml
import polars as pl
from   dataclasses import asdict
from   datetime    import datetime, timezone
from   pathlib     import Path
from   typing      import Any

from .noise        import  NoiseConfig, inject_all
from .configs      import PipeConfig
from .extractors   import (simulate_ga4_events,
                           load_kaggle_behavior,
                           load_kaggle_orders,
                           KaggleSetup,
                           KG_IDDown)
from .pipeline     import  ensure_nonempty_events, grow_split_to_size
from .transformers import (build_item_features, 
                           build_user_features, 
                           write_implicit_matrix)

def _load_config(path: str | Path) -> dict[str, Any]:
    """Load configuration YAML file."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _ensure_dirs(*paths: str | Path) -> None:
    """Create directories if they do not exist."""
    for p in paths:
        Path(p).mkdir(parents=True, exist_ok=True)


def _file_or_pattern_exists(path_str: str) -> bool:
    """Check if file exists or matches a wildcard pattern using pathlib."""
    p = Path(path_str)
    if "*" in path_str or "?" in path_str:
        return any(Path().glob(path_str))
    return p.exists()


def _auto_download_sources(cfg: dict[str, Any]) -> None:
    """Download Kaggle sources if missing."""
    beh_path = cfg["sources"]["kaggle_behavior"]["path"]
    ord_path = cfg["sources"]["kaggle_orders"]["path"]
    
    # Menggunakan pathlib via helper function _file_or_pattern_exists
    if not _file_or_pattern_exists(beh_path) or not _file_or_pattern_exists(ord_path):
        if KaggleSetup():
            ds_ids = cfg.get("sources", {}).get(
                "kaggle_dataset_ids",
                ["kgurl-01", "kgurl-02"],
            )
            KG_IDDown(dataset_ids=ds_ids)


def build_dataset(
    config_path: str | Path = (
        "src/zapudataset/configs/pipeconf.yaml"
    ),
) -> dict[str, Any]:
    """Run full dataset building pipeline."""
    cfg = _load_config(config_path)
    seed = cfg["pipeline"]["seed"]
    out_dir = Path(cfg["pipeline"]["output_dir"])
    int_dir = Path(cfg["pipeline"]["intermediate_dir"])
    _ensure_dirs(out_dir, int_dir)

    # Auto download raw datasets if missing
    _auto_download_sources(cfg)

    # 1. Extract & harmonize Kaggle sources
    beh_cfg = cfg["sources"]["kaggle_behavior"]
    ord_cfg = cfg["sources"]["kaggle_orders"]
    behavior = load_kaggle_behavior(
        beh_cfg["path"],
        beh_cfg["event_type_map"],
    )
    orders = load_kaggle_orders(ord_cfg["path"])
    events = pl.concat(
        [behavior, orders],
        how="vertical_relaxed",
    ).collect()
    events = ensure_nonempty_events(events, cfg)

    # 2. Augment with GA4 telemetry
    events = simulate_ga4_events(events, config=cfg)

    # 3. Noise injection
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
    (out_dir / "cold_items.json").write_text(
        json.dumps(sorted(cold_items))
    )
    (out_dir / "null_items.json").write_text(
        json.dumps(sorted(null_items))
    )

    # 4. Train/test split
    min_int = cfg["split"]["min_user_interactions"]
    user_counts = (
        corrupted.group_by(pl.col("user_id"))
        .agg(pl.len().alias("n"))
        .filter(pl.col("n") >= min_int)
    )
    eligible = user_counts["user_id"]
    n_test = int(cfg["split"]["test_fraction"] * eligible.len())
    test_users = set(
        eligible.sample(n=n_test, seed=seed).to_list()
    )
    train = corrupted.filter(
        ~pl.col("user_id").is_in(list(test_users))
    )
    test = corrupted.filter(
        pl.col("user_id").is_in(list(test_users))
    )
    train_path = out_dir / "train_interactions.parquet"
    test_path = out_dir / "test_interactions.parquet"
    train, test = grow_split_to_size(
        train,
        test,
        train_path,
        test_path,
        cfg,
    )

    # 5. Implicit matrix (DuckDB)
    weights = cfg["implicit_weights"]
    imp_cfg = cfg["implicit_normalization"]
    implicit_path = write_implicit_matrix(
        train_path,
        out_dir / "implicit_matrix.parquet",
        weights,
        norm_min=imp_cfg["min_rating"],
        norm_max=imp_cfg["max_rating"],
    )

    # 6. Feature parquet exports
    full_cat = pl.concat(
        [train, test],
        how="vertical_relaxed",
    )
    item_path, item_maps = build_item_features(
        full_cat,
        cold_items,
        out_dir / "item_features.parquet",
    )
    user_path, user_maps = build_user_features(
        train,
        out_dir / "user_features.parquet",
    )

    # 7. Manifest
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
    build_dataset()