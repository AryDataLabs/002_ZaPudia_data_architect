#!/usr/bin/env python3

__author__     = "Aryanto"
__copyright__  = "Copyright 2026, AryDataLabs/ZaPuDia Series"
__credits__    = ["aryanto"]
__license__    = "GNU_Public"
__version__    = "0.0.1"
__maintainer__ = "Aryanto, M.Si"
__email__      = "aryanto.dandan@gmail.com"
__created__    = "2026-08-31"
__modified__   = "2026-09-06"


"""End-to-end dataset builder orchestrator.

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
from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl
import yaml

from extractors.ga4_stream_simulator import simulate_ga4_events
from extractors.kaggle_loader import (
    load_kaggle_behavior,
    load_kaggle_orders,
)
from noise.anomaly_injector import NoiseConfig, inject_all
from transformers.feature_encoder import build_item_features, build_user_features
from transformers.implicit_builder import write_implicit_matrix


def _load_config(path: str | Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _ensure_dirs(*paths: str | Path) -> None:
    for p in paths:
        Path(p).mkdir(parents=True, exist_ok=True)


def build_dataset(config_path: str | Path = "data_pipeline/configs/pipeline_config.yaml") -> dict[str, Any]:
    """Run the full pipeline. Returns a build manifest dict."""
    cfg = _load_config(config_path)
    seed: int = cfg["pipeline"]["seed"]
    out_dir = Path(cfg["pipeline"]["output_dir"])
    int_dir = Path(cfg["pipeline"]["intermediate_dir"])
    _ensure_dirs(out_dir, int_dir)

    # ---- 1. Extract & harmonize Kaggle sources ----
    behavior = load_kaggle_behavior(
        cfg["sources"]["kaggle_behavior"]["path"],
        cfg["sources"]["kaggle_behavior"]["event_type_map"],
    )
    orders = load_kaggle_orders(cfg["sources"]["kaggle_orders"]["path"])
    events = pl.concat([behavior, orders], how="vertical_relaxed").collect()

    # ---- 2. Augment with GA4 telemetry ----
    events = simulate_ga4_events(
        events, seed=seed, provinces=cfg["geo_provinces"]
    )

    # ---- 3. Noise injection (§2) ----
    ncfg = NoiseConfig(
        telemetry_drop_rate=cfg["noise"]["telemetry_drop_rate"],
        bot_fraction=cfg["noise"]["bot_fraction"],
        bot_lambda_per_sec=cfg["noise"]["bot_lambda_per_sec"],
        bot_interarrival_min_ms=cfg["noise"]["bot_interarrival_min_ms"],
        bot_interarrival_max_ms=cfg["noise"]["bot_interarrival_max_ms"],
        oo_lambda_per_sec=cfg["noise"]["oo_lambda_per_sec"],
        schema_null_rate=cfg["noise"]["schema_null_rate"],
        cold_start_rate=cfg["noise"]["cold_start_rate"],
        seed=seed,
    )
    corrupted, cold_items, null_items = inject_all(events, ncfg, cfg["geo_provinces"])

    # persist cold/null sets
    (out_dir / "cold_items.json").write_text(json.dumps(sorted(cold_items)))
    (out_dir / "null_items.json").write_text(json.dumps(sorted(null_items)))

    # ---- 4. Train/test split (user-level holdout, no leakage) ----
    user_counts = (
        corrupted.group_by(pl.col("user_id"))
        .agg(pl.len().alias("n"))
        .filter(pl.col("n") >= cfg["split"]["min_user_interactions"])
    )
    rng_users = pl.Series(corrupted["user_id"].unique().shuffle(seed=seed))
    eligible = user_counts["user_id"]
    n_test = int(cfg["split"]["test_fraction"] * eligible.len())
    test_users = set(eligible.sample(n=n_test, seed=seed).to_list())

    train = corrupted.filter(~pl.col("user_id").is_in(list(test_users)))
    test = corrupted.filter(pl.col("user_id").is_in(list(test_users)))

    train_path = out_dir / "train_interactions.parquet"
    test_path = out_dir / "test_interactions.parquet"
    train.write_parquet(train_path)
    test.write_parquet(test_path)

    # ---- 5. Implicit matrix (DuckDB) ----
    weights = cfg["implicit_weights"]
    implicit_path = write_implicit_matrix(
        train_path,
        out_dir / "implicit_matrix.parquet",
        weights,
        norm_min=cfg["implicit_normalization"]["min_rating"],
        norm_max=cfg["implicit_normalization"]["max_rating"],
    )

    # ---- 6. Feature parquet exports ----
    # item catalog = train + test unique items (cold items retained in catalog)
    full_catalog_events = pl.concat([train, test], how="vertical_relaxed")
    item_path, item_maps = build_item_features(
        full_catalog_events, cold_items, out_dir / "item_features.parquet"
    )
    user_path, user_maps = build_user_features(
        train, out_dir / "user_features.parquet"
    )

    # ---- 7. Manifest ----
    manifest = {
        "pipeline": cfg["pipeline"],
        "n_events_raw": int(events.height),
        "n_events_train": int(train.height),
        "n_events_test": int(test.height),
        "n_cold_items": len(cold_items),
        "n_null_items": len(null_items),
        "n_users_train": int(train["user_id"].n_unique()),
        "n_users_test": int(test["user_id"].n_unique()),
        "n_items_catalog": int(full_catalog_events["item_id"].n_unique()),
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
    (out_dir / "build_manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


if __name__ == "__main__":
    build_dataset()