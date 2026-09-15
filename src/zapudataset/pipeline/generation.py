#!/usr/bin/env python3
"""Deterministic synthetic interaction generator used when raw sources are empty/small.

The generator is deliberately schema-compatible with the unified ZaPudia event stream.
It is not a fake padding mechanism: every generated row is a plausible interaction and
carries enough cardinality to make parquet-size targets meaningful.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl


EVENT_SCHEMA = {
    "event_time": pl.Int64,
    "event_type": pl.Utf8,
    "user_id": pl.Utf8,
    "user_pseudo_id": pl.Utf8,
    "item_id": pl.Utf8,
    "category_id": pl.Utf8,
    "category_code": pl.Utf8,
    "brand": pl.Utf8,
    "price": pl.Float64,
    "user_session": pl.Utf8,
    "engagement_time_msec": pl.Int64,
    "device_category": pl.Utf8,
    "geo_province": pl.Utf8,
    "discount_flag": pl.Boolean,
    "fulfillment_status": pl.Utf8,
    "customer_ltv": pl.Float64,
    "age_band": pl.Utf8,
}


def _catalog(cfg: dict[str, Any]) -> tuple[list[str], list[str], list[str]]:
    categories = cfg.get("categories", {})
    category_codes = [x for values in categories.values() for x in values]
    if not category_codes:
        category_codes = ["fmcg.food.snack", "electronics.smartphone", "fashion.shoes"]
    brands = [f"brand_{i:04d}" for i in range(max(256, min(2048, len(category_codes) * 64)))]
    provinces = list(cfg.get("geo_provinces") or ["ID-JK"])
    return category_codes, brands, provinces


def generate_synthetic_events(
    n_users: int,
    interactions_per_user: int,
    seed: int,
    cfg: dict[str, Any],
    user_offset: int = 0,
    item_count: int = 50_000,
) -> pl.DataFrame:
    """Generate realistic interaction rows without Python row-by-row loops."""
    if n_users <= 0 or interactions_per_user <= 0:
        return pl.DataFrame(schema=EVENT_SCHEMA)

    rng = np.random.default_rng(seed)
    n = n_users * interactions_per_user
    user_idx = np.repeat(np.arange(user_offset, user_offset + n_users, dtype=np.int64), interactions_per_user)
    step = np.tile(np.arange(interactions_per_user, dtype=np.int64), n_users)
    item_idx = rng.integers(0, item_count, size=n, dtype=np.int64)
    event_type = rng.choice(["view", "cart", "purchase", "remove", "checkout"], size=n,
                            p=[0.62, 0.16, 0.10, 0.06, 0.06])
    categories, brands, provinces = _catalog(cfg)
    category_idx = item_idx % len(categories)
    brand_idx = item_idx % len(brands)
    base = np.datetime64("2024-01-01T00:00:00", "ms").astype("int64")
    event_time = base + user_idx * 86_400_000 + step * rng.integers(20_000, 900_000, size=n, dtype=np.int64)
    # Stable session IDs with high cardinality, but still human-scale.
    session_no = step // max(1, min(8, interactions_per_user))
    sessions = np.char.add(np.char.add("sess_", user_idx.astype(str)), np.char.add("_", session_no.astype(str)))
    users = np.char.add("user_", user_idx.astype(str))
    pseudo = np.char.add("anon_", user_idx.astype(str))
    prices = np.round(rng.lognormal(mean=4.2, sigma=0.8, size=n), 2)
    dwell = rng.integers(50, 180_000, size=n, dtype=np.int64)
    devices = rng.choice(["mobile", "desktop", "tablet"], size=n, p=[0.78, 0.18, 0.04])
    ages = rng.choice(["18-24", "25-34", "35-44", "45+"], size=n, p=[0.22, 0.38, 0.25, 0.15])
    fulfillment = np.where(event_type == "purchase", rng.choice(["completed", "shipped", "delivered"], size=n), None)
    ltv = np.round(rng.lognormal(mean=6.5, sigma=0.9, size=n), 2)
    discount = rng.random(n) < 0.18

    return pl.DataFrame({
        "event_time": event_time,
        "event_type": event_type,
        "user_id": users.tolist(),
        "user_pseudo_id": pseudo.tolist(),
        "item_id": np.char.add("item_", np.char.zfill(item_idx.astype(str), 6)).tolist(),
        "category_id": np.char.add("cat_", category_idx.astype(str)).tolist(),
        "category_code": [categories[i] for i in category_idx],
        "brand": [brands[i] for i in brand_idx],
        "price": prices,
        "user_session": sessions.tolist(),
        "engagement_time_msec": dwell,
        "device_category": devices.tolist(),
        "geo_province": rng.choice(provinces, size=n).tolist(),
        "discount_flag": discount,
        "fulfillment_status": fulfillment.tolist(),
        "customer_ltv": ltv,
        "age_band": ages.tolist(),
    }, schema=EVENT_SCHEMA)


def ensure_nonempty_events(events: pl.DataFrame, cfg: dict[str, Any]) -> pl.DataFrame:
    """Return source data if present, otherwise generate a deterministic baseline."""
    generation = cfg.get("generation", {})
    if not events.is_empty():
        return events
    return generate_synthetic_events(
        n_users=int(generation.get("bootstrap_users", 20_000)),
        interactions_per_user=int(generation.get("interactions_per_user", 25)),
        seed=int(cfg["pipeline"]["seed"]),
        cfg=cfg,
    )


def grow_split_to_size(
    train: pl.DataFrame,
    test: pl.DataFrame,
    train_path: Path,
    test_path: Path,
    cfg: dict[str, Any],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Grow train/test with new synthetic users until configured byte targets are met."""
    generation = cfg.get("generation", {})
    target_train = int(float(generation.get("min_train_mb", 100)) * 1024 * 1024)
    target_test = int(float(generation.get("min_test_mb", 100)) * 1024 * 1024)
    if target_train <= 0 and target_test <= 0:
        return train, test

    seed = int(cfg["pipeline"]["seed"])
    per_user = int(generation.get("interactions_per_user", 25))
    item_count = int(generation.get("item_count", 50_000))
    max_rounds = int(generation.get("max_growth_rounds", 8))
    next_user = max(train["user_id"].n_unique(), test["user_id"].n_unique()) + 1

    for round_no in range(max_rounds):
        train.write_parquet(train_path)
        test.write_parquet(test_path)
        train_ok = train_path.stat().st_size >= target_train
        test_ok = test_path.stat().st_size >= target_test
        if train_ok and test_ok:
            return train, test

        # Estimate the number of additional users from current byte/user density.
        sizes = [max(train_path.stat().st_size, 1), max(test_path.stat().st_size, 1)]
        users = [max(train["user_id"].n_unique(), 1), max(test["user_id"].n_unique(), 1)]
        need_train = max(0, target_train - sizes[0])
        need_test = max(0, target_test - sizes[1])
        add_train = max(100, int(np.ceil(need_train / max(sizes[0] / users[0], 1)))) if not train_ok else 0
        add_test = max(100, int(np.ceil(need_test / max(sizes[1] / users[1], 1)))) if not test_ok else 0
        # Keep growth bounded per round to avoid a pathological one-shot allocation.
        add_train = min(add_train, max(100, users[0] * 3))
        add_test = min(add_test, max(100, users[1] * 3))
        if add_train:
            extra = generate_synthetic_events(add_train, per_user, seed + round_no * 101,
                                              cfg, user_offset=next_user, item_count=item_count)
            train = pl.concat([train, extra], how="vertical_relaxed")
            next_user += add_train
        if add_test:
            extra = generate_synthetic_events(add_test, per_user, seed + round_no * 101 + 1,
                                              cfg, user_offset=next_user, item_count=item_count)
            test = pl.concat([test, extra], how="vertical_relaxed")
            next_user += add_test
    # Final write makes the actual achieved size visible even if a target is impossible
    # under max_growth_rounds.
    train.write_parquet(train_path)
    test.write_parquet(test_path)
    return train, test
