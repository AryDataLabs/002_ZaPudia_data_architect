"""Individual pipeline stage implementations.

Each stage is a class with logging, error handling, and joblib optimization.
Stages can be executed independently or composed in the main orchestrator.
"""
from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import polars as pl
from joblib import Parallel, delayed

from extractors.ga4_stream_simulator import simulate_ga4_events
from extractors.kaggle_loader import load_kaggle_behavior, load_kaggle_orders
from noise.anomaly_injector import NoiseConfig, inject_all
from transformers.feature_encoder import build_item_features, build_user_features
from transformers.implicit_builder import write_implicit_matrix

logger = logging.getLogger(__name__)


class PipelineStage(ABC):
    """Base class for all pipeline stages."""
    
    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(f"{__name__}.{name}")
    
    @abstractmethod
    def execute(self, **kwargs) -> Any:
        """Execute the stage. Must be implemented by subclasses."""
        pass
    
    def log_start(self):
        """Log stage start."""
        self.logger.info(f"{'='*60}")
        self.logger.info(f"Starting stage: {self.name}")
        self.logger.info(f"{'='*60}")
    
    def log_end(self, result_summary: str = ""):
        """Log stage completion."""
        self.logger.info(f"Completed stage: {self.name}")
        if result_summary:
            self.logger.info(f"Result: {result_summary}")
        self.logger.info(f"{'='*60}\n")


class DataExtractionStage(PipelineStage):
    """Extract and harmonize data from multiple sources in parallel."""
    
    def __init__(self, n_jobs: int = -1):
        super().__init__("DataExtraction")
        self.n_jobs = n_jobs
    
    def execute(
        self,
        behavior_path: str,
        orders_path: str,
        event_type_map: dict[str, str],
    ) -> pl.DataFrame:
        """Load Kaggle behavior and orders data in parallel.
        
        Args:
            behavior_path: Path to Kaggle behavior CSV
            orders_path: Path to Kaggle orders CSV
            event_type_map: Mapping of event types
            
        Returns:
            Combined events DataFrame
        """
        self.log_start()
        
        self.logger.info(f"Loading data sources in parallel (n_jobs={self.n_jobs})")
        self.logger.debug(f"Behavior path: {behavior_path}")
        self.logger.debug(f"Orders path: {orders_path}")
        
        # Parallel data loading using joblib
        results = Parallel(n_jobs=min(2, self.n_jobs if self.n_jobs > 0 else 2))(
            [
                delayed(load_kaggle_behavior)(behavior_path, event_type_map),
                delayed(load_kaggle_orders)(orders_path),
            ]
        )
        
        behavior, orders = results
        self.logger.info(f"Loaded behavior data: {behavior.select(pl.len()).collect().item():,} rows")
        self.logger.info(f"Loaded orders data: {orders.select(pl.len()).collect().item():,} rows")
        
        # Combine sources
        events = pl.concat([behavior, orders], how="vertical_relaxed").collect()
        
        self.log_end(f"Total events: {events.height:,}")
        return events


class TelemetryAugmentationStage(PipelineStage):
    """Augment events with GA4 telemetry data."""
    
    def __init__(self):
        super().__init__("TelemetryAugmentation")
    
    def execute(
        self,
        events: pl.DataFrame,
        seed: int,
        provinces: list[str],
    ) -> pl.DataFrame:
        """Add GA4 telemetry fields to events.
        
        Args:
            events: Input events DataFrame
            seed: Random seed for reproducibility
            provinces: List of geographic provinces
            
        Returns:
            Events with GA4 telemetry fields
        """
        self.log_start()
        
        self.logger.info(f"Simulating GA4 events with seed={seed}")
        self.logger.debug(f"Input shape: {events.shape}")
        
        augmented = simulate_ga4_events(events, seed=seed, provinces=provinces)
        
        self.logger.info(f"Added telemetry fields: {set(augmented.columns) - set(events.columns)}")
        self.log_end(f"Output shape: {augmented.shape}")
        
        return augmented


class NoiseInjectionStage(PipelineStage):
    """Inject realistic noise and anomalies into the dataset."""
    
    def __init__(self):
        super().__init__("NoiseInjection")
    
    def execute(
        self,
        events: pl.DataFrame,
        config: NoiseConfig,
        provinces: list[str],
        output_dir: Path,
    ) -> tuple[pl.DataFrame, set[str], set[str]]:
        """Inject noise and track anomalous items.
        
        Args:
            events: Clean events DataFrame
            config: Noise injection configuration
            provinces: Geographic provinces
            output_dir: Directory to save anomaly metadata
            
        Returns:
            Tuple of (corrupted_events, cold_items, null_items)
        """
        self.log_start()
        
        self.logger.info(f"Noise config: {asdict(config)}")
        
        corrupted, cold_items, null_items = inject_all(events, config, provinces)
        
        self.logger.info(f"Noise injection complete:")
        self.logger.info(f"  - Cold start items: {len(cold_items)}")
        self.logger.info(f"  - Null items: {len(null_items)}")
        self.logger.info(f"  - Output rows: {corrupted.height:,}")
        
        # Persist anomaly metadata
        cold_path = output_dir / "cold_items.json"
        null_path = output_dir / "null_items.json"
        
        cold_path.write_text(json.dumps(sorted(cold_items), indent=2))
        null_path.write_text(json.dumps(sorted(null_items), indent=2))
        
        self.logger.debug(f"Saved cold items to {cold_path}")
        self.logger.debug(f"Saved null items to {null_path}")
        
        self.log_end(f"Corrupted: {corrupted.height:,} events")
        return corrupted, cold_items, null_items


class TrainTestSplitStage(PipelineStage):
    """User-level train/test split with no data leakage."""
    
    def __init__(self):
        super().__init__("TrainTestSplit")
    
    def execute(
        self,
        events: pl.DataFrame,
        test_fraction: float,
        min_user_interactions: int,
        seed: int,
        output_dir: Path,
    ) -> tuple[Path, Path, pl.DataFrame, pl.DataFrame]:
        """Split users into train and test sets.
        
        Args:
            events: Full events DataFrame
            test_fraction: Fraction of users for test set
            min_user_interactions: Minimum interactions per user
            seed: Random seed
            output_dir: Directory to save splits
            
        Returns:
            Tuple of (train_path, test_path, train_df, test_df)
        """
        self.log_start()
        
        self.logger.info(f"Split config: test_fraction={test_fraction}, "
                        f"min_interactions={min_user_interactions}, seed={seed}")
        
        # Filter eligible users
        user_counts = (
            events.group_by(pl.col("user_id"))
            .agg(pl.len().alias("n"))
            .filter(pl.col("n") >= min_user_interactions)
        )
        
        eligible_users = user_counts["user_id"]
        self.logger.info(f"Eligible users (>={min_user_interactions} interactions): {eligible_users.len()}")
        
        # Sample test users
        n_test = int(test_fraction * eligible_users.len())
        test_users = set(eligible_users.sample(n=n_test, seed=seed).to_list())
        
        self.logger.info(f"Test users: {len(test_users)} ({test_fraction*100:.1f}%)")
        
        # Split data
        train = events.filter(~pl.col("user_id").is_in(list(test_users)))
        test = events.filter(pl.col("user_id").is_in(list(test_users)))
        
        self.logger.info(f"Train events: {train.height:,}")
        self.logger.info(f"Test events: {test.height:,}")
        
        # Save to parquet
        train_path = output_dir / "train_interactions.parquet"
        test_path = output_dir / "test_interactions.parquet"
        
        train.write_parquet(train_path)
        test.write_parquet(test_path)
        
        self.logger.debug(f"Saved train data to {train_path}")
        self.logger.debug(f"Saved test data to {test_path}")
        
        self.log_end(f"Train: {train.height:,}, Test: {test.height:,}")
        return train_path, test_path, train, test


class ImplicitMatrixStage(PipelineStage):
    """Build implicit feedback matrix from training interactions."""
    
    def __init__(self):
        super().__init__("ImplicitMatrix")
    
    def execute(
        self,
        train_path: Path,
        output_path: Path,
        weights: dict[str, float],
        norm_min: float,
        norm_max: float,
    ) -> Path:
        """Generate weighted implicit feedback matrix.
        
        Args:
            train_path: Path to training interactions parquet
            output_path: Output path for implicit matrix
            weights: Event type weights
            norm_min: Normalization minimum
            norm_max: Normalization maximum
            
        Returns:
            Path to saved implicit matrix
        """
        self.log_start()
        
        self.logger.info(f"Building implicit matrix from {train_path}")
        self.logger.debug(f"Weights: {weights}")
        self.logger.debug(f"Normalization: [{norm_min}, {norm_max}]")
        
        implicit_path = write_implicit_matrix(
            train_path,
            output_path,
            weights,
            norm_min=norm_min,
            norm_max=norm_max,
        )
        
        self.logger.info(f"Implicit matrix saved to {implicit_path}")
        self.log_end(f"Matrix: {implicit_path}")
        
        return implicit_path


class FeatureEngineeringStage(PipelineStage):
    """Build item and user feature parquets with encoding metadata."""
    
    def __init__(self, n_jobs: int = -1):
        super().__init__("FeatureEngineering")
        self.n_jobs = n_jobs
    
    def execute(
        self,
        train: pl.DataFrame,
        test: pl.DataFrame,
        cold_items: set[str],
        output_dir: Path,
    ) -> tuple[Path, Path, dict, dict]:
        """Build item and user features in parallel.
        
        Args:
            train: Training interactions
            test: Test interactions
            cold_items: Set of cold-start item IDs
            output_dir: Output directory
            
        Returns:
            Tuple of (item_path, user_path, item_maps, user_maps)
        """
        self.log_start()
        
        # Full catalog = train + test
        full_catalog = pl.concat([train, test], how="vertical_relaxed")
        self.logger.info(f"Full catalog unique items: {full_catalog['item_id'].n_unique()}")
        
        # Parallel feature building
        self.logger.info(f"Building features in parallel (n_jobs={self.n_jobs})")
        
        results = Parallel(n_jobs=min(2, self.n_jobs if self.n_jobs > 0 else 2))(
            [
                delayed(build_item_features)(
                    full_catalog, 
                    cold_items, 
                    output_dir / "item_features.parquet"
                ),
                delayed(build_user_features)(
                    train, 
                    output_dir / "user_features.parquet"
                ),
            ]
        )
        
        (item_path, item_maps), (user_path, user_maps) = results
        
        self.logger.info(f"Item features saved: {item_path}")
        self.logger.info(f"User features saved: {user_path}")
        
        self.log_end(f"Features: items={item_path.name}, users={user_path.name}")
        return item_path, user_path, item_maps, user_maps
