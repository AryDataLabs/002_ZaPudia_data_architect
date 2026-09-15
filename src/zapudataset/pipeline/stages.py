#!/usr/bin/env python3

__author__     = "Aryanto"
__copyright__  = "Copyright 2026, AryDataLabs/ZaPuDia Series"
__credits__    = ["aryanto"]
__license__    = "GNU_Public"
__version__    = "0.0.2"
__maintainer__ = "Aryanto, M.Si"
__email__      = "aryanto.dandan@gmail.com"
__created__    = "2026-08-31"
__modified__   = "2026-09-15"


"""Individual pipeline stage implementations.

Each stage is a class with logging, error handling, and joblib optimization.
Stages can be executed independently or composed in the main orchestrator.
"""

import json
import polars       as     pl
from   typing       import Any
from   pathlib      import Path
from   dataclasses  import asdict, dataclass
from   joblib       import Parallel, delayed
from   abc          import ABC, abstractmethod

from .generation    import  grow_split_to_size
from ..configs      import  logger
from ..noise        import  NoiseConfig, inject_all
from ..extractors   import (simulate_ga4_events, 
                            load_kaggle_behavior, 
                            load_kaggle_orders,
                            KG_IDDown)
from ..transformers import (write_implicit_matrix, 
                            build_item_features, 
                            build_user_features)


class PipelineStage(ABC):
    """Base class for all pipeline stages."""
    def __init__(self, name: str):
        self.name = name
    
    @abstractmethod
    def execute(self, **kwargs) -> Any:
        """Execute the stage. Must be implemented by subclasses."""
        pass
    
    def log_start(self):
        """Log stage start."""
        logger.info(f"{'='*60}")
        logger.info(f"Starting stage: {self.name}")
        logger.info(f"{'='*60}")
    
    def log_end(self, result_summary: str = ""):
        """Log stage completion."""
        logger.info(f"Completed stage: {self.name}")
        if result_summary:
            logger.info(f"Result: {result_summary}")
        logger.info(f"{'='*60}\n")


class DataExtractionStage(PipelineStage):
    """Extract and harmonize data from multiple sources in parallel."""
    def __init__(self, n_jobs: int = -1):
        super().__init__("DataExtraction")
        self.n_jobs = n_jobs
    
    def execute(
        self,
        behavior_path: str | Path,
        orders_path: str | Path,
        event_type_map: dict[str, str],
        dataset_ids: str | list[str] | None = None,
        json_path: str | Path | None = None,
        base_dir: str | Path = 'data/raw',
    ) -> pl.DataFrame:
        """Load Kaggle behavior and orders data in parallel.
        Args:
            behavior_path: Path to Kaggle behavior CSV
            orders_path: Path to Kaggle orders CSV
            event_type_map: Mapping of event types
            dataset_ids: Kaggle dataset ID(s) to download first via KG_IDDown
            json_path: Path to kaggle_datasets.json
            base_dir: Base directory where Kaggle datasets are extracted

        Returns:
            Combined events DataFrame
        """
        self.log_start()
        
        # 1. Jalankan unduhan otomatis jika dataset_ids disuplai
        if dataset_ids:
            logger.info(f"Triggering Kaggle download for dataset ID(s): {dataset_ids}")
            kwargs = {"base_dir": base_dir}
            if json_path:
                kwargs["json_path"] = json_path
            
            download_results = KG_IDDown(dataset_ids=dataset_ids, **kwargs)
            logger.info(f"Kaggle download results: {download_results}")

        b_path = Path(behavior_path)
        o_path = Path(orders_path)

        # 2. Peringatan jika file target masih belum ditemukan
        if not b_path.exists():
            logger.warning(f"Behavior file not found at: {b_path.resolve()}")
        if not o_path.exists():
            logger.warning(f"Orders file not found at: {o_path.resolve()}")

        logger.info(f"Loading data sources in parallel (n_jobs={self.n_jobs})")
        logger.debug(f"Behavior path: {b_path}")
        logger.debug(f"Orders path: {o_path}")
        
        # Parallel data loading using joblib
        results = Parallel(n_jobs=min(2, self.n_jobs if self.n_jobs > 0 else 2))(
            [
                delayed(load_kaggle_behavior)(str(b_path), event_type_map),
                delayed(load_kaggle_orders)(str(o_path)),
            ]
        )
        
        behavior, orders = results
        
        b_count = behavior.select(pl.len()).collect().item()
        o_count = orders.select(pl.len()).collect().item()

        logger.info(f"Loaded behavior data: {b_count:,} rows")
        logger.info(f"Loaded orders data: {o_count:,} rows")

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
        config: Any,
    ) -> pl.DataFrame:
        """Add GA4 telemetry fields to events.
        
        Args:
            events: Input events DataFrame
            config: PipeConfig object containing seed and provinces
            
        Returns:
            Events with GA4 telemetry fields
        """
        self.log_start()
        
        logger.info(f"Simulating GA4 events with seed={config.pipeline.seed}")
        logger.debug(f"Input shape: {events.shape}")
        
        augmented = simulate_ga4_events(events, config=config)
        
        logger.info(f"Added telemetry fields: {set(augmented.columns) - set(events.columns)}")
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
        logger.info(f"Noise config: {asdict(config)}")
        corrupted, cold_items, null_items = inject_all(events, config, provinces)
        logger.info(f"Noise injection complete:")
        logger.info(f"  - Cold start items: {len(cold_items)}")
        logger.info(f"  - Null items: {len(null_items)}")
        logger.info(f"  - Output rows: {corrupted.height:,}")
        
        # Persist anomaly metadata
        cold_path = output_dir / "cold_items.json"
        null_path = output_dir / "null_items.json"
        cold_path.write_text(json.dumps(sorted(cold_items), indent=2))
        null_path.write_text(json.dumps(sorted(null_items), indent=2))
        logger.debug(f"Saved cold items to {cold_path}")
        logger.debug(f"Saved null items to {null_path}")
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
        generation_config: dict[str, Any] | None = None,
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
        
        logger.info(f"Split config: test_fraction={test_fraction}, "
                        f"min_interactions={min_user_interactions}, seed={seed}")
        
        # Filter eligible users
        user_counts = (
            events.group_by(pl.col("user_id"))
            .agg(pl.len().alias("n"))
            .filter(pl.col("n") >= min_user_interactions)
        )
        
        eligible_users = user_counts["user_id"]
        logger.info(f"Eligible users (>={min_user_interactions} interactions): {eligible_users.len()}")
        
        # Sample test users
        n_test = int(test_fraction * eligible_users.len())
        if n_test <= 0:
            # A smoke test must never silently create an empty parquet.
            n_test = 1 if eligible_users.len() >= 2 and test_fraction > 0 else 0
        test_users = set(eligible_users.sample(n=n_test, seed=seed).to_list()) if n_test else set()
        
        logger.info(f"Test users: {len(test_users)} ({test_fraction*100:.1f}%)")
        
        # Split data
        train = events.filter(~pl.col("user_id").is_in(list(test_users)))
        test = events.filter(pl.col("user_id").is_in(list(test_users)))
        
        logger.info(f"Train events: {train.height:,}")
        logger.info(f"Test events: {test.height:,}")
        
        # Save to parquet
        train_path = output_dir / "train_interactions.parquet"
        test_path = output_dir / "test_interactions.parquet"
        
        generation_config = generation_config or {}
        train, test = grow_split_to_size(
            train, test, train_path, test_path,
            {"pipeline": {"seed": seed}, "generation": generation_config},
        )

        logger.info(
            f"Final split sizes: train={train_path.stat().st_size / 1024 / 1024:.2f} MB, "
            f"test={test_path.stat().st_size / 1024 / 1024:.2f} MB"
        )
        logger.debug(f"Saved train data to {train_path}")
        logger.debug(f"Saved test data to {test_path}")
        
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
        
        logger.info(f"Building implicit matrix from {train_path}")
        logger.debug(f"Weights: {weights}")
        logger.debug(f"Normalization: [{norm_min}, {norm_max}]")
        
        implicit_path = write_implicit_matrix(
            train_path,
            output_path,
            weights,
            norm_min=norm_min,
            norm_max=norm_max,
        )
        
        logger.info(f"Implicit matrix saved to {implicit_path}")
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
        logger.info(f"Full catalog unique items: {full_catalog['item_id'].n_unique()}")
        
        # Parallel feature building
        logger.info(f"Building features in parallel (n_jobs={self.n_jobs})")
        
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
        
        logger.info(f"Item features saved: {item_path}")
        logger.info(f"User features saved: {user_path}")
        
        self.log_end(f"Features: items={item_path.name}, users={user_path.name}")
        return item_path, user_path, item_maps, user_maps


if __name__ == '__main__':
    pass