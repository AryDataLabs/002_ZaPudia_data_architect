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


"""Example usage of the refactored DatasetPipeline.

Demonstrates various ways to run the pipeline with different
configurations and logging setups.
"""
from datetime import datetime
from pathlib import Path

from pipeline_orchestrator import DatasetPipeline


def example_basic():
    """Basic pipeline execution with console logging."""
    print("\n" + "="*80)
    print("Example 1: Basic Execution")
    print("="*80 + "\n")
    
    pipeline = DatasetPipeline(
        config_path="data_pipeline/configs/pipeline_config.yaml",
        log_level="INFO"
    )
    
    manifest = pipeline.run()
    
    print(f"\n✓ Pipeline completed successfully!")
    print(f"✓ Total duration: {manifest['timing']['total_duration_formatted']}")
    print(f"✓ Outputs: {pipeline.config.output_dir}")
    
    return manifest


def example_with_file_logging():
    """Pipeline with file logging for detailed diagnostics."""
    print("\n" + "="*80)
    print("Example 2: With File Logging")
    print("="*80 + "\n")
    
    # Create timestamped log file
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"pipeline_{datetime.now():%Y%m%d_%H%M%S}.log"
    
    pipeline = DatasetPipeline(
        config_path="data_pipeline/configs/pipeline_config.yaml",
        log_level="INFO",
        log_file=log_file
    )
    
    print(f"Log file: {log_file}")
    
    manifest = pipeline.run()
    
    print(f"\n✓ Pipeline completed!")
    print(f"✓ Check detailed logs in: {log_file}")
    
    return manifest


def example_debug_mode():
    """Debug mode with maximum verbosity."""
    print("\n" + "="*80)
    print("Example 3: Debug Mode")
    print("="*80 + "\n")
    
    log_file = Path("logs") / f"debug_{datetime.now():%Y%m%d_%H%M%S}.log"
    
    pipeline = DatasetPipeline(
        config_path="data_pipeline/configs/pipeline_config.yaml",
        log_level="DEBUG",  # Maximum verbosity
        log_file=log_file
    )
    
    manifest = pipeline.run()
    
    print(f"\n✓ Debug run completed!")
    print(f"✓ Full debug logs: {log_file}")
    
    return manifest


def example_custom_config():
    """Use a custom configuration file."""
    print("\n" + "="*80)
    print("Example 4: Custom Configuration")
    print("="*80 + "\n")
    
    # You can create different configs for different experiments
    custom_config = "data_pipeline/configs/pipeline_config_experiment.yaml"
    
    pipeline = DatasetPipeline(
        config_path=custom_config,
        log_level="INFO"
    )
    
    manifest = pipeline.run()
    
    print(f"\n✓ Custom config run completed!")
    print(f"✓ Config: {custom_config}")
    
    return manifest


def example_inspect_manifest():
    """Run pipeline and inspect the manifest."""
    print("\n" + "="*80)
    print("Example 5: Inspect Build Manifest")
    print("="*80 + "\n")
    
    pipeline = DatasetPipeline(
        config_path="data_pipeline/configs/pipeline_config.yaml",
        log_level="INFO"
    )
    
    manifest = pipeline.run()
    
    # Inspect results
    print("\n" + "-"*80)
    print("MANIFEST INSPECTION")
    print("-"*80)
    
    print(f"\nPipeline Version: {manifest['pipeline']['version']}")
    print(f"Architecture: {manifest['pipeline']['architecture']}")
    
    print(f"\nData Metrics:")
    metrics = manifest['metrics']
    for key, value in metrics.items():
        print(f"  {key:25s}: {value:>10,}")
    
    print(f"\nTiming Breakdown:")
    for stage, duration in manifest['timing']['stage_durations_seconds'].items():
        total = manifest['timing']['total_duration_seconds']
        pct = (duration / total) * 100
        print(f"  {stage:30s}: {duration:6.2f}s ({pct:5.1f}%)")
    
    print(f"\nOutput Files:")
    for name, path in manifest['outputs'].items():
        size = Path(path).stat().st_size / (1024 * 1024)  # MB
        print(f"  {name:25s}: {Path(path).name} ({size:.2f} MB)")
    
    return manifest


def example_error_handling():
    """Demonstrate error handling with invalid config."""
    print("\n" + "="*80)
    print("Example 6: Error Handling")
    print("="*80 + "\n")
    
    try:
        # This will fail with a clear error message
        pipeline = DatasetPipeline(
            config_path="nonexistent_config.yaml",
            log_level="INFO"
        )
        manifest = pipeline.run()
    except FileNotFoundError as e:
        print(f"✗ Expected error caught: {e}")
        print("✓ Error handling works correctly!")
    except Exception as e:
        print(f"✗ Unexpected error: {e}")


def main():
    """Run all examples (comment out the ones you don't need)."""
    print("\n" + "#"*80)
    print("#" + " "*78 + "#")
    print("#" + " "*20 + "DATASET PIPELINE - USAGE EXAMPLES" + " "*25 + "#")
    print("#" + " "*78 + "#")
    print("#"*80)
    
    # Run the example you want (uncomment the one you need)
    
    # Basic usage - recommended for first run
    manifest = example_basic()
    
    # With file logging - recommended for production
    # manifest = example_with_file_logging()
    
    # Debug mode - use when troubleshooting
    # manifest = example_debug_mode()
    
    # Custom config - for experiments
    # manifest = example_custom_config()
    
    # Inspect manifest - see detailed results
    # manifest = example_inspect_manifest()
    
    # Error handling demo
    # example_error_handling()
    
    print("\n" + "#"*80)
    print("#" + " "*78 + "#")
    print("#" + " "*25 + "ALL EXAMPLES COMPLETED" + " "*31 + "#")
    print("#" + " "*78 + "#")
    print("#"*80 + "\n")


if __name__ == "__main__":
    main()
