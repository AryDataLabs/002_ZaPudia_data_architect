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


"""Test client for Dataset Pipeline API.

Provides functions to test all API endpoints and a complete workflow example.

Usage:
    python test_api_client.py
"""
import json
import time
from typing import Any

import requests


class PipelineAPIClient:
    """Client for Dataset Pipeline API."""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
    
    def health_check(self) -> dict[str, Any]:
        """Check API health."""
        response = self.session.get(f"{self.base_url}/health")
        response.raise_for_status()
        return response.json()
    
    def get_service_info(self) -> dict[str, Any]:
        """Get service information."""
        response = self.session.get(f"{self.base_url}/")
        response.raise_for_status()
        return response.json()
    
    def trigger_pipeline(
        self,
        config_path: str | None = None
    ) -> dict[str, Any]:
        """Trigger a new pipeline run.
        
        Args:
            config_path: Optional custom config path
            
        Returns:
            Job information with job_id
        """
        data = {}
        if config_path:
            data["config_path"] = config_path
        
        response = self.session.post(
            f"{self.base_url}/pipeline/run",
            json=data
        )
        response.raise_for_status()
        return response.json()
    
    def get_job_status(self, job_id: str) -> dict[str, Any]:
        """Get job status.
        
        Args:
            job_id: Job UUID
            
        Returns:
            Job status information
        """
        response = self.session.get(
            f"{self.base_url}/pipeline/jobs/{job_id}"
        )
        response.raise_for_status()
        return response.json()
    
    def list_jobs(self) -> dict[str, Any]:
        """List all jobs.
        
        Returns:
            List of all jobs
        """
        response = self.session.get(f"{self.base_url}/pipeline/jobs")
        response.raise_for_status()
        return response.json()
    
    def get_job_manifest(self, job_id: str) -> dict[str, Any]:
        """Get job manifest.
        
        Args:
            job_id: Job UUID
            
        Returns:
            Build manifest
        """
        response = self.session.get(
            f"{self.base_url}/pipeline/jobs/{job_id}/manifest"
        )
        response.raise_for_status()
        return response.json()
    
    def get_job_logs(self, job_id: str) -> dict[str, Any]:
        """Get job logs.
        
        Args:
            job_id: Job UUID
            
        Returns:
            Job logs
        """
        response = self.session.get(
            f"{self.base_url}/pipeline/jobs/{job_id}/logs"
        )
        response.raise_for_status()
        return response.json()
    
    def wait_for_completion(
        self,
        job_id: str,
        poll_interval: int = 5,
        timeout: int = 3600,
        verbose: bool = True
    ) -> dict[str, Any]:
        """Wait for job to complete.
        
        Args:
            job_id: Job UUID
            poll_interval: Seconds between polls
            timeout: Maximum wait time in seconds
            verbose: Print progress updates
            
        Returns:
            Final job status
            
        Raises:
            TimeoutError: If job doesn't complete within timeout
        """
        start_time = time.time()
        
        while True:
            elapsed = time.time() - start_time
            
            if elapsed > timeout:
                raise TimeoutError(f"Job {job_id} did not complete within {timeout}s")
            
            status = self.get_job_status(job_id)
            current_status = status["status"]
            
            if verbose:
                print(f"[{elapsed:.0f}s] Job {job_id}: {current_status}")
            
            if current_status in ["completed", "failed"]:
                return status
            
            time.sleep(poll_interval)


def test_health_check(client: PipelineAPIClient):
    """Test health check endpoint."""
    print("\n" + "="*80)
    print("TEST: Health Check")
    print("="*80)
    
    result = client.health_check()
    print(f"✓ Health: {result['status']}")
    print(f"  Timestamp: {result['timestamp']}")
    
    assert result["status"] == "healthy"
    print("✓ PASSED")


def test_service_info(client: PipelineAPIClient):
    """Test service info endpoint."""
    print("\n" + "="*80)
    print("TEST: Service Info")
    print("="*80)
    
    result = client.get_service_info()
    print(f"✓ Service: {result['service']}")
    print(f"  Version: {result['version']}")
    print(f"  Status: {result['status']}")
    print(f"  Endpoints: {len(result['endpoints'])} available")
    
    assert result["service"] == "Dataset Pipeline API"
    print("✓ PASSED")


def test_list_jobs(client: PipelineAPIClient):
    """Test list jobs endpoint."""
    print("\n" + "="*80)
    print("TEST: List Jobs")
    print("="*80)
    
    result = client.list_jobs()
    print(f"✓ Total jobs: {result['total']}")
    
    if result["total"] > 0:
        print("\n  Recent jobs:")
        for job in result["jobs"][:5]:
            print(f"    - {job['job_id']}: {job['status']}")
    
    print("✓ PASSED")


def test_full_pipeline_workflow(client: PipelineAPIClient, config_path: str | None = None):
    """Test complete pipeline workflow."""
    print("\n" + "="*80)
    print("TEST: Complete Pipeline Workflow")
    print("="*80)
    
    # 1. Trigger pipeline
    print("\n1. Triggering pipeline...")
    trigger_result = client.trigger_pipeline(config_path)
    job_id = trigger_result["job_id"]
    print(f"   ✓ Job created: {job_id}")
    print(f"   Status: {trigger_result['status']}")
    print(f"   Created: {trigger_result['created_at']}")
    
    # 2. Check initial status
    print("\n2. Checking initial status...")
    status = client.get_job_status(job_id)
    print(f"   ✓ Status: {status['status']}")
    print(f"   Config: {status['config_path']}")
    
    # 3. Wait for completion
    print("\n3. Waiting for completion...")
    print("   (This may take several minutes)\n")
    
    try:
        final_status = client.wait_for_completion(
            job_id,
            poll_interval=5,
            timeout=3600,
            verbose=True
        )
        
        print(f"\n   ✓ Final status: {final_status['status']}")
        
        if final_status["status"] == "completed":
            print("   ✓ Pipeline completed successfully!")
            
            # 4. Get manifest
            print("\n4. Retrieving manifest...")
            manifest = client.get_job_manifest(job_id)
            
            print(f"   ✓ Pipeline version: {manifest['pipeline']['version']}")
            print(f"   ✓ Duration: {manifest['timing']['total_duration_formatted']}")
            
            print("\n   Metrics:")
            for key, value in manifest["metrics"].items():
                print(f"     {key:25s}: {value:>10,}")
            
            print("\n   Stage Durations:")
            for stage, duration in manifest["timing"]["stage_durations_seconds"].items():
                print(f"     {stage:30s}: {duration:>6.2f}s")
            
            print("\n   Outputs:")
            for name, path in manifest["outputs"].items():
                print(f"     {name:25s}: {path}")
            
            print("\n✓ WORKFLOW PASSED")
            return True
            
        else:
            print(f"   ✗ Pipeline failed: {final_status.get('error', 'Unknown error')}")
            
            # Get logs
            print("\n4. Retrieving logs...")
            logs_result = client.get_job_logs(job_id)
            print("\n   Last 10 log lines:")
            for log in logs_result["logs"][-10:]:
                print(f"     {log}")
            
            print("\n✗ WORKFLOW FAILED")
            return False
            
    except TimeoutError as e:
        print(f"\n   ✗ Timeout: {e}")
        print("\n✗ WORKFLOW FAILED")
        return False


def run_all_tests(
    base_url: str = "http://localhost:8000",
    run_pipeline: bool = False,
    config_path: str | None = None
):
    """Run all API tests.
    
    Args:
        base_url: API base URL
        run_pipeline: Whether to test full pipeline execution (slow)
        config_path: Optional custom config for pipeline test
    """
    print("\n" + "#"*80)
    print("#" + " "*78 + "#")
    print("#" + " "*20 + "DATASET PIPELINE API - TEST SUITE" + " "*25 + "#")
    print("#" + " "*78 + "#")
    print("#"*80)
    
    print(f"\nBase URL: {base_url}")
    
    client = PipelineAPIClient(base_url)
    
    # Basic tests
    try:
        test_health_check(client)
        test_service_info(client)
        test_list_jobs(client)
        
        # Optional: Full pipeline test
        if run_pipeline:
            test_full_pipeline_workflow(client, config_path)
        else:
            print("\n" + "="*80)
            print("SKIPPING: Full pipeline workflow test")
            print("Run with --pipeline flag to test pipeline execution")
            print("="*80)
        
        print("\n" + "#"*80)
        print("#" + " "*78 + "#")
        print("#" + " "*25 + "ALL TESTS COMPLETED" + " "*34 + "#")
        print("#" + " "*78 + "#")
        print("#"*80 + "\n")
        
    except Exception as e:
        print(f"\n✗ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Test Dataset Pipeline API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Quick tests (no pipeline execution)
  python test_api_client.py
  
  # Full test including pipeline execution
  python test_api_client.py --pipeline
  
  # Custom API URL
  python test_api_client.py --url http://api.example.com
  
  # Test with custom config
  python test_api_client.py --pipeline --config configs/test.yaml
        """
    )
    
    parser.add_argument(
        "--url",
        default="http://localhost:8000",
        help="API base URL (default: http://localhost:8000)"
    )
    parser.add_argument(
        "--pipeline",
        action="store_true",
        help="Run full pipeline execution test (slow)"
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Custom config path for pipeline test"
    )
    
    args = parser.parse_args()
    
    success = run_all_tests(
        base_url=args.url,
        run_pipeline=args.pipeline,
        config_path=args.config
    )
    
    exit(0 if success else 1)
