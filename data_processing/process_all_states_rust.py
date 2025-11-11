#!/usr/bin/env python3
"""
Process all downloaded OSM files to parquet format using the Rust processor.

Usage:
    python process_all_states_rust.py
    
This script will:
- Find all OSM PBF files in data/osm/
- Process each using the Rust processor to data/streetdfs/
- Skip states that are already processed
- Log any errors to process_errors.log
- Continue processing even if some states fail
"""

import sys
import subprocess
from pathlib import Path
from datetime import datetime


def process_all_states():
    """Process all OSM files to parquet using Rust processor."""
    
    # Setup directories
    script_dir = Path(__file__).parent.parent
    osm_dir = script_dir / "data" / "osm"
    streetdfs_dir = script_dir / "data" / "streetdfs"
    rust_binary = script_dir / "osm_processor_rust" / "target" / "release" / "osm_processor_rust"
    
    # Check if Rust binary exists
    if not rust_binary.exists():
        print("ERROR: Rust binary not found!")
        print(f"Expected location: {rust_binary}")
        print("\nPlease build it first:")
        print("  cd osm_processor_rust")
        print("  cargo build --release")
        sys.exit(1)
    
    # Get all OSM files
    osm_files = sorted(osm_dir.glob("*-latest.osm.pbf"))
    
    if not osm_files:
        print("No OSM files found in data/osm/")
        print("Please run download_all_states.py first.")
        sys.exit(1)
    
    print("\n" + "=" * 70)
    print("PROCESSING OSM FILES TO PARQUET (RUST)")
    print("=" * 70)
    print(f"Total OSM files:   {len(osm_files)}")
    print(f"Input directory:   {osm_dir}")
    print(f"Output directory:  {streetdfs_dir}")
    print(f"Rust binary:       {rust_binary}")
    print("=" * 70)
    
    # Track progress
    successful = []
    skipped = []
    failed = []
    
    # Create error log file
    log_file = Path("process_errors_rust.log")
    
    for i, osm_file in enumerate(osm_files, 1):
        # Extract state name from filename (e.g., "delaware-latest.osm.pbf" -> "delaware")
        state_name = osm_file.stem.replace("-latest.osm", "")
        
        print("\n" + "=" * 70)
        print(f"STATE {i}/{len(osm_files)}: {state_name.upper()}")
        print("=" * 70)
        
        # Check if already processed
        expected_parquet = streetdfs_dir / f"{state_name}_streets.parquet"
        
        if expected_parquet.exists():
            file_size_mb = expected_parquet.stat().st_size / (1024 * 1024)
            print(f"✓ Already processed")
            print(f"  File: {expected_parquet.name}")
            print(f"  Size: {file_size_mb:.2f} MB")
            skipped.append(state_name)
            print(f"\nProgress: {len(skipped) + len(successful)}/{len(osm_files)} states processed")
            continue
        
        # Process the file using Rust
        try:
            print(f"Processing {state_name} with Rust...")
            
            # Call the Rust binary
            result = subprocess.run(
                [str(rust_binary), state_name, str(osm_file)],
                capture_output=True,
                text=True,
                check=True
            )
            
            # Print output
            print(result.stdout)
            
            successful.append(state_name)
            print(f"✓ Successfully processed: {state_name}")
            print(f"\nProgress: {len(skipped) + len(successful)}/{len(osm_files)} states processed")
            
        except subprocess.CalledProcessError as e:
            error_msg = f"✗ Failed to process {state_name}: {e}"
            print(error_msg)
            print("STDOUT:", e.stdout)
            print("STDERR:", e.stderr)
            failed.append((state_name, str(e)))
            
            # Log error
            with open(log_file, "a") as f:
                timestamp = datetime.now().isoformat()
                f.write(f"[{timestamp}] {error_msg}\n")
                f.write(f"STDOUT: {e.stdout}\n")
                f.write(f"STDERR: {e.stderr}\n\n")
            
            print(f"\nProgress: {len(skipped) + len(successful)}/{len(osm_files)} states processed ({len(failed)} failed)")
        
        except Exception as e:
            error_msg = f"✗ Unexpected error processing {state_name}: {e}"
            print(error_msg)
            failed.append((state_name, str(e)))
            
            # Log error
            with open(log_file, "a") as f:
                timestamp = datetime.now().isoformat()
                f.write(f"[{timestamp}] {error_msg}\n\n")
            
            print(f"\nProgress: {len(skipped) + len(successful)}/{len(osm_files)} states processed ({len(failed)} failed)")
    
    # Print summary
    print("\n" + "=" * 70)
    print("PROCESSING SUMMARY")
    print("=" * 70)
    print(f"Total OSM files:           {len(osm_files)}")
    print(f"Already processed:         {len(skipped)}")
    print(f"Newly processed:           {len(successful)}")
    print(f"Failed:                    {len(failed)}")
    print(f"Total available:           {len(skipped) + len(successful)}")
    print("=" * 70)
    
    if successful:
        print(f"\n✓ Newly processed ({len(successful)}):")
        for state in successful:
            print(f"  • {state}")
    
    if failed:
        print(f"\n✗ Failed processing ({len(failed)}):")
        for state, error in failed:
            print(f"  • {state}")
            print(f"    Error: {error}")
        print(f"\nErrors logged to: {log_file}")
    
    print("\n" + "=" * 70)
    
    # Exit with error code if any processing failed
    if failed:
        sys.exit(1)
    else:
        print("All processing completed successfully!")
        sys.exit(0)


if __name__ == "__main__":
    process_all_states()

