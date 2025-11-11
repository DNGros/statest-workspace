#!/usr/bin/env python3
"""
Process all downloaded OSM files to parquet format.

Usage:
    python process_all_states.py
    
This script will:
- Find all OSM PBF files in data/osm/
- Process each to parquet format in data/streetdfs/
- Skip states that are already processed
- Log any errors to process_errors.log
- Continue processing even if some states fail
"""

import sys
from pathlib import Path
from datetime import datetime
from data_processing.process_osm_to_parquet import process_osm_to_parquet


def process_all_states():
    """Process all OSM files to parquet with error handling."""
    
    # Setup directories
    script_dir = Path(__file__).parent.parent
    osm_dir = script_dir / "data" / "osm"
    streetdfs_dir = script_dir / "data" / "streetdfs"
    
    # Get all OSM files
    osm_files = sorted(osm_dir.glob("*-latest.osm.pbf"))
    
    if not osm_files:
        print("No OSM files found in data/osm/")
        print("Please run download_all_states.py first.")
        sys.exit(1)
    
    print("\n" + "=" * 70)
    print("PROCESSING OSM FILES TO PARQUET")
    print("=" * 70)
    print(f"Total OSM files:   {len(osm_files)}")
    print(f"Input directory:   {osm_dir}")
    print(f"Output directory:  {streetdfs_dir}")
    print("=" * 70)
    
    # Track progress
    successful = []
    skipped = []
    failed = []
    
    # Create error log file
    log_file = Path("process_errors.log")
    
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
        
        # Process the file
        try:
            print(f"Processing {state_name}...")
            process_osm_to_parquet(osm_file, state_name)
            successful.append(state_name)
            print(f"✓ Successfully processed: {state_name}")
            print(f"\nProgress: {len(skipped) + len(successful)}/{len(osm_files)} states processed")
        except Exception as e:
            error_msg = f"✗ Failed to process {state_name}: {e}"
            print(error_msg)
            failed.append((state_name, str(e)))
            
            # Log error
            with open(log_file, "a") as f:
                timestamp = datetime.now().isoformat()
                f.write(f"[{timestamp}] {error_msg}\n")
            
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


