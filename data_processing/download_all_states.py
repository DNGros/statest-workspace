#!/usr/bin/env python3
"""
Download OSM data for all US states from Geofabrik.

Usage:
    python download_all_states.py
    
This script will:
- Download OSM data for all 50 US states
- Skip states that are already downloaded
- Log any errors to download_errors.log
- Continue downloading even if some states fail
"""

import sys
from pathlib import Path
from datetime import datetime
from data_processing.download_state_osm import download_state_osm, setup_data_directory
from workspace.states import USState


def download_all_states():
    """Download OSM data for all US states with error handling."""
    print("hello world")
    # Setup directories
    osm_dir = setup_data_directory()
    print("setup director")
    
    # Get all state names
    all_states = USState.all_names()
    print("got states")
    
    print("\n" + "=" * 70)
    print("DOWNLOADING OSM DATA FOR ALL US STATES")
    print("=" * 70)
    print(f"Total states:      {len(all_states)}")
    print(f"Output directory:  {osm_dir}")
    print(f"Source:            Geofabrik (download.geofabrik.de)")
    print("=" * 70)
    
    # Track progress
    successful = []
    skipped = []
    failed = []
    
    # Create error log file
    log_file = Path("download_errors.log")
    
    for i, state_name in enumerate(all_states, 1):
        print("\n" + "=" * 70)
        print(f"STATE {i}/{len(all_states)}: {state_name.upper()}")
        print("=" * 70)
        
        # Convert state name to filename format (spaces to hyphens)
        state_filename = state_name.replace(" ", "-")
        expected_file = osm_dir / f"{state_filename}-latest.osm.pbf"
        
        # Skip if already downloaded
        if expected_file.exists():
            file_size_mb = expected_file.stat().st_size / (1024 * 1024)
            print(f"✓ Already downloaded")
            print(f"  File: {expected_file.name}")
            print(f"  Size: {file_size_mb:.2f} MB")
            skipped.append(state_name)
            print(f"\nProgress: {len(skipped) + len(successful)}/{len(all_states)} states processed")
            continue
        
        # Try to download
        try:
            print(f"Downloading {state_name}...")
            download_state_osm(state_name)
            successful.append(state_name)
            print(f"✓ Successfully downloaded: {state_name}")
            print(f"\nProgress: {len(skipped) + len(successful)}/{len(all_states)} states processed")
        except Exception as e:
            error_msg = f"✗ Failed to download {state_name}: {e}"
            print(error_msg)
            failed.append((state_name, str(e)))
            
            # Log error
            with open(log_file, "a") as f:
                timestamp = datetime.now().isoformat()
                f.write(f"[{timestamp}] {error_msg}\n")
            
            print(f"\nProgress: {len(skipped) + len(successful)}/{len(all_states)} states processed ({len(failed)} failed)")
    
    # Print summary
    print("\n" + "=" * 70)
    print("DOWNLOAD SUMMARY")
    print("=" * 70)
    print(f"Total states:              {len(all_states)}")
    print(f"Already downloaded:        {len(skipped)}")
    print(f"Newly downloaded:          {len(successful)}")
    print(f"Failed:                    {len(failed)}")
    print(f"Total available:           {len(skipped) + len(successful)}")
    print("=" * 70)
    
    if successful:
        print(f"\n✓ Newly downloaded ({len(successful)}):")
        for state in successful:
            print(f"  • {state}")
    
    if failed:
        print(f"\n✗ Failed downloads ({len(failed)}):")
        for state, error in failed:
            print(f"  • {state}")
            print(f"    Error: {error}")
        print(f"\nErrors logged to: {log_file}")
    
    print("\n" + "=" * 70)
    
    # Exit with error code if any downloads failed
    if failed:
        sys.exit(1)
    else:
        print("All downloads completed successfully!")
        sys.exit(0)


if __name__ == "__main__":
    download_all_states()

