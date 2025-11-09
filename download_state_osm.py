#!/usr/bin/env python3
"""
Download OSM data for a US state from Geofabrik.

Usage:
    python download_state_osm.py [state_name]
    
Example:
    python download_state_osm.py california
    python download_state_osm.py texas
"""

import sys
import os
from pathlib import Path
from urllib.request import urlretrieve
from urllib.error import URLError


def setup_data_directory() -> Path:
    """
    Check if data directory exists, and if not, prompt user for symlink target.
    Creates osm/ subdirectory for raw OSM files.
    
    Returns:
        Path to the data/osm directory
    """
    data_dir = Path("data")
    
    # Check if data directory exists and is valid
    if data_dir.exists():
        if data_dir.is_symlink():
            # Check if symlink is valid (not broken)
            if data_dir.resolve().exists():
                pass  # Continue to create osm subdirectory
            else:
                print(f"Warning: 'data' is a broken symlink. Removing it...")
                data_dir.unlink()
        elif data_dir.is_dir():
            pass  # Continue to create osm subdirectory
        else:
            print(f"Error: 'data' exists but is not a directory or symlink.")
            sys.exit(1)
    
    # data directory doesn't exist, prompt user for symlink target
    if not data_dir.exists():
        print("The 'data' directory does not exist.")
        print("Please provide the path where you want to store OSM data files.")
        print("(This will create a symlink from 'data' to your specified location)")
        
        while True:
            target_path = input("Enter target path for data directory: ").strip()
            
            if not target_path:
                print("Path cannot be empty. Please try again.")
                continue
            
            # Expand user home directory
            target_path = os.path.expanduser(target_path)
            target = Path(target_path)
            
            # Create target directory if it doesn't exist
            if not target.exists():
                create = input(f"Directory '{target}' does not exist. Create it? [y/N]: ").strip().lower()
                if create == 'y':
                    try:
                        target.mkdir(parents=True, exist_ok=True)
                        print(f"Created directory: {target}")
                    except Exception as e:
                        print(f"Error creating directory: {e}")
                        continue
                else:
                    print("Please provide an existing directory path.")
                    continue
            
            if not target.is_dir():
                print(f"Error: '{target}' exists but is not a directory.")
                continue
            
            # Create the symlink
            try:
                data_dir.symlink_to(target.resolve())
                print(f"Created symlink: {data_dir} -> {target.resolve()}")
                break
            except OSError as e:
                print(f"Error creating symlink: {e}")
                print("Please try again with a different path.")
                continue
    
    # Create osm subdirectory
    osm_dir = data_dir / "osm"
    osm_dir.mkdir(exist_ok=True)
    
    return osm_dir


def download_state_osm(state_name: str = "california") -> None:
    """
    Download OSM data for a given US state.
    
    Args:
        state_name: Name of the state (case-insensitive)
    """
    state_lower = state_name.lower()
    
    # Convert spaces to hyphens for URL and filename
    state_url = state_lower.replace(" ", "-")
    
    base_url = "https://download.geofabrik.de/north-america/us"
    filename = f"{state_url}-latest.osm.pbf"
    url = f"{base_url}/{filename}"
    
    # Setup data directory (check for symlink, prompt if needed)
    # Returns data/osm directory
    osm_dir = setup_data_directory()
    output_path = osm_dir / filename
    
    # Removed verbose output - caller can print what they need
    
    import time
    start_time = time.time()
    last_downloaded = [0]  # Use list to allow modification in nested function
    last_time = [start_time]
    
    def show_progress(block_num, block_size, total_size):
        """Show download progress with speed and ETA."""
        downloaded = block_num * block_size
        percent = min(100, (downloaded / total_size) * 100) if total_size > 0 else 0
        size_mb = total_size / (1024 * 1024) if total_size > 0 else 0
        downloaded_mb = downloaded / (1024 * 1024)
        
        # Calculate speed and ETA
        current_time = time.time()
        time_diff = current_time - last_time[0]
        
        if time_diff >= 0.5:  # Update speed every 0.5 seconds
            bytes_diff = downloaded - last_downloaded[0]
            speed_mbps = (bytes_diff / (1024 * 1024)) / time_diff if time_diff > 0 else 0
            
            # Calculate ETA
            if speed_mbps > 0 and downloaded < total_size:
                remaining_mb = size_mb - downloaded_mb
                eta_seconds = remaining_mb / speed_mbps
                eta_str = f"{int(eta_seconds // 60)}m {int(eta_seconds % 60)}s"
            else:
                eta_str = "--"
            
            last_downloaded[0] = downloaded
            last_time[0] = current_time
        else:
            # Use previous values
            if last_downloaded[0] > 0:
                time_elapsed = current_time - start_time
                speed_mbps = downloaded_mb / time_elapsed if time_elapsed > 0 else 0
                if speed_mbps > 0 and downloaded < total_size:
                    remaining_mb = size_mb - downloaded_mb
                    eta_seconds = remaining_mb / speed_mbps
                    eta_str = f"{int(eta_seconds // 60)}m {int(eta_seconds % 60)}s"
                else:
                    eta_str = "--"
            else:
                speed_mbps = 0
                eta_str = "--"
        
        # Progress bar
        bar_length = 30
        filled = int(bar_length * percent / 100)
        bar = '=' * filled + '-' * (bar_length - filled)
        
        print(f"\r[{bar}] {percent:.1f}% {downloaded_mb:.1f}/{size_mb:.1f} MB | {speed_mbps:.2f} MB/s | ETA: {eta_str}", end='', flush=True)
    
    try:
        urlretrieve(url, output_path, show_progress)
        print()  # New line after progress bar
        
        # Show final file size
        file_size = output_path.stat().st_size
        file_size_mb = file_size / (1024 * 1024)
        print(f"  Downloaded: {file_size_mb:.2f} MB")
        
    except URLError as e:
        print(f"\nError downloading file: {e}")
        print(f"Please check that the state name '{state_name}' is correct.")
        print("State names should be lowercase (e.g., 'california', 'texas', 'new-york')")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\nDownload cancelled by user.")
        if output_path.exists():
            output_path.unlink()
            print("Partial file removed.")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    state = sys.argv[1] if len(sys.argv) > 1 else "california"
    download_state_osm(state)

