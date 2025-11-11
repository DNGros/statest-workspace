#!/usr/bin/env python3
"""Command-line utility for managing the cache."""

import argparse
from pathlib import Path
from datetime import datetime
from cache_utils import FileCache

DEFAULT_CACHE_DIR = Path(__file__).parent / "data" / "cache"


def list_cache(cache_dir: Path):
    """List all cached items."""
    cache = FileCache(cache_dir)
    items = cache.list_cache()
    
    if not items:
        print("Cache is empty.")
        return
    
    print(f"\nCache directory: {cache_dir}")
    print(f"Total items: {len(items)}\n")
    print("=" * 80)
    
    total_size_mb = 0
    for i, item in enumerate(items, 1):
        print(f"\n{i}. {item['key']} (hash: {item['cache_hash']})")
        print(f"   Size: {item['size_mb']:.2f} MB")
        
        cached_time = datetime.fromtimestamp(item['cached_at'])
        print(f"   Cached: {cached_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        print(f"   Parameters:")
        for key, value in item['params'].items():
            print(f"     - {key}: {value}")
        
        print(f"   Dependencies: {len(item['dependencies'])} file(s)")
        
        total_size_mb += item['size_mb']
    
    print("\n" + "=" * 80)
    print(f"Total cache size: {total_size_mb:.2f} MB\n")


def clear_cache(cache_dir: Path, key: str = None):
    """Clear cache files."""
    cache = FileCache(cache_dir)
    
    if key:
        print(f"Clearing cache for key: {key}")
    else:
        print("Clearing all cache files...")
    
    cache.clear(key=key)
    print("Done!")


def main():
    parser = argparse.ArgumentParser(
        description='Manage the street data cache',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all cached items
  python manage_cache.py list
  
  # Clear all cache
  python manage_cache.py clear
  
  # Clear only state_streets cache
  python manage_cache.py clear --key state_streets
  
  # Use custom cache directory
  python manage_cache.py list --cache-dir /path/to/cache
        """
    )
    
    parser.add_argument(
        'command',
        choices=['list', 'clear'],
        help='Command to execute'
    )
    
    parser.add_argument(
        '--key',
        type=str,
        help='Cache key to operate on (for clear command)'
    )
    
    parser.add_argument(
        '--cache-dir',
        type=Path,
        default=DEFAULT_CACHE_DIR,
        help=f'Cache directory (default: {DEFAULT_CACHE_DIR})'
    )
    
    args = parser.parse_args()
    
    # Ensure cache directory exists
    args.cache_dir.mkdir(parents=True, exist_ok=True)
    
    if args.command == 'list':
        list_cache(args.cache_dir)
    elif args.command == 'clear':
        clear_cache(args.cache_dir, key=args.key)


if __name__ == "__main__":
    main()

