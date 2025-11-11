"""Simple file-based caching utility for expensive computations.

This module provides a make-style caching mechanism that invalidates cache
based on input parameters and modification times of dependent files.
"""

import hashlib
import json
from pathlib import Path
from typing import Any, Callable, Optional
import polars as pl


class FileCache:
    """
    Simple file-based cache for DataFrames with dependency tracking.
    
    Caches computation results as parquet files and invalidates based on:
    - Changes to input parameters
    - Changes to dependent file modification times
    
    Example:
        >>> cache = FileCache(cache_dir=Path("workspace/data/cache"))
        >>> 
        >>> def expensive_computation():
        ...     # Load and process data
        ...     return df
        >>> 
        >>> params = {"state": "california", "filter": True}
        >>> dependencies = [Path("data/california.parquet")]
        >>> 
        >>> df = cache.get_or_compute(
        ...     key="my_computation",
        ...     params=params,
        ...     dependencies=dependencies,
        ...     compute_fn=expensive_computation
        ... )
    """
    
    def __init__(self, cache_dir: Path):
        """
        Initialize the cache.
        
        Args:
            cache_dir: Directory to store cached parquet files
        """
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def _compute_hash(self, key: str, params: dict[str, Any], dependencies: list[Path]) -> str:
        """
        Compute a hash from the key, parameters, and file modification times.
        
        Args:
            key: Base key for the computation
            params: Dictionary of parameters (must be JSON-serializable)
            dependencies: List of file paths whose modification times affect the cache
            
        Returns:
            Hex string hash
        """
        hasher = hashlib.sha256()
        
        # Hash the key
        hasher.update(key.encode('utf-8'))
        
        # Hash the parameters (sorted for deterministic ordering)
        params_str = json.dumps(params, sort_keys=True, default=str)
        hasher.update(params_str.encode('utf-8'))
        
        # Hash the modification times of dependencies
        for dep_path in sorted(dependencies):
            if dep_path.exists():
                mtime = dep_path.stat().st_mtime
                hasher.update(f"{dep_path}:{mtime}".encode('utf-8'))
            else:
                # If file doesn't exist, include that in the hash
                hasher.update(f"{dep_path}:missing".encode('utf-8'))
        
        return hasher.hexdigest()[:16]  # Use first 16 chars for shorter filenames
    
    def _get_cache_path(self, cache_hash: str, key: str) -> Path:
        """Get the path to the cached parquet file."""
        return self.cache_dir / f"{key}_{cache_hash}.parquet"
    
    def _get_metadata_path(self, cache_hash: str, key: str) -> Path:
        """Get the path to the cache metadata file."""
        return self.cache_dir / f"{key}_{cache_hash}.json"
    
    def get_or_compute(
        self,
        key: str,
        params: dict[str, Any],
        dependencies: list[Path],
        compute_fn: Callable[[], pl.DataFrame],
        force_recompute: bool = False,
    ) -> pl.DataFrame:
        """
        Get cached result or compute if cache is invalid.
        
        Args:
            key: Base key for the computation (e.g., "state_streets")
            params: Dictionary of parameters that affect the computation
            dependencies: List of file paths that the computation depends on
            compute_fn: Function that computes the result (returns a DataFrame)
            force_recompute: If True, ignore cache and recompute
            
        Returns:
            DataFrame with the computation result
        """
        # Compute hash from inputs
        cache_hash = self._compute_hash(key, params, dependencies)
        cache_path = self._get_cache_path(cache_hash, key)
        metadata_path = self._get_metadata_path(cache_hash, key)
        
        # Check if cache exists and is valid
        if not force_recompute and cache_path.exists():
            try:
                # Load cached result
                df = pl.read_parquet(cache_path)
                print(f"✓ Cache hit: {cache_path.name}")
                return df
            except Exception as e:
                print(f"⚠ Cache read failed ({e}), recomputing...")
        
        # Cache miss or invalid - compute result
        print(f"✗ Cache miss: computing {key}...")
        df = compute_fn()
        
        # Save to cache
        try:
            df.write_parquet(cache_path)
            
            # Save metadata for debugging
            metadata = {
                "key": key,
                "params": params,
                "dependencies": [str(p) for p in dependencies],
                "cache_hash": cache_hash,
            }
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            print(f"✓ Cached result: {cache_path.name}")
        except Exception as e:
            print(f"⚠ Failed to cache result: {e}")
        
        return df
    
    def clear(self, key: Optional[str] = None):
        """
        Clear cached files.
        
        Args:
            key: If provided, only clear cache files for this key.
                 If None, clear all cache files.
        """
        if key is None:
            # Clear all cache files
            pattern = "*"
        else:
            # Clear only files for this key
            pattern = f"{key}_*"
        
        removed_count = 0
        for cache_file in self.cache_dir.glob(pattern):
            if cache_file.suffix in ['.parquet', '.json']:
                cache_file.unlink()
                removed_count += 1
        
        print(f"Cleared {removed_count} cache file(s)")
    
    def list_cache(self) -> list[dict[str, Any]]:
        """
        List all cached items with their metadata.
        
        Returns:
            List of metadata dictionaries for each cached item
        """
        cache_items = []
        for metadata_file in sorted(self.cache_dir.glob("*.json")):
            try:
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                    
                # Add file size and modification time
                parquet_file = metadata_file.with_suffix('.parquet')
                if parquet_file.exists():
                    stat = parquet_file.stat()
                    metadata['size_mb'] = stat.st_size / (1024 * 1024)
                    metadata['cached_at'] = stat.st_mtime
                    
                cache_items.append(metadata)
            except Exception:
                continue
        
        return cache_items

