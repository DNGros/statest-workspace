# Caching System for Street Data

## Overview

A simple make-style caching system has been added to speed up repeated data loading operations. The cache automatically invalidates when input parameters or source data files change.

## Quick Start

### Using the Cache (Default Behavior)

```python
from workspace.load_street_df import load_state_streets_df

# First call - computes and caches
lf = load_state_streets_df()
df = lf.collect()

# Second call - uses cache (much faster!)
lf = load_state_streets_df()
df = lf.collect()
```

### Disabling the Cache

```python
# Force recomputation
lf = load_state_streets_df(use_cache=False)
```

## How It Works

### Caching Logic

The `load_state_streets_df()` function now caches its results based on:
1. **Input parameters**: `state`, `data_dir`, `filter_to_types`, `exclude_numbered`
2. **Source file modification times**: All parquet files that are loaded

When you call the function:
- A hash is computed from the parameters and file modification times
- If a cache file with that hash exists, it's loaded instantly
- If not, the computation runs and the result is cached for next time

### Cache Location

Cached files are stored in: `workspace/data/cache/`

Each cache entry consists of:
- `state_streets_{hash}.parquet` - The cached data
- `state_streets_{hash}.json` - Metadata for debugging

### Cache Invalidation

The cache automatically invalidates when:
- Any input parameter changes
- Any source parquet file is modified (based on mtime)
- The cache file is manually deleted

## Managing the Cache

### View Cache Contents

```python
from pathlib import Path
from workspace.cache_utils import FileCache

cache = FileCache(Path("workspace/data/cache"))
items = cache.list_cache()

for item in items:
    print(f"Key: {item['key']}")
    print(f"Size: {item['size_mb']:.1f} MB")
    print(f"Params: {item['params']}")
    print()
```

### Clear Cache

```python
from pathlib import Path
from workspace.cache_utils import FileCache

cache = FileCache(Path("workspace/data/cache"))

# Clear all cache files
cache.clear()

# Clear only state_streets cache files
cache.clear(key="state_streets")
```

Or manually delete files:
```bash
rm workspace/data/cache/*
```

## Testing

Run the test script to verify caching works:

```bash
conda run -n statest python -m workspace.test_cache
```

This will:
1. Load data (compute and cache)
2. Load data again (use cache - should be much faster)
3. Compare timing and verify results are identical

## Changes to `most_common_state_st.py`

The `extract_state_names_from_street_name()` function has been updated to return only the **longest state name** found in each street name. This solves the duplicate street issue when mapping.

**Before**: "West Virginia Ave" would match both "West Virginia" and "Virginia", creating two rows after exploding.

**After**: "West Virginia Ave" only matches "West Virginia", keeping one row per street.

This makes the data cleaner for mapping purposes while still accurately counting state name occurrences.

## Performance

Expected speedup depends on your data size, but typical improvements:
- **First call**: Normal speed (computes and caches)
- **Subsequent calls**: 10-50x faster (loads from cache)

The cache is especially useful during:
- Development and iteration
- Running the same script multiple times
- Generating multiple plots from the same data

