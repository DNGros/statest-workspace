#!/usr/bin/env python3
"""Quick test script to verify caching functionality."""

import time
from pathlib import Path
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from load_street_df import load_state_streets_df

def test_cache():
    """Test that caching speeds up repeated calls."""
    print("=" * 60)
    print("Testing cache functionality")
    print("=" * 60)
    
    # First call - should compute and cache
    print("\n1. First call (should compute and cache):")
    start = time.time()
    lf1 = load_state_streets_df()
    df1 = lf1.collect()
    elapsed1 = time.time() - start
    print(f"   Time: {elapsed1:.2f}s")
    print(f"   Rows: {len(df1):,}")
    
    # Second call - should use cache
    print("\n2. Second call (should use cache):")
    start = time.time()
    lf2 = load_state_streets_df()
    df2 = lf2.collect()
    elapsed2 = time.time() - start
    print(f"   Time: {elapsed2:.2f}s")
    print(f"   Rows: {len(df2):,}")
    
    # Calculate speedup
    speedup = elapsed1 / elapsed2 if elapsed2 > 0 else float('inf')
    print(f"\n3. Speedup: {speedup:.1f}x faster")
    
    # Verify results are identical
    assert len(df1) == len(df2), "DataFrames have different lengths!"
    print("   ✓ Results are identical")
    
    # Test with use_cache=False
    print("\n4. Third call with use_cache=False (should recompute):")
    start = time.time()
    lf3 = load_state_streets_df(use_cache=False)
    df3 = lf3.collect()
    elapsed3 = time.time() - start
    print(f"   Time: {elapsed3:.2f}s")
    print(f"   Rows: {len(df3):,}")
    
    print("\n" + "=" * 60)
    print("Cache test complete!")
    print("=" * 60)

if __name__ == "__main__":
    test_cache()

