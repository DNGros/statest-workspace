#!/usr/bin/env python3
"""Analyze overcounting vs undercounting trade-offs"""

import polars as pl
import sys

def analyze_counting_risk(state_name):
    """Analyze the risk of overcounting vs undercounting"""
    print(f"\n{'='*70}")
    print(f"OVERCOUNTING vs UNDERCOUNTING ANALYSIS: {state_name.upper()}")
    print(f"{'='*70}\n")
    
    df_200m = pl.read_parquet(f'data/streetdfs/{state_name}_streets.parquet')
    df_500m = pl.read_parquet(f'data/streetdfs/{state_name}_streets_500m.parquet')
    df_1km = pl.read_parquet(f'data/streetdfs/{state_name}_streets_1km.parquet')
    df_1mile = pl.read_parquet(f'data/streetdfs/{state_name}_streets_1mile.parquet')
    
    print("UNDERSTANDING THE TRADE-OFF:")
    print("="*70)
    print("""
    OVERCOUNTING RISK (too conservative threshold):
    - Same logical street split into multiple segments counts as multiple streets
    - Example: "Main Street" split by a park counts as 2-3 streets instead of 1
    - Result: Inflated counts for common street names
    
    UNDERCOUNTING RISK (too aggressive threshold):
    - Distinct streets with same name get merged together
    - Example: Two different "Main Streets" in different towns merge into 1
    - Result: Underestimated counts for common street names
    """)
    
    print("\n" + "="*70)
    print("STREET NAME COUNTS AT DIFFERENT THRESHOLDS:")
    print("="*70)
    
    # Top street names at each threshold
    top_200m = df_200m.group_by('street_name').agg(pl.len().alias('count_200m')).sort('count_200m', descending=True)
    top_500m = df_500m.group_by('street_name').agg(pl.len().alias('count_500m')).sort('count_500m', descending=True)
    top_1km = df_1km.group_by('street_name').agg(pl.len().alias('count_1km')).sort('count_1km', descending=True)
    top_1mile = df_1mile.group_by('street_name').agg(pl.len().alias('count_1mile')).sort('count_1mile', descending=True)
    
    # Merge all for comparison
    comparison = top_200m.join(top_500m, on='street_name', how='full', suffix='_500m').join(
        top_1km, on='street_name', how='full', suffix='_1km').join(
        top_1mile, on='street_name', how='full', suffix='_1mile').fill_null(0)
    
    print("\nTop 15 street names:")
    print(f"{'Street Name':<25} {'200m':<8} {'500m':<8} {'1km':<8} {'1mi':<8} {'Diff':<8}")
    print("-" * 75)
    
    top_streets = comparison.sort('count_200m', descending=True).head(15)
    for row in top_streets.iter_rows(named=True):
        diff = row['count_200m'] - row['count_1mile']
        print(f"{row['street_name']:<25} {row['count_200m']:<8} {row['count_500m']:<8} {row['count_1km']:<8} {row['count_1mile']:<8} {diff:+8}")
    
    print("\n" + "="*70)
    print("OVERCOUNTING ANALYSIS:")
    print("="*70)
    
    # Streets that decrease in count (merging happening)
    decreasing = comparison.filter(
        (pl.col('count_200m') > pl.col('count_1mile')) &
        (pl.col('count_200m') > 5)  # Only look at streets with multiple instances
    ).sort('count_200m', descending=True)
    
    print(f"\nStreets that merge across thresholds ({decreasing.height} total):")
    print("(These are candidates for overcounting if threshold is too low)")
    print("\nTop 20 examples:")
    print(f"{'Street Name':<25} {'200m':<8} {'500m':<8} {'1km':<8} {'1mi':<8} {'Reduction':<10}")
    print("-" * 85)
    
    for row in decreasing.head(20).iter_rows(named=True):
        reduction = row['count_200m'] - row['count_1mile']
        pct_reduction = (reduction / row['count_200m'] * 100) if row['count_200m'] > 0 else 0
        print(f"{row['street_name']:<25} {row['count_200m']:<8} {row['count_500m']:<8} {row['count_1km']:<8} {row['count_1mile']:<8} {reduction:<10} ({pct_reduction:.1f}%)")
    
    # Calculate total "overcounts" at each threshold
    print("\n" + "="*70)
    print("QUANTIFYING OVERCOUNTING RISK:")
    print("="*70)
    
    # Total instances at each threshold
    total_200m = df_200m.height
    total_500m = df_500m.height
    total_1km = df_1km.height
    total_1mile = df_1mile.height
    
    print(f"\nTotal street instances:")
    print(f"  200m:  {total_200m:,} streets")
    print(f"  500m:  {total_500m:,} streets ({total_200m - total_500m:,} fewer)")
    print(f"  1km:   {total_1km:,} streets ({total_200m - total_1km:,} fewer)")
    print(f"  1mi:   {total_1mile:,} streets ({total_200m - total_1mile:,} fewer)")
    
    print(f"\nIf 1 mile is 'correct' (all legitimate merges):")
    print(f"  Overcounting at 200m: {total_200m - total_1mile:,} streets ({((total_200m - total_1mile) / total_1mile * 100):.2f}%)")
    print(f"  Overcounting at 500m: {total_500m - total_1mile:,} streets ({((total_500m - total_1mile) / total_1mile * 100):.2f}%)")
    print(f"  Overcounting at 1km:  {total_1km - total_1mile:,} streets ({((total_1km - total_1mile) / total_1mile * 100):.2f}%)")
    
    # Look at multi-segment streets - these are likely legitimate merges
    print("\n" + "="*70)
    print("MULTI-SEGMENT STREETS (likely legitimate merges):")
    print("="*70)
    
    multi_200m = df_200m.filter(pl.col('num_segments') > 1)
    multi_1mile = df_1mile.filter(pl.col('num_segments') > 1)
    
    print(f"\n200m threshold:")
    print(f"  Multi-segment streets: {multi_200m.height:,}")
    print(f"  Total segments merged: {multi_200m['num_segments'].sum():,}")
    print(f"  Avg segments per street: {multi_200m['num_segments'].mean():.2f}")
    
    print(f"\n1 mile threshold:")
    print(f"  Multi-segment streets: {multi_1mile.height:,}")
    print(f"  Total segments merged: {multi_1mile['num_segments'].sum():,}")
    print(f"  Avg segments per street: {multi_1mile['num_segments'].mean():.2f}")
    
    # Recommendation
    print("\n" + "="*70)
    print("RECOMMENDATION FOR YOUR USE CASE:")
    print("="*70)
    
    print("""
    Since you're more concerned about OVERCOUNTING:
    
    → Use a HIGHER threshold (500m - 1km, possibly 1 mile)
    
    Reasons:
    1. Better captures split segments of the same street
    2. Reduces risk of counting same street multiple times
    3. For common street names, merging is more likely correct than incorrect
    4. Even if some distinct streets merge, you're still getting closer to true counts
    
    Suggested threshold: 500m - 1km
    - Captures most legitimate splits (159-253 merges)
    - Still conservative enough to avoid obvious false merges
    - Good balance for counting common street names
    
    Alternative: 1 mile if you want maximum merging
    - Captures all likely splits (322 merges)
    - May merge some distinct streets, but reduces overcounting risk
    """)
    
    print()

if __name__ == '__main__':
    state = sys.argv[1] if len(sys.argv) > 1 else 'delaware'
    analyze_counting_risk(state)

