#!/usr/bin/env python3
"""Analyze merge thresholds to determine optimal value"""

import polars as pl
import sys

def analyze_thresholds(state_name):
    """Analyze what happens at different thresholds"""
    print(f"\n{'='*70}")
    print(f"ANALYZING MERGE THRESHOLDS FOR {state_name.upper()}")
    print(f"{'='*70}\n")
    
    df_200m = pl.read_parquet(f'data/streetdfs/{state_name}_streets.parquet')
    df_1km = pl.read_parquet(f'data/streetdfs/{state_name}_streets_1km.parquet')
    
    # Compare segment counts
    print("SEGMENT DISTRIBUTION:")
    print("\n200m threshold:")
    seg_dist_200m = df_200m.group_by('num_segments').agg(pl.len().alias('count')).sort('num_segments')
    print(seg_dist_200m)
    
    print("\n1km threshold:")
    seg_dist_1km = df_1km.group_by('num_segments').agg(pl.len().alias('count')).sort('num_segments')
    print(seg_dist_1km)
    
    # Look at streets that merged differently
    print("\n" + "="*70)
    print("STREETS THAT MERGED DIFFERENTLY:")
    print("="*70)
    
    # Find streets that exist in both but have different segment counts
    merged_200m = df_200m.group_by('street_name').agg([
        pl.len().alias('count_200m'),
        pl.sum('num_segments').alias('total_segments_200m'),
        pl.mean('length_km').alias('avg_length_200m')
    ])
    
    merged_1km = df_1km.group_by('street_name').agg([
        pl.len().alias('count_1km'),
        pl.sum('num_segments').alias('total_segments_1km'),
        pl.mean('length_km').alias('avg_length_1km')
    ])
    
    comparison = merged_200m.join(merged_1km, on='street_name', how='inner')
    
    # Streets that merged more at 1km (fewer instances but more segments)
    more_merged = comparison.filter(
        (pl.col('count_200m') > pl.col('count_1km')) &
        (pl.col('total_segments_1km') > pl.col('total_segments_200m'))
    ).sort('count_200m', descending=True)
    
    print(f"\nStreets that merged more at 1km threshold ({more_merged.height} total):")
    print("(Fewer instances but more segments per instance)")
    if more_merged.height > 0:
        print(more_merged.head(15))
    
    # Look at length statistics
    print("\n" + "="*70)
    print("LENGTH STATISTICS:")
    print("="*70)
    
    if 'length_km' in df_200m.columns and 'length_km' in df_1km.columns:
        print("\n200m threshold:")
        print(f"  Total length: {df_200m['length_km'].sum():,.2f} km")
        print(f"  Mean: {df_200m['length_km'].mean():.4f} km")
        print(f"  Median: {df_200m['length_km'].median():.4f} km")
        print(f"  Q75: {df_200m['length_km'].quantile(0.75):.4f} km")
        print(f"  Q90: {df_200m['length_km'].quantile(0.90):.4f} km")
        print(f"  Q95: {df_200m['length_km'].quantile(0.95):.4f} km")
        print(f"  Max: {df_200m['length_km'].max():.2f} km")
        
        print("\n1km threshold:")
        print(f"  Total length: {df_1km['length_km'].sum():,.2f} km")
        print(f"  Mean: {df_1km['length_km'].mean():.4f} km")
        print(f"  Median: {df_1km['length_km'].median():.4f} km")
        print(f"  Q75: {df_1km['length_km'].quantile(0.75):.4f} km")
        print(f"  Q90: {df_1km['length_km'].quantile(0.90):.4f} km")
        print(f"  Q95: {df_1km['length_km'].quantile(0.95):.4f} km")
        print(f"  Max: {df_1km['length_km'].max():.2f} km")
    
    # Analyze single-segment vs multi-segment streets
    print("\n" + "="*70)
    print("SINGLE vs MULTI-SEGMENT ANALYSIS:")
    print("="*70)
    
    single_200m = df_200m.filter(pl.col('num_segments') == 1)
    multi_200m = df_200m.filter(pl.col('num_segments') > 1)
    
    single_1km = df_1km.filter(pl.col('num_segments') == 1)
    multi_1km = df_1km.filter(pl.col('num_segments') > 1)
    
    print(f"\n200m threshold:")
    print(f"  Single-segment: {single_200m.height:,} ({single_200m.height/df_200m.height*100:.1f}%)")
    print(f"  Multi-segment: {multi_200m.height:,} ({multi_200m.height/df_200m.height*100:.1f}%)")
    if 'length_km' in df_200m.columns:
        print(f"  Avg length (single): {single_200m['length_km'].mean():.4f} km")
        print(f"  Avg length (multi): {multi_200m['length_km'].mean():.4f} km")
    
    print(f"\n1km threshold:")
    print(f"  Single-segment: {single_1km.height:,} ({single_1km.height/df_1km.height*100:.1f}%)")
    print(f"  Multi-segment: {multi_1km.height:,} ({multi_1km.height/df_1km.height*100:.1f}%)")
    if 'length_km' in df_1km.columns:
        print(f"  Avg length (single): {single_1km['length_km'].mean():.4f} km")
        print(f"  Avg length (multi): {multi_1km['length_km'].mean():.4f} km")
    
    # Recommendation
    print("\n" + "="*70)
    print("RECOMMENDATION:")
    print("="*70)
    
    reduction_pct = ((df_1km.height / df_200m.height - 1) * 100)
    
    print(f"\nKey observations:")
    print(f"  • 1km threshold reduces unique streets by {abs(reduction_pct):.2f}% vs 200m")
    print(f"  • This suggests most merges happen within 200m")
    print(f"  • Only {df_1km.height - df_200m.height:,} additional streets merge between 200m-1km")
    
    if reduction_pct > -2:
        print(f"\n  → 200m appears to be a good balance:")
        print(f"    - Captures most legitimate connections")
        print(f"    - Avoids merging distinct streets that are far apart")
        print(f"    - Only ~1% difference suggests diminishing returns beyond 200m")
    else:
        print(f"\n  → Consider a threshold between 200m and 1km")
    
    print()

if __name__ == '__main__':
    state = sys.argv[1] if len(sys.argv) > 1 else 'delaware'
    analyze_thresholds(state)

