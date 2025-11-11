#!/usr/bin/env python3
"""Compare all thresholds including 1 mile"""

import polars as pl
import sys

def compare_all_thresholds(state_name):
    """Compare all thresholds"""
    print(f"\n{'='*70}")
    print(f"COMPARING ALL THRESHOLDS FOR {state_name.upper()}")
    print(f"{'='*70}\n")
    
    # Load dataframes
    df_200m = pl.read_parquet(f'data/streetdfs/{state_name}_streets.parquet')
    df_500m = pl.read_parquet(f'data/streetdfs/{state_name}_streets_500m.parquet')
    df_1km = pl.read_parquet(f'data/streetdfs/{state_name}_streets_1km.parquet')
    df_1mile = pl.read_parquet(f'data/streetdfs/{state_name}_streets_1mile.parquet')
    
    print("COMPARISON TABLE:")
    print(f"{'Metric':<35} {'200m':<12} {'500m':<12} {'1km':<12} {'1mi':<12}")
    print("-" * 83)
    
    # Unique streets
    print(f"{'Unique streets':<35} {df_200m.height:<12,} {df_500m.height:<12,} {df_1km.height:<12,} {df_1mile.height:<12,}")
    
    # Multi-segment streets
    multi_200m = df_200m.filter(pl.col('num_segments') > 1).height
    multi_500m = df_500m.filter(pl.col('num_segments') > 1).height
    multi_1km = df_1km.filter(pl.col('num_segments') > 1).height
    multi_1mile = df_1mile.filter(pl.col('num_segments') > 1).height
    print(f"{'Multi-segment streets':<35} {multi_200m:<12,} {multi_500m:<12,} {multi_1km:<12,} {multi_1mile:<12,}")
    
    # Length statistics
    if 'length_km' in df_200m.columns:
        total_200m = df_200m['length_km'].sum()
        total_500m = df_500m['length_km'].sum()
        total_1km = df_1km['length_km'].sum()
        total_1mile = df_1mile['length_km'].sum()
        print(f"{'Total length (km)':<35} {total_200m:<12,.2f} {total_500m:<12,.2f} {total_1km:<12,.2f} {total_1mile:<12,.2f}")
        
        avg_200m = df_200m['length_km'].mean()
        avg_500m = df_500m['length_km'].mean()
        avg_1km = df_1km['length_km'].mean()
        avg_1mile = df_1mile['length_km'].mean()
        print(f"{'Average length (km)':<35} {avg_200m:<12,.4f} {avg_500m:<12,.4f} {avg_1km:<12,.4f} {avg_1mile:<12,.4f}")
        
        median_200m = df_200m['length_km'].median()
        median_500m = df_500m['length_km'].median()
        median_1km = df_1km['length_km'].median()
        median_1mile = df_1mile['length_km'].median()
        print(f"{'Median length (km)':<35} {median_200m:<12,.4f} {median_500m:<12,.4f} {median_1km:<12,.4f} {median_1mile:<12,.4f}")
    
    print()
    
    # Calculate differences
    print("DIFFERENCES:")
    print(f"  200m → 500m:  {df_500m.height - df_200m.height:+,} streets ({((df_500m.height / df_200m.height - 1) * 100):+.2f}%)")
    print(f"  500m → 1km:   {df_1km.height - df_500m.height:+,} streets ({((df_1km.height / df_500m.height - 1) * 100):+.2f}%)")
    print(f"  1km → 1mi:    {df_1mile.height - df_1km.height:+,} streets ({((df_1mile.height / df_1km.height - 1) * 100):+.2f}%)")
    print(f"  200m → 1mi:   {df_1mile.height - df_200m.height:+,} streets ({((df_1mile.height / df_200m.height - 1) * 100):+.2f}%)")
    print()
    
    print(f"  Multi-segment: 200m→500m: {multi_500m - multi_200m:+,} ({((multi_500m / multi_200m - 1) * 100):+.2f}%)")
    print(f"  Multi-segment: 500m→1km:  {multi_1km - multi_500m:+,} ({((multi_1km / multi_500m - 1) * 100):+.2f}%)")
    print(f"  Multi-segment: 1km→1mi:   {multi_1mile - multi_1km:+,} ({((multi_1mile / multi_1km - 1) * 100):+.2f}%)")
    print()
    
    # Top street names
    print("TOP 10 STREET NAMES:")
    print("\n200m:")
    top_200m = df_200m.group_by('street_name').agg(pl.len().alias('count')).sort('count', descending=True).head(10)
    print(top_200m)
    
    print("\n1 mile:")
    top_1mile = df_1mile.group_by('street_name').agg(pl.len().alias('count')).sort('count', descending=True).head(10)
    print(top_1mile)
    
    # Analyze what changes between 1km and 1 mile
    print("\n" + "="*70)
    print("CHANGES BETWEEN 1KM → 1 MILE:")
    print("="*70)
    
    counts_1km = df_1km.group_by('street_name').agg(pl.len().alias('count_1km'))
    counts_1mile = df_1mile.group_by('street_name').agg(pl.len().alias('count_1mile'))
    
    merged_1km_1mile = counts_1km.join(counts_1mile, on='street_name', how='inner')
    changed_1km_1mile = merged_1km_1mile.filter(pl.col('count_1km') != pl.col('count_1mile'))
    
    print(f"\nStreets that change between 1km → 1 mile: {changed_1km_1mile.height}")
    if changed_1km_1mile.height > 0:
        print("(showing first 15)")
        print(changed_1km_1mile.sort('count_1km', descending=True).head(15))
    
    # Cumulative analysis
    print("\n" + "="*70)
    print("CUMULATIVE MERGING ANALYSIS:")
    print("="*70)
    
    total_merges_200_500 = df_200m.height - df_500m.height
    total_merges_500_1km = df_500m.height - df_1km.height
    total_merges_1km_1mile = df_1km.height - df_1mile.height
    total_merges_all = df_200m.height - df_1mile.height
    
    print(f"\nMerges by threshold range:")
    print(f"  200m → 500m:  {total_merges_200_500:,} merges ({total_merges_200_500/total_merges_all*100:.1f}% of total)")
    print(f"  500m → 1km:    {total_merges_500_1km:,} merges ({total_merges_500_1km/total_merges_all*100:.1f}% of total)")
    print(f"  1km → 1mi:     {total_merges_1km_1mile:,} merges ({total_merges_1km_1mile/total_merges_all*100:.1f}% of total)")
    print(f"  Total:         {total_merges_all:,} merges")
    
    print()

if __name__ == '__main__':
    state = sys.argv[1] if len(sys.argv) > 1 else 'delaware'
    compare_all_thresholds(state)
