#!/usr/bin/env python3
"""Sample Wisconsin streets to explore why Wisconsin is #1 in the rankings."""

import sys
from pathlib import Path
import polars as pl
import random

# Add parent directory to path to import workspace modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from workspace.load_street_df import load_state_streets_df
from workspace.explore.state_sts.most_common_state_st import extract_state_names_from_street_name


def sample_wisconsin_streets(num_samples: int = 20):
    """
    Sample Wisconsin streets, both in-state and out-of-state.
    
    Args:
        num_samples: Number of samples to show for each category
    """
    print("Loading state-named streets...")
    lf = load_state_streets_df()
    df = lf.collect()
    
    print(f"Total streets loaded: {len(df):,}")
    
    # Filter to streets that contain "Wisconsin" in the name
    print("\nFiltering to streets with 'Wisconsin' in the name...")
    
    # Extract state names for each street
    wisconsin_streets = []
    for row in df.iter_rows(named=True):
        street_name = row['street_name']
        physical_state = row['state'].lower().replace("-", " ")
        
        found_states = extract_state_names_from_street_name(street_name)
        
        # Check if Wisconsin is in the found states
        if any(state.lower() == "wisconsin" for state in found_states):
            is_in_state = (physical_state == "wisconsin")
            wisconsin_streets.append({
                'street_name': street_name,
                'physical_state': physical_state,
                'is_in_state': is_in_state
            })
    
    wisconsin_df = pl.DataFrame(wisconsin_streets)
    
    print(f"Found {len(wisconsin_df):,} streets with 'Wisconsin' in the name")
    
    # Separate in-state and out-of-state
    in_state = wisconsin_df.filter(pl.col("is_in_state") == True)
    out_of_state = wisconsin_df.filter(pl.col("is_in_state") == False)
    
    print(f"\nIn-state (Wisconsin streets in Wisconsin): {len(in_state):,}")
    print(f"Out-of-state (Wisconsin streets in other states): {len(out_of_state):,}")
    
    # Count by physical state for out-of-state
    print("\n" + "="*80)
    print("OUT-OF-STATE WISCONSIN STREETS - Breakdown by physical state:")
    print("="*80)
    out_of_state_by_state = (
        out_of_state
        .group_by("physical_state")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
    )
    print(out_of_state_by_state)
    
    # Sample out-of-state streets
    print("\n" + "="*80)
    print(f"SAMPLE OF {num_samples} OUT-OF-STATE WISCONSIN STREETS:")
    print("="*80)
    if len(out_of_state) > 0:
        sampled_out = out_of_state.sample(min(num_samples, len(out_of_state)), seed=42)
        for row in sampled_out.iter_rows(named=True):
            print(f"  {row['street_name']:50s} (in {row['physical_state'].title()})")
    else:
        print("  No out-of-state Wisconsin streets found!")
    
    # Sample in-state streets
    print("\n" + "="*80)
    print(f"SAMPLE OF {num_samples} IN-STATE WISCONSIN STREETS:")
    print("="*80)
    if len(in_state) > 0:
        sampled_in = in_state.sample(min(num_samples, len(in_state)), seed=42)
        for row in sampled_in.iter_rows(named=True):
            print(f"  {row['street_name']:50s} (in Wisconsin)")
    else:
        print("  No in-state Wisconsin streets found!")
    
    # Look for patterns - check for common prefixes/suffixes
    print("\n" + "="*80)
    print("PATTERN ANALYSIS:")
    print("="*80)
    
    # Check for common patterns in out-of-state streets
    print("\nOut-of-state street name patterns:")
    out_names = out_of_state['street_name'].to_list()
    if out_names:
        # Count how many start with "Wisconsin"
        starts_with_wisconsin = sum(1 for name in out_names if name.lower().startswith("wisconsin"))
        print(f"  Streets starting with 'Wisconsin': {starts_with_wisconsin}/{len(out_names)} ({100*starts_with_wisconsin/len(out_names):.1f}%)")
        
        # Count how many contain "Wisconsin Ave" or similar
        contains_ave = sum(1 for name in out_names if "ave" in name.lower() or "avenue" in name.lower())
        print(f"  Streets containing 'Ave' or 'Avenue': {contains_ave}/{len(out_names)} ({100*contains_ave/len(out_names):.1f}%)")
        
        # Show unique street name patterns (first word after Wisconsin if it starts with Wisconsin)
        wisconsin_prefix_names = [name for name in out_names if name.lower().startswith("wisconsin")]
        if wisconsin_prefix_names:
            print(f"\n  Examples of streets starting with 'Wisconsin':")
            for name in sorted(set(wisconsin_prefix_names))[:10]:
                print(f"    - {name}")
    
    # Check for common patterns in in-state streets
    print("\nIn-state street name patterns:")
    in_names = in_state['street_name'].to_list()
    if in_names:
        starts_with_wisconsin = sum(1 for name in in_names if name.lower().startswith("wisconsin"))
        print(f"  Streets starting with 'Wisconsin': {starts_with_wisconsin}/{len(in_names)} ({100*starts_with_wisconsin/len(in_names):.1f}%)")
        
        contains_ave = sum(1 for name in in_names if "ave" in name.lower() or "avenue" in name.lower())
        print(f"  Streets containing 'Ave' or 'Avenue': {contains_ave}/{len(in_names)} ({100*contains_ave/len(in_names):.1f}%)")
        
        # Show unique street name patterns
        wisconsin_prefix_names = [name for name in in_names if name.lower().startswith("wisconsin")]
        if wisconsin_prefix_names:
            print(f"\n  Examples of streets starting with 'Wisconsin':")
            for name in sorted(set(wisconsin_prefix_names))[:10]:
                print(f"    - {name}")
    
    # Look for Wisconsin-specific place names that might explain high in-state percentage
    print("\n" + "="*80)
    print("WISCONSIN-SPECIFIC PLACE NAME ANALYSIS:")
    print("="*80)
    
    # Check for Wisconsin-specific terms
    wisconsin_terms = ["dells", "river", "bay", "creek", "lake"]
    
    print("\nChecking for Wisconsin-specific geographic terms:")
    for term in wisconsin_terms:
        in_with_term = sum(1 for name in in_names if term in name.lower())
        out_with_term = sum(1 for name in out_names if term in name.lower())
        print(f"  '{term}': {in_with_term} in-state, {out_with_term} out-of-state")
        
        if in_with_term > 0:
            print(f"    In-state examples:")
            examples = [name for name in in_names if term in name.lower()][:5]
            for ex in examples:
                print(f"      - {ex}")
        if out_with_term > 0:
            print(f"    Out-of-state examples:")
            examples = [name for name in out_names if term in name.lower()][:5]
            for ex in examples:
                print(f"      - {ex}")
    
    # Compare unique street names
    print("\n" + "="*80)
    print("UNIQUE STREET NAME ANALYSIS:")
    print("="*80)
    
    in_unique = set(in_names)
    out_unique = set(out_names)
    only_in_state = in_unique - out_unique
    only_out_state = out_unique - in_unique
    both = in_unique & out_unique
    
    print(f"\nStreets ONLY found in Wisconsin: {len(only_in_state)}")
    if only_in_state:
        print("  Examples:")
        for name in sorted(list(only_in_state))[:15]:
            print(f"    - {name}")
    
    print(f"\nStreets ONLY found out-of-state: {len(only_out_state)}")
    if only_out_state:
        print("  Examples:")
        for name in sorted(list(only_out_state))[:15]:
            print(f"    - {name}")
    
    print(f"\nStreets found in BOTH: {len(both)}")
    if both:
        print("  Examples:")
        for name in sorted(list(both))[:15]:
            print(f"    - {name}")
    
    return wisconsin_df, in_state, out_of_state


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Sample Wisconsin streets to explore ranking patterns'
    )
    parser.add_argument(
        '--num-samples',
        type=int,
        default=20,
        help='Number of samples to show for each category (default: 20)'
    )
    
    args = parser.parse_args()
    
    sample_wisconsin_streets(num_samples=args.num_samples)

