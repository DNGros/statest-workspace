#!/usr/bin/env python3
"""Analyze what fraction of streets in each state are named after that state."""

import sys
from pathlib import Path
import polars as pl

# Add parent directory to path to import workspace modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from workspace.load_street_df import load_street_df
from workspace.states import USState
from workspace.explore.state_sts.most_common_state_st import extract_state_names_from_street_name


def analyze_state_naming_fractions():
    """
    For each state, calculate:
    1. Total number of streets in that state
    2. Number of streets in that state named after that state
    3. Fraction of streets named after the state
    """
    print("Loading all streets from all states (excluding numbered streets)...")
    # Exclude numbered streets to match the stacked bar chart analysis
    all_streets_lf = load_street_df()
    all_streets_lf = all_streets_lf.filter(~pl.col("street_name").str.contains(r"\d", literal=False))
    
    # Collect to DataFrame for processing
    print("Collecting data...")
    all_streets_df = all_streets_lf.collect()
    
    print(f"Total streets loaded: {len(all_streets_df):,}")
    
    # Get all state names
    all_state_names = USState.all_names()
    
    results = []
    
    for state_name in all_state_names:
        state_lower = state_name.lower()
        # Convert state name to dash format (as stored in the data)
        state_dash = state_lower.replace(" ", "-")
        
        # Filter to streets in this state
        state_streets = all_streets_df.filter(pl.col("state") == state_dash)
        total_streets = len(state_streets)
        
        if total_streets == 0:
            print(f"Warning: No streets found for {state_name}")
            continue
        
        # Count streets in this state that are named after this state
        # We'll use a simpler approach: check if the state name appears in the street name
        # This matches the logic used in extract_state_names_from_street_name
        import re
        escaped_name = re.escape(state_name)
        pattern = r'\b' + escaped_name + r'\b'
        state_named_streets = state_streets.filter(
            pl.col("street_name").str.to_lowercase().str.contains(pattern, literal=False)
        )
        state_named_count = len(state_named_streets)
        
        fraction = (state_named_count / total_streets * 100) if total_streets > 0 else 0.0
        
        results.append({
            'state_name': state_name,
            'total_streets': total_streets,
            'state_named_streets': state_named_count,
            'fraction_pct': round(fraction, 3)
        })
        
        print(f"{state_name:20s} - {state_named_count:4d} / {total_streets:6,} = {fraction:5.3f}%")
    
    # Convert to DataFrame and sort
    results_df = pl.DataFrame(results)
    results_df = results_df.sort("fraction_pct", descending=True)
    
    print("\n" + "="*80)
    print("RESULTS SORTED BY FRACTION (highest to lowest):")
    print("="*80)
    print(results_df)
    
    print("\n" + "="*80)
    print("TOP 10 STATES BY FRACTION:")
    print("="*80)
    print(results_df.head(10))
    
    print("\n" + "="*80)
    print("BOTTOM 10 STATES BY FRACTION:")
    print("="*80)
    print(results_df.tail(10))
    
    # Special focus on New York
    ny_result = results_df.filter(pl.col("state_name").str.to_lowercase() == "new york")
    if len(ny_result) > 0:
        print("\n" + "="*80)
        print("NEW YORK SPECIFIC:")
        print("="*80)
        print(ny_result)
    
    return results_df


if __name__ == "__main__":
    analyze_state_naming_fractions()

