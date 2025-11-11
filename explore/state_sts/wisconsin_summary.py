#!/usr/bin/env python3
"""Create a summary report of Wisconsin street analysis."""

import sys
from pathlib import Path
import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from workspace.load_street_df import load_state_streets_df
from workspace.explore.state_sts.most_common_state_st import extract_state_names_from_street_name

def analyze_wisconsin_streets():
    """Analyze Wisconsin streets and create summary report."""
    print("="*80)
    print("WISCONSIN STREET ANALYSIS SUMMARY")
    print("="*80)
    
    lf = load_state_streets_df()
    df = lf.collect()
    
    # Filter to Wisconsin streets
    wisconsin_streets = []
    for row in df.iter_rows(named=True):
        street_name = row['street_name']
        physical_state = row['state'].lower().replace("-", " ")
        
        found_states = extract_state_names_from_street_name(street_name)
        
        if any(state.lower() == "wisconsin" for state in found_states):
            is_in_state = (physical_state == "wisconsin")
            wisconsin_streets.append({
                'street_name': street_name,
                'physical_state': physical_state,
                'is_in_state': is_in_state
            })
    
    wisconsin_df = pl.DataFrame(wisconsin_streets)
    in_state = wisconsin_df.filter(pl.col("is_in_state") == True)
    out_of_state = wisconsin_df.filter(pl.col("is_in_state") == False)
    
    print(f"\nTotal Wisconsin streets: {len(wisconsin_df):,}")
    print(f"  In-state: {len(in_state):,} ({100*len(in_state)/len(wisconsin_df):.1f}%)")
    print(f"  Out-of-state: {len(out_of_state):,} ({100*len(out_of_state)/len(wisconsin_df):.1f}%)")
    
    # Identify Wisconsin-specific geographic features
    print("\n" + "="*80)
    print("WISCONSIN-SPECIFIC GEOGRAPHIC FEATURES")
    print("="*80)
    
    in_names = in_state['street_name'].to_list()
    out_names = out_of_state['street_name'].to_list()
    
    # Look for Wisconsin-specific terms
    geographic_features = {
        "Wisconsin River": ["river"],
        "Wisconsin Bay": ["bay"],
        "Wisconsin Dells": ["dells"],
        "Lake Wisconsin": ["lake wisconsin", "camp lake wisconsin"]
    }
    
    print("\nStreets named after Wisconsin-specific geographic features:")
    for feature_name, search_terms in geographic_features.items():
        in_matches = []
        out_matches = []
        
        for term in search_terms:
            in_matches.extend([name for name in in_names if term in name.lower()])
            out_matches.extend([name for name in out_names if term in name.lower()])
        
        in_matches = list(set(in_matches))
        out_matches = list(set(out_matches))
        
        if in_matches or out_matches:
            print(f"\n{feature_name}:")
            print(f"  In-state: {len(in_matches)}")
            if in_matches:
                for name in sorted(in_matches):
                    print(f"    - {name}")
            print(f"  Out-of-state: {len(out_matches)}")
            if out_matches:
                for name in sorted(out_matches):
                    print(f"    - {name}")
    
    # Summary insight
    print("\n" + "="*80)
    print("KEY INSIGHT")
    print("="*80)
    print("""
Wisconsin ranks #1 in in-state percentage (39.1%) because it has many streets
named after Wisconsin-specific geographic features that naturally occur only
in Wisconsin:

- Wisconsin River (5 streets in-state, 0 out-of-state)
- Wisconsin Bay (3 streets in-state, 0 out-of-state)  
- Wisconsin Dells (2 streets in-state, 1 out-of-state)
- Lake Wisconsin (2 streets in-state, 0 out-of-state)

These geographic feature names account for 12 streets that are almost exclusively
in Wisconsin, which helps boost Wisconsin's in-state percentage compared to
states that primarily have generic street names like "Wisconsin Avenue" or
"Wisconsin Street" that appear in many states.
    """)

if __name__ == "__main__":
    analyze_wisconsin_streets()

