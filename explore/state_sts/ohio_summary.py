#!/usr/bin/env python3
"""Create a summary report of Ohio street analysis."""

import sys
from pathlib import Path
import polars as pl
from collections import Counter

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from workspace.load_street_df import load_state_streets_df
from workspace.explore.state_sts.most_common_state_st import extract_state_names_from_street_name

def analyze_ohio_streets():
    """Analyze Ohio streets and create summary report."""
    print("="*80)
    print("OHIO STREET ANALYSIS SUMMARY")
    print("="*80)
    
    lf = load_state_streets_df()
    df = lf.collect()
    
    # Filter to Ohio streets
    ohio_streets = []
    for row in df.iter_rows(named=True):
        street_name = row['street_name']
        physical_state = row['state'].lower().replace("-", " ")
        
        found_states = extract_state_names_from_street_name(street_name)
        
        if any(state.lower() == "ohio" for state in found_states):
            is_in_state = (physical_state == "ohio")
            ohio_streets.append({
                'street_name': street_name,
                'physical_state': physical_state,
                'is_in_state': is_in_state
            })
    
    ohio_df = pl.DataFrame(ohio_streets)
    in_state = ohio_df.filter(pl.col("is_in_state") == True)
    out_of_state = ohio_df.filter(pl.col("is_in_state") == False)
    
    print(f"\nTotal Ohio streets: {len(ohio_df):,}")
    print(f"  In-state: {len(in_state):,} ({100*len(in_state)/len(ohio_df):.1f}%)")
    print(f"  Out-of-state: {len(out_of_state):,} ({100*len(out_of_state)/len(ohio_df):.1f}%)")
    
    # Analyze street name patterns
    print("\n" + "="*80)
    print("STREET NAME PATTERNS")
    print("="*80)
    
    in_names = in_state['street_name'].to_list()
    out_names = out_of_state['street_name'].to_list()
    
    # Extract street types (last word after "Ohio")
    def extract_street_type(name):
        """Extract the street type that comes after Ohio."""
        name_lower = name.lower()
        if 'ohio' not in name_lower:
            return None
        # Find position of 'ohio' and get next word
        parts = name.split()
        for i, part in enumerate(parts):
            if 'ohio' in part.lower():
                if i + 1 < len(parts):
                    return parts[i + 1]
        return None
    
    in_types = [extract_street_type(name) for name in in_names]
    out_types = [extract_street_type(name) for name in out_names]
    
    in_type_counts = Counter([t for t in in_types if t])
    out_type_counts = Counter([t for t in out_types if t])
    
    print("\nMost common street types in-state:")
    for street_type, count in in_type_counts.most_common(10):
        print(f"  {street_type}: {count}")
    
    print("\nMost common street types out-of-state:")
    for street_type, count in out_type_counts.most_common(10):
        print(f"  {street_type}: {count}")
    
    # Look for Ohio-specific geographic features
    print("\n" + "="*80)
    print("OHIO-SPECIFIC GEOGRAPHIC FEATURES")
    print("="*80)
    
    geographic_features = {
        "Ohio River": ["ohio river"],
        "Ohio State": ["ohio state"],
        "Ohio University": ["ohio university"],
        "Ohio Turnpike": ["ohio turnpike"],
        "Lake Erie": ["erie"],  # Major lake bordering Ohio
    }
    
    print("\nStreets named after Ohio-specific features:")
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
            if len(in_matches) <= 10:
                for name in sorted(in_matches):
                    print(f"    - {name}")
            print(f"  Out-of-state: {len(out_matches)}")
            if len(out_matches) <= 10:
                for name in sorted(out_matches):
                    print(f"    - {name}")
    
    # Analyze distribution across states
    print("\n" + "="*80)
    print("GEOGRAPHIC DISTRIBUTION")
    print("="*80)
    
    state_counts = Counter(out_of_state['physical_state'].to_list())
    print(f"\nOhio streets appear in {len(state_counts)} different states")
    print("\nTop 10 states with Ohio-named streets (excluding Ohio itself):")
    for state, count in state_counts.most_common(10):
        print(f"  {state}: {count}")
    
    # Sample street names
    print("\n" + "="*80)
    print("SAMPLE STREET NAMES")
    print("="*80)
    
    print("\nSample in-state Ohio streets:")
    for name in sorted(in_names)[:15]:
        print(f"  - {name}")
    
    print("\nSample out-of-state Ohio streets:")
    for name in sorted(out_names)[:15]:
        print(f"  - {name}")
    
    # Key insight
    print("\n" + "="*80)
    print("KEY INSIGHTS")
    print("="*80)
    print(f"""
Ohio ranks #3 in total number of streets ({len(ohio_df):,}), behind Washington and Virginia.

In-state percentage: {100*len(in_state)/len(ohio_df):.1f}%

Why is Ohio so popular?
- Ohio is a common street name pattern across many states
- The name "Ohio" itself comes from an Iroquois word meaning "great river"
- Ohio was an important early state (admitted 1803, 17th state)
- Major historical significance in westward expansion and settlement
- Many cities across the US named streets after states in alphabetical or geographic order
    """)

if __name__ == "__main__":
    analyze_ohio_streets()

