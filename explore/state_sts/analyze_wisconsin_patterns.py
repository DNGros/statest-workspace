#!/usr/bin/env python3
"""Deep analysis of Wisconsin street name patterns to understand high in-state percentage."""

import sys
from pathlib import Path
import polars as pl
from collections import Counter

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from workspace.load_street_df import load_state_streets_df
from workspace.explore.state_sts.most_common_state_st import extract_state_names_from_street_name

def analyze_wisconsin_patterns():
    """Analyze patterns in Wisconsin street names."""
    print("="*80)
    print("WISCONSIN STREET PATTERN ANALYSIS")
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
    
    # Analyze common patterns
    print("\n" + "="*80)
    print("COMMON STREET NAME PATTERNS")
    print("="*80)
    
    in_names = in_state['street_name'].to_list()
    out_names = out_of_state['street_name'].to_list()
    
    # Look for common generic patterns
    generic_patterns = {
        "Wisconsin Avenue": ["wisconsin avenue", "wisconsin ave"],
        "Wisconsin Street": ["wisconsin street", "wisconsin st"],
        "Wisconsin Road": ["wisconsin road", "wisconsin rd"],
        "Wisconsin Drive": ["wisconsin drive", "wisconsin dr"],
        "Wisconsin Lane": ["wisconsin lane", "wisconsin ln"],
        "Wisconsin Way": ["wisconsin way"],
        "Wisconsin Boulevard": ["wisconsin boulevard", "wisconsin blvd"],
        "Wisconsin Court": ["wisconsin court", "wisconsin ct"],
        "Wisconsin Circle": ["wisconsin circle", "wisconsin cir"],
    }
    
    print("\nGeneric street type patterns:")
    for pattern_name, search_terms in generic_patterns.items():
        in_matches = []
        out_matches = []
        
        for term in search_terms:
            in_matches.extend([name for name in in_names if name.lower() == term])
            out_matches.extend([name for name in out_names if name.lower() == term])
        
        in_matches = list(set(in_matches))
        out_matches = list(set(out_matches))
        
        if in_matches or out_matches:
            in_pct = 100 * len(in_matches) / (len(in_matches) + len(out_matches)) if (len(in_matches) + len(out_matches)) > 0 else 0
            print(f"\n{pattern_name}:")
            print(f"  In-state: {len(in_matches)} ({in_pct:.1f}%)")
            print(f"  Out-of-state: {len(out_matches)} ({100-in_pct:.1f}%)")
    
    # Look for more specific patterns (not just generic street types)
    print("\n" + "="*80)
    print("NON-GENERIC PATTERNS (more specific names)")
    print("="*80)
    
    # Find streets that are NOT just "Wisconsin [Street Type]"
    def is_generic_pattern(name):
        name_lower = name.lower()
        generic_endings = [
            " avenue", " ave", " street", " st", " road", " rd",
            " drive", " dr", " lane", " ln", " way", " boulevard", " blvd",
            " court", " ct", " circle", " cir", " parkway", " pkwy",
            " place", " pl", " trail", " trl"
        ]
        # Check if it's exactly "wisconsin" + one of these endings
        for ending in generic_endings:
            if name_lower == f"wisconsin{ending}" or name_lower == f"wisconsin {ending.lstrip()}":
                return True
        return False
    
    in_non_generic = [name for name in in_names if not is_generic_pattern(name)]
    out_non_generic = [name for name in out_names if not is_generic_pattern(name)]
    
    print(f"\nNon-generic patterns:")
    print(f"  In-state: {len(in_non_generic)} ({100*len(in_non_generic)/len(in_names):.1f}% of in-state)")
    print(f"  Out-of-state: {len(out_non_generic)} ({100*len(out_non_generic)/len(out_names):.1f}% of out-of-state)")
    
    # Show most common non-generic patterns
    print("\nMost common non-generic in-state patterns:")
    in_non_generic_counter = Counter(in_non_generic)
    for name, count in in_non_generic_counter.most_common(20):
        print(f"  {name}: {count}")
    
    print("\nMost common non-generic out-of-state patterns:")
    out_non_generic_counter = Counter(out_non_generic)
    for name, count in out_non_generic_counter.most_common(20):
        print(f"  {name}: {count}")
    
    # Calculate what percentage of generic vs non-generic
    in_generic_count = len(in_names) - len(in_non_generic)
    out_generic_count = len(out_names) - len(out_non_generic)
    
    print("\n" + "="*80)
    print("KEY INSIGHT")
    print("="*80)
    print(f"""
Generic patterns (like "Wisconsin Avenue", "Wisconsin Street"):
  In-state: {in_generic_count} ({100*in_generic_count/len(in_names):.1f}%)
  Out-of-state: {out_generic_count} ({100*out_generic_count/len(out_names):.1f}%)

Non-generic patterns (more specific names):
  In-state: {len(in_non_generic)} ({100*len(in_non_generic)/len(in_names):.1f}%)
  Out-of-state: {len(out_non_generic)} ({100*len(out_non_generic)/len(out_names):.1f}%)

The in-state percentage for generic patterns: {100*in_generic_count/(in_generic_count+out_generic_count):.1f}%
The in-state percentage for non-generic patterns: {100*len(in_non_generic)/(len(in_non_generic)+len(out_non_generic)):.1f}%
    """)

if __name__ == "__main__":
    analyze_wisconsin_patterns()

