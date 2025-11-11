#!/usr/bin/env python3
"""Compare Wisconsin to other states to understand what makes it different."""

import sys
from pathlib import Path
import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from workspace.load_street_df import load_state_streets_df
from workspace.explore.state_sts.most_common_state_st import extract_state_names_from_street_name, calculate_in_state_percentage

def compare_states():
    """Compare Wisconsin to other top states."""
    print("="*80)
    print("COMPARING WISCONSIN TO OTHER STATES")
    print("="*80)
    
    # Get in-state percentages for all states
    lf = load_state_streets_df()
    percentages = calculate_in_state_percentage(lf)
    
    # Get top states
    top_states = percentages.head(10)
    print("\nTop 10 states by in-state percentage:")
    for row in top_states.iter_rows(named=True):
        print(f"  {row['state_name']}: {row['percentage']:.1f}% "
              f"({row['in_state']} in-state, {row['out_of_state']} out-of-state)")
    
    # Now analyze Wisconsin vs Texas (both in top 3)
    print("\n" + "="*80)
    print("DETAILED COMPARISON: WISCONSIN vs TEXAS")
    print("="*80)
    
    df = lf.collect()
    
    for state_name in ["Wisconsin", "Texas"]:
        print(f"\n{state_name.upper()}:")
        state_streets = []
        for row in df.iter_rows(named=True):
            street_name = row['street_name']
            physical_state = row['state'].lower().replace("-", " ")
            
            found_states = extract_state_names_from_street_name(street_name)
            
            if any(state.lower() == state_name.lower() for state in found_states):
                is_in_state = (physical_state == state_name.lower())
                state_streets.append({
                    'street_name': street_name,
                    'physical_state': physical_state,
                    'is_in_state': is_in_state
                })
        
        state_df = pl.DataFrame(state_streets)
        in_state_df = state_df.filter(pl.col("is_in_state") == True)
        out_of_state_df = state_df.filter(pl.col("is_in_state") == False)
        
        total = len(state_df)
        in_count = len(in_state_df)
        out_count = len(out_of_state_df)
        pct = 100 * in_count / total if total > 0 else 0
        
        print(f"  Total: {total}")
        print(f"  In-state: {in_count} ({pct:.1f}%)")
        print(f"  Out-of-state: {out_count} ({100-pct:.1f}%)")
        
        # Count generic vs non-generic
        def is_generic_pattern(name, state):
            name_lower = name.lower()
            state_lower = state.lower()
            generic_endings = [
                " avenue", " ave", " street", " st", " road", " rd",
                " drive", " dr", " lane", " ln", " way", " boulevard", " blvd",
                " court", " ct", " circle", " cir", " parkway", " pkwy",
                " place", " pl", " trail", " trl"
            ]
            for ending in generic_endings:
                if name_lower == f"{state_lower}{ending}" or name_lower == f"{state_lower} {ending.lstrip()}":
                    return True
            return False
        
        in_names = in_state_df['street_name'].to_list()
        out_names = out_of_state_df['street_name'].to_list()
        
        in_generic = sum(1 for name in in_names if is_generic_pattern(name, state_name))
        out_generic = sum(1 for name in out_names if is_generic_pattern(name, state_name))
        
        in_non_generic = in_count - in_generic
        out_non_generic = out_count - out_generic
        
        print(f"\n  Generic patterns:")
        print(f"    In-state: {in_generic} ({100*in_generic/in_count:.1f}% of in-state)")
        print(f"    Out-of-state: {out_generic} ({100*out_generic/out_count:.1f}% of out-of-state)")
        generic_pct = 100 * in_generic / (in_generic + out_generic) if (in_generic + out_generic) > 0 else 0
        print(f"    In-state % for generic: {generic_pct:.1f}%")
        
        print(f"\n  Non-generic patterns:")
        print(f"    In-state: {in_non_generic} ({100*in_non_generic/in_count:.1f}% of in-state)")
        print(f"    Out-of-state: {out_non_generic} ({100*out_non_generic/out_count:.1f}% of out-of-state)")
        non_generic_pct = 100 * in_non_generic / (in_non_generic + out_non_generic) if (in_non_generic + out_non_generic) > 0 else 0
        print(f"    In-state % for non-generic: {non_generic_pct:.1f}%")
        
        # Show some examples of non-generic patterns
        print(f"\n  Sample non-generic in-state patterns:")
        in_non_generic_list = [name for name in in_names if not is_generic_pattern(name, state_name)]
        from collections import Counter
        counter = Counter(in_non_generic_list)
        for name, count in counter.most_common(5):
            print(f"    {name}: {count}")

if __name__ == "__main__":
    compare_states()

