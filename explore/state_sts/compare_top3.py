#!/usr/bin/env python3
"""Compare the top 3 most common state names: Washington, Virginia, and Ohio."""

import sys
from pathlib import Path
import polars as pl
from collections import Counter

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from workspace.load_street_df import load_state_streets_df
from workspace.explore.state_sts.most_common_state_st import extract_state_names_from_street_name

def analyze_state(state_name):
    """Analyze streets for a specific state name."""
    lf = load_state_streets_df()
    df = lf.collect()
    
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
    
    return pl.DataFrame(state_streets)

def main():
    print("="*80)
    print("COMPARISON: TOP 3 STATE NAMES IN STREETS")
    print("="*80)
    
    states = ['Washington', 'Virginia', 'Ohio']
    results = {}
    
    for state in states:
        print(f"\nAnalyzing {state}...")
        df = analyze_state(state)
        results[state] = df
    
    # Summary comparison
    print("\n" + "="*80)
    print("OVERALL COMPARISON")
    print("="*80)
    
    print("\n{:<15} {:>10} {:>10} {:>10} {:>10}".format(
        "State", "Total", "In-State", "Out-State", "In-State %"
    ))
    print("-" * 60)
    
    for state in states:
        df = results[state]
        total = len(df)
        in_state = len(df.filter(pl.col("is_in_state") == True))
        out_state = len(df.filter(pl.col("is_in_state") == False))
        pct = 100 * in_state / total if total > 0 else 0
        
        print("{:<15} {:>10,} {:>10,} {:>10,} {:>9.1f}%".format(
            state, total, in_state, out_state, pct
        ))
    
    # Geographic distribution comparison
    print("\n" + "="*80)
    print("GEOGRAPHIC SPREAD")
    print("="*80)
    
    for state in states:
        df = results[state]
        out_of_state = df.filter(pl.col("is_in_state") == False)
        state_counts = Counter(out_of_state['physical_state'].to_list())
        
        print(f"\n{state} streets appear in {len(state_counts)} different states")
        print(f"Top 5 states (excluding {state}):")
        for phys_state, count in state_counts.most_common(5):
            print(f"  {phys_state}: {count}")
    
    # Name pattern comparison
    print("\n" + "="*80)
    print("NAME PATTERNS: Simple vs Complex")
    print("="*80)
    
    import re
    
    for state in states:
        df = results[state]
        all_names = df['street_name'].to_list()
        
        # Count simple names (just "[Direction] StateName [Type]")
        simple_pattern = re.compile(
            rf'^(north|south|east|west\s+)?{state}\s+(street|avenue|road|drive|boulevard|lane|way|court|circle|place|pike|turnpike)$',
            re.IGNORECASE
        )
        
        simple = sum(1 for name in all_names if simple_pattern.match(name))
        complex_names = len(all_names) - simple
        
        print(f"\n{state}:")
        print(f"  Simple names: {simple:,} ({100*simple/len(all_names):.1f}%)")
        print(f"  Complex names: {complex_names:,} ({100*complex_names/len(all_names):.1f}%)")
    
    # Historical context
    print("\n" + "="*80)
    print("HISTORICAL CONTEXT")
    print("="*80)
    
    print("""
Washington:
  - Named after George Washington (1732-1799), first US President
  - Washington state admitted 1889 (42nd state)
  - Most streets likely honor the person, not the state
  - Washington D.C. established 1790 as capital
  - Extremely common street name across all of America

Virginia:
  - Named after Queen Elizabeth I (the "Virgin Queen")
  - Original 13 colonies, admitted 1788 (10th state to ratify)
  - Rich colonial and Revolutionary War history
  - Many historic sites and associations
  - West Virginia split off in 1863

Ohio:
  - Name from Iroquois word meaning "great river" (Ohio River)
  - Admitted 1803 (17th state)
  - First state created from Northwest Territory
  - Key role in westward expansion
  - Major transportation hub (Ohio River, canals, railroads)
  - Chesapeake & Ohio Railway and Baltimore & Ohio Railroad connections
    """)
    
    # Key insights
    print("\n" + "="*80)
    print("WHY IS OHIO #3?")
    print("="*80)
    
    print("""
1. HISTORICAL IMPORTANCE
   - Ohio was a gateway to westward expansion in the early 1800s
   - As one of the first states beyond the original colonies, it represented
     the frontier and American growth
   - Many cities established in the 1800s named streets after existing states

2. GEOGRAPHIC CENTRALITY
   - Ohio is centrally located in the eastern/midwestern US
   - The Ohio River was a major transportation route
   - Ohio bordered many other states, creating cultural connections

3. RAILROAD INFLUENCE
   - Major railroads like Chesapeake & Ohio and Baltimore & Ohio
   - These railroads operated across many states, leading to streets named
     after them that contain "Ohio"

4. SIMPLE, MEMORABLE NAME
   - "Ohio" is short, easy to pronounce, and distinctive
   - Unlike compound names (New York, New Jersey, etc.), it's a single word
   - Fits well in street naming conventions

5. WIDESPREAD DISTRIBUTION
   - Ohio-named streets appear in 47 different states
   - Particularly common in neighboring states (Indiana, Pennsylvania, 
     West Virginia) but also widespread in distant states
   - Shows Ohio's broad cultural influence across America

6. STANDARDIZATION OF STREET NAMING
   - Many cities adopted systematic street naming in the 1800s-1900s
   - Common pattern: name streets after US states
   - Ohio, as an important early state, was frequently included in these
     naming schemes
    """)

if __name__ == "__main__":
    main()

