#!/usr/bin/env python3
"""Detailed analysis of Ohio street naming patterns."""

import sys
from pathlib import Path
import polars as pl
from collections import Counter
import re

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from workspace.load_street_df import load_state_streets_df
from workspace.explore.state_sts.most_common_state_st import extract_state_names_from_street_name

def analyze_ohio_patterns():
    """Detailed pattern analysis of Ohio streets."""
    print("="*80)
    print("DETAILED OHIO STREET PATTERN ANALYSIS")
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
            ohio_streets.append({
                'street_name': street_name,
                'physical_state': physical_state,
            })
    
    ohio_df = pl.DataFrame(ohio_streets)
    all_names = ohio_df['street_name'].to_list()
    
    # Analyze specific patterns
    print("\n" + "="*80)
    print("RAILROAD-RELATED STREETS")
    print("="*80)
    
    railroad_patterns = [
        "chesapeake and ohio",
        "baltimore and ohio",
        "baltimore ohio",
        "c&o",
        "b&o",
    ]
    
    railroad_streets = []
    for name in all_names:
        name_lower = name.lower()
        for pattern in railroad_patterns:
            if pattern in name_lower:
                railroad_streets.append(name)
                break
    
    print(f"\nFound {len(railroad_streets)} railroad-related streets:")
    for name in sorted(set(railroad_streets)):
        print(f"  - {name}")
    
    # Analyze compound names (Ohio + another location)
    print("\n" + "="*80)
    print("COMPOUND LOCATION NAMES")
    print("="*80)
    
    compound_streets = []
    for name in all_names:
        # Look for patterns like "City Ohio Street" or "Ohio-City"
        if re.search(r'\b\w+\s+ohio\b', name.lower()) or re.search(r'ohio[-\s]\w+', name.lower()):
            compound_streets.append(name)
    
    # Group by pattern type
    city_ohio = [n for n in compound_streets if re.search(r'\b\w+\s+ohio\s+(street|avenue|road|drive)', n.lower())]
    ohio_city = [n for n in compound_streets if re.search(r'ohio\s+\w+\s+(street|avenue|road|drive)', n.lower())]
    
    print(f"\nStreets with '[City] Ohio [Type]' pattern ({len(city_ohio)}):")
    for name in sorted(set(city_ohio))[:20]:
        print(f"  - {name}")
    
    print(f"\nStreets with 'Ohio [City] [Type]' pattern ({len(ohio_city)}):")
    for name in sorted(set(ohio_city))[:20]:
        print(f"  - {name}")
    
    # Analyze directional patterns
    print("\n" + "="*80)
    print("DIRECTIONAL PATTERNS")
    print("="*80)
    
    directions = ['north', 'south', 'east', 'west']
    directional_counts = Counter()
    
    for name in all_names:
        name_lower = name.lower()
        for direction in directions:
            if direction in name_lower and 'ohio' in name_lower:
                directional_counts[direction] += 1
    
    print("\nDirectional prefixes/suffixes:")
    for direction, count in directional_counts.most_common():
        print(f"  {direction.capitalize()}: {count}")
    
    # Analyze simple vs complex names
    print("\n" + "="*80)
    print("NAME COMPLEXITY")
    print("="*80)
    
    simple_names = []  # Just "Ohio [Type]"
    complex_names = []  # Everything else
    
    simple_pattern = re.compile(r'^(north|south|east|west\s+)?ohio\s+(street|avenue|road|drive|boulevard|lane|way|court|circle|place|pike|turnpike)$', re.IGNORECASE)
    
    for name in all_names:
        if simple_pattern.match(name):
            simple_names.append(name)
        else:
            complex_names.append(name)
    
    print(f"\nSimple names (just 'Ohio [Type]'): {len(simple_names)} ({100*len(simple_names)/len(all_names):.1f}%)")
    print(f"Complex names (with additional context): {len(complex_names)} ({100*len(complex_names)/len(all_names):.1f}%)")
    
    print("\nSample simple names:")
    for name in sorted(set(simple_names))[:15]:
        print(f"  - {name}")
    
    print("\nSample complex names:")
    for name in sorted(set(complex_names))[:20]:
        print(f"  - {name}")
    
    # Historical/institutional references
    print("\n" + "="*80)
    print("HISTORICAL & INSTITUTIONAL REFERENCES")
    print("="*80)
    
    institutional_keywords = {
        'University': ['university', 'college'],
        'State/Government': ['state', 'capitol'],
        'River/Geographic': ['river', 'creek', 'valley', 'hill'],
        'Transportation': ['turnpike', 'pike', 'highway', 'railway', 'railroad'],
        'Historic Places': ['furnace', 'mill', 'station', 'depot'],
    }
    
    for category, keywords in institutional_keywords.items():
        matches = []
        for name in all_names:
            name_lower = name.lower()
            if any(kw in name_lower for kw in keywords):
                matches.append(name)
        
        print(f"\n{category}: {len(matches)} streets")
        if len(matches) <= 15:
            for name in sorted(set(matches)):
                print(f"  - {name}")

if __name__ == "__main__":
    analyze_ohio_patterns()

