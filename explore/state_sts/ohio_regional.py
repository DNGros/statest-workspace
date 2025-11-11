#!/usr/bin/env python3
"""Analyze regional patterns of Ohio street names."""

import sys
from pathlib import Path
import polars as pl
from collections import Counter

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from workspace.load_street_df import load_state_streets_df
from workspace.explore.state_sts.most_common_state_st import extract_state_names_from_street_name

def main():
    print("="*80)
    print("REGIONAL ANALYSIS: OHIO STREET NAMES")
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
    
    # Define US regions
    regions = {
        'Northeast': ['maine', 'new hampshire', 'vermont', 'massachusetts', 'rhode island', 
                     'connecticut', 'new york', 'new jersey', 'pennsylvania'],
        'Southeast': ['delaware', 'maryland', 'virginia', 'west virginia', 'kentucky', 
                     'north carolina', 'south carolina', 'tennessee', 'georgia', 'florida',
                     'alabama', 'mississippi', 'arkansas', 'louisiana'],
        'Midwest': ['ohio', 'indiana', 'illinois', 'michigan', 'wisconsin', 'minnesota',
                   'iowa', 'missouri', 'north dakota', 'south dakota', 'nebraska', 'kansas'],
        'Southwest': ['oklahoma', 'texas', 'new mexico', 'arizona'],
        'West': ['montana', 'idaho', 'wyoming', 'colorado', 'utah', 'nevada', 'california',
                'oregon', 'washington', 'alaska', 'hawaii'],
    }
    
    # Count by region
    region_counts = Counter()
    state_to_region = {}
    for region, states in regions.items():
        for state in states:
            state_to_region[state] = region
    
    for state in ohio_df['physical_state'].to_list():
        region = state_to_region.get(state, 'Unknown')
        region_counts[region] += 1
    
    print("\n" + "="*80)
    print("OHIO STREETS BY REGION")
    print("="*80)
    
    total = sum(region_counts.values())
    print(f"\nTotal Ohio-named streets: {total:,}\n")
    
    for region in ['Midwest', 'Southeast', 'Northeast', 'Southwest', 'West']:
        count = region_counts[region]
        pct = 100 * count / total
        print(f"{region:15} {count:5,} ({pct:5.1f}%)")
    
    # Detailed state breakdown by region
    print("\n" + "="*80)
    print("DETAILED STATE BREAKDOWN BY REGION")
    print("="*80)
    
    state_counts = Counter(ohio_df['physical_state'].to_list())
    
    for region in ['Midwest', 'Southeast', 'Northeast', 'Southwest', 'West']:
        print(f"\n{region}:")
        region_states = regions[region]
        region_total = sum(state_counts[s] for s in region_states)
        
        # Sort states in this region by count
        region_state_counts = [(s, state_counts[s]) for s in region_states if state_counts[s] > 0]
        region_state_counts.sort(key=lambda x: x[1], reverse=True)
        
        for state, count in region_state_counts:
            pct = 100 * count / region_total if region_total > 0 else 0
            print(f"  {state:20} {count:4,} ({pct:5.1f}% of region)")
    
    # Ohio's neighbors analysis
    print("\n" + "="*80)
    print("OHIO'S NEIGHBORING STATES")
    print("="*80)
    
    neighbors = ['michigan', 'indiana', 'kentucky', 'west virginia', 'pennsylvania']
    
    print("\nOhio borders 5 states. How many Ohio-named streets do they have?\n")
    
    neighbor_total = sum(state_counts[s] for s in neighbors)
    print(f"Total in neighboring states: {neighbor_total:,} ({100*neighbor_total/total:.1f}% of all Ohio streets)\n")
    
    for state in neighbors:
        count = state_counts[state]
        pct = 100 * count / neighbor_total
        print(f"  {state.title():20} {count:4,} ({pct:5.1f}% of neighbors)")
    
    # Distance from Ohio analysis
    print("\n" + "="*80)
    print("KEY INSIGHTS")
    print("="*80)
    
    midwest_pct = 100 * region_counts['Midwest'] / total
    neighbor_pct = 100 * neighbor_total / total
    
    print(f"""
Regional Concentration:
- {midwest_pct:.1f}% of Ohio-named streets are in the Midwest region
- {neighbor_pct:.1f}% are in states that border Ohio
- Indiana alone has {state_counts['indiana']} Ohio streets ({100*state_counts['indiana']/total:.1f}%)

This shows strong regional clustering - Ohio streets are most common near Ohio itself,
reflecting:
1. Geographic proximity and cultural connections
2. Shared history of settlement and development
3. The Ohio River as a regional boundary and transportation route
4. Migration patterns from Ohio to neighboring states

However, Ohio streets still appear in all regions, showing its national significance
as a symbol of American expansion and development.
    """)

if __name__ == "__main__":
    main()

