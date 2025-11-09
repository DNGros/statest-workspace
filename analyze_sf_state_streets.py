"""Analyze SF streets to find how many contain state names."""

import json
from workspace.states import USState


def load_sf_streets(filepath):
    """Load SF streets data from JSON file."""
    with open(filepath, 'r') as f:
        data = json.load(f)
    return data['streets']


def street_contains_state(street_name, state_names):
    """Check if a street name contains any state name as a complete word (case insensitive)."""
    import re
    
    street_lower = street_name.lower()
    matching_states = []
    
    for state in state_names:
        # Use word boundary matching to ensure complete word match
        # This handles multi-word states like "new york" correctly
        pattern = r'\b' + re.escape(state.lower()) + r'\b'
        if re.search(pattern, street_lower):
            matching_states.append(state)
    
    return matching_states


def analyze_state_streets(streets_data):
    """Analyze streets to find those containing state names."""
    state_names = USState.all_names()
    
    streets_with_states = []
    state_counts = {state: 0 for state in state_names}
    
    for street in streets_data:
        properties = street.get('properties', {})
        
        # Check multiple name fields
        names_to_check = set()
        
        # Add the clean name
        if 'clean' in properties:
            names_to_check.add(properties['clean'])
        
        # Add cleanWithSpace
        if 'cleanWithSpace' in properties:
            names_to_check.add(properties['cleanWithSpace'])
        
        # Add display name
        if 'display' in properties:
            names_to_check.add(properties['display'])
        
        # Add all fullNames
        if 'fullNames' in properties:
            for name in properties['fullNames']:
                names_to_check.add(name)
        
        # Check each unique name for state matches
        all_matching_states = set()
        for name in names_to_check:
            matching_states = street_contains_state(name, state_names)
            all_matching_states.update(matching_states)
        
        if all_matching_states:
            streets_with_states.append({
                'names': list(names_to_check),
                'matching_states': sorted(list(all_matching_states)),
                'miles': properties.get('miles', 0)
            })
            
            # Count each state
            for state in all_matching_states:
                state_counts[state] += 1
    
    return streets_with_states, state_counts


def main():
    """Main analysis function."""
    print("Loading SF streets data...")
    streets = load_sf_streets('workspace/data/data-sf-all.json')
    print(f"Total streets loaded: {len(streets)}")
    
    print("\nAnalyzing streets for state names...")
    streets_with_states, state_counts = analyze_state_streets(streets)
    
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"Total streets: {len(streets)}")
    print(f"Streets containing state names: {len(streets_with_states)}")
    print(f"Percentage: {len(streets_with_states) / len(streets) * 100:.2f}%")
    
    # Show state counts (only non-zero)
    print(f"\n{'='*60}")
    print(f"STATE NAME OCCURRENCES")
    print(f"{'='*60}")
    
    sorted_states = sorted(state_counts.items(), key=lambda x: x[1], reverse=True)
    for state, count in sorted_states:
        if count > 0:
            print(f"{state.title():20s}: {count:3d}")
    
    # Show some examples
    print(f"\n{'='*60}")
    print(f"EXAMPLES (first 20)")
    print(f"{'='*60}")
    
    for i, street in enumerate(streets_with_states[:20]):
        print(f"\n{i+1}. States found: {', '.join([s.title() for s in street['matching_states']])}")
        print(f"   Street names: {', '.join(street['names'][:3])}")  # Show first 3 names
        if len(street['names']) > 3:
            print(f"   ... and {len(street['names']) - 3} more")
    
    # Save detailed results to file
    output_file = 'workspace/output/sf_streets_with_states.json'
    with open(output_file, 'w') as f:
        json.dump({
            'summary': {
                'total_streets': len(streets),
                'streets_with_states': len(streets_with_states),
                'percentage': len(streets_with_states) / len(streets) * 100
            },
            'state_counts': {k: v for k, v in sorted_states if v > 0},
            'streets': streets_with_states
        }, f, indent=2)
    
    print(f"\n{'='*60}")
    print(f"Detailed results saved to: {output_file}")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()

