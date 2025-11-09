#!/usr/bin/env python3
"""Generate state colors using distinctipy with regional constraints."""

from distinctipy import distinctipy
import colorsys

# Define regions and their states
regions = {
    'west_coast': ['california', 'oregon', 'washington'],
    'southwest': ['texas', 'arizona', 'new mexico', 'oklahoma'],
    'southeast': ['florida', 'georgia', 'alabama', 'mississippi', 'louisiana', 
                  'south carolina', 'north carolina', 'tennessee', 'kentucky', 
                  'virginia', 'west virginia'],
    'midwest': ['illinois', 'indiana', 'ohio', 'michigan', 'wisconsin', 
                'minnesota', 'iowa', 'missouri'],
    'northeast': ['new york', 'pennsylvania', 'new jersey', 'massachusetts', 
                  'connecticut', 'rhode island', 'vermont', 'new hampshire', 
                  'maine', 'delaware', 'maryland'],
    'mountain': ['colorado', 'wyoming', 'montana', 'idaho', 'utah', 'nevada'],
    'plains': ['kansas', 'nebraska', 'south dakota', 'north dakota'],
    'other': ['alaska', 'hawaii', 'arkansas']
}

# Define hue ranges for each region (in 0-1 scale)
hue_ranges = {
    'west_coast': (0.5, 0.67),      # Blues/cyans (180-240°)
    'southwest': (0.0, 0.1),         # Reds/oranges (0-36°)
    'southeast': (0.25, 0.45),       # Greens (90-162°)
    'midwest': (0.1, 0.2),           # Yellows/oranges (36-72°)
    'northeast': (0.75, 0.92),       # Purples/magentas (270-330°)
    'mountain': (0.05, 0.15),        # Browns/earth (18-54°, lower saturation)
    'plains': (0.0, 0.12),           # Warm oranges (0-43°)
    'other': (0.5, 0.6),             # Cyans (180-216°)
}

def generate_state_colors():
    """Generate distinct colors for each state within regional constraints."""
    state_colors = {}
    
    for region, states in regions.items():
        n_colors = len(states)
        hue_min, hue_max = hue_ranges[region]
        
        # Generate distinct colors
        colors = distinctipy.get_colors(n_colors, n_attempts=1000)
        
        # Adjust to be within hue range
        adjusted_colors = []
        for color in colors:
            r, g, b = color
            h, s, v = colorsys.rgb_to_hsv(r, g, b)
            
            # Adjust hue to be in range
            h_adjusted = hue_min + (h % 1.0) * (hue_max - hue_min)
            
            # Adjust saturation and value
            if region == 'mountain':
                s = min(s, 0.5)  # Lower saturation for earth tones
            else:
                s = max(s, 0.5)  # Ensure decent saturation
            v = max(v, 0.5)  # Ensure decent brightness
            
            r_new, g_new, b_new = colorsys.hsv_to_rgb(h_adjusted, s, v)
            adjusted_colors.append((r_new, g_new, b_new))
        
        # Assign to states
        for state, color in zip(states, adjusted_colors):
            hex_color = '#{:02x}{:02x}{:02x}'.format(
                int(color[0] * 255),
                int(color[1] * 255),
                int(color[2] * 255)
            )
            state_colors[state] = hex_color
    
    return state_colors


if __name__ == '__main__':
    colors = generate_state_colors()
    
    # Print Python dict format
    print('"""Color scheme for US states using distinctipy with regional constraints."""')
    print()
    print('STATE_COLORS = {')
    for state in sorted(colors.keys()):
        print(f"    '{state}': '{colors[state]}',")
    print('}')
    print()
    print()
    print('def get_state_color(state_name: str) -> str:')
    print('    """Get color for a state (case insensitive)."""')
    print("    return STATE_COLORS.get(state_name.lower(), '#7f7f7f')  # Default gray")

