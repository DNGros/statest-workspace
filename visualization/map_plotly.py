#!/usr/bin/env python3
"""Create an interactive Plotly map of state-named streets."""

import sys
from pathlib import Path
import polars as pl
import plotly.graph_objects as go
from typing import Optional
import json

# Add workspace to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from load_street_df import load_state_streets_df
from states import USState
from state_colors import get_state_color


def create_state_streets_map(
    output_path: Path,
    sample_size: Optional[int] = 100000,
):
    """
    Create an interactive map of state-named streets using Plotly.
    
    Args:
        output_path: Where to save the HTML file
        sample_size: Number of streets to sample (None for all)
    """
    print("Loading state-named streets...")
    lf = load_state_streets_df()
    df = lf.collect()
    
    print(f"Loaded {len(df):,} state-named streets")
    
    # Sample if needed
    if sample_size and len(df) > sample_size:
        df = df.sample(sample_size, seed=42)
        print(f"Sampled to {len(df):,} streets for visualization")
    
    # Add a column for which state name is in the street name
    print("Identifying state names in street names...")
    state_names = USState.all_names()
    
    def find_state_in_name(street_name: str) -> str:
        """Find which state name appears in the street name."""
        street_lower = street_name.lower()
        for state_name in state_names:
            if state_name in street_lower:
                return state_name.title()
        return "Unknown"
    
    # Add found_state column
    df = df.with_columns([
        pl.col("street_name").map_elements(find_state_in_name, return_dtype=pl.Utf8).alias("found_state")
    ])
    
    # Add color column based on found state
    def get_color_for_state(state_name: str) -> str:
        """Get hex color for a state."""
        if state_name == "Unknown":
            return "#7f7f7f"
        return get_state_color(state_name.lower())
    
    df = df.with_columns([
        pl.col("found_state").map_elements(get_color_for_state, return_dtype=pl.Utf8).alias("color")
    ])
    
    # Create hover text
    df = df.with_columns([
        (pl.col("street_name") + "<br>" + 
         pl.col("highway_type").str.to_titlecase() + " • " +
         pl.col("length_km").round(2).cast(pl.Utf8) + " km").alias("hover_text")
    ])
    
    print("Creating Plotly map...")
    
    # Get unique states for filter list
    unique_states = sorted(df["found_state"].unique().to_list())
    
    # Create the figure using scattermapbox
    fig = go.Figure()
    
    # Group by found_state
    for state_name in unique_states:
        state_df = df.filter(pl.col("found_state") == state_name)
        
        fig.add_trace(go.Scattermapbox(
            lat=state_df["lat"].to_list(),
            lon=state_df["lon"].to_list(),
            mode='markers',
            marker=dict(
                size=5,
                color=state_df["color"].to_list()[0],
                opacity=0.6
            ),
            text=state_df["hover_text"].to_list(),
            hoverinfo='text',
            name=state_name,
            showlegend=False  # No legend - using custom filter controls instead
        ))
    
    # Update layout - remove legend, make responsive
    fig.update_layout(
        mapbox=dict(
            style="carto-positron",
            center=dict(lat=39.8283, lon=-98.5795),
            zoom=3.5
        ),
        height=800,
        margin=dict(l=0, r=0, t=0, b=0),
        showlegend=False,  # No legend
        hovermode='closest'
    )
    
    # Config for interactivity
    config = {
        'displayModeBar': True,
        'displaylogo': False,
        'modeBarButtonsToRemove': ['select2d', 'lasso2d', 'autoScale2d'],
        'doubleClick': False,  # Disable default reset, we'll handle zoom manually
        'scrollZoom': True,
        'responsive': True
    }
    
    # Get Plotly data as JSON for custom HTML
    plotly_data = fig.to_dict()
    
    # Create custom HTML with filtering controls
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>State-Named Streets Map</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        * {{
            box-sizing: border-box;
        }}
        
        body {{
            margin: 0;
            padding: 0;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            overflow: hidden;
        }}
        
        .controls {{
            position: fixed;
            bottom: 10px;
            left: 10px;
            right: 10px;
            z-index: 1000;
            background: rgba(255, 255, 255, 0.98);
            padding: 12px;
            border-radius: 8px;
            box-shadow: 0 2px 12px rgba(0, 0, 0, 0.15);
            max-height: 60vh;
            display: flex;
            flex-direction: column;
            transition: max-height 0.3s ease;
        }}
        
        .controls.collapsed {{
            max-height: 50px;
            overflow: hidden;
        }}
        
        .controls-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 10px;
            cursor: pointer;
        }}
        
        .controls-header h3 {{
            margin: 0;
            font-size: 16px;
            font-weight: 600;
            color: #333;
        }}
        
        .toggle-icon {{
            font-size: 18px;
            color: #666;
            user-select: none;
        }}
        
        .state-filter {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(110px, 1fr));
            gap: 8px;
            max-height: 300px;
            overflow-y: auto;
            padding-right: 4px;
        }}
        
        .state-checkbox {{
            display: flex;
            align-items: center;
            font-size: 13px;
            cursor: pointer;
            padding: 4px;
            border-radius: 4px;
            transition: background-color 0.2s;
        }}
        
        .state-checkbox:hover {{
            background-color: rgba(0, 0, 0, 0.05);
        }}
        
        .state-checkbox input {{
            margin-right: 6px;
            cursor: pointer;
            width: 16px;
            height: 16px;
        }}
        
        .state-checkbox span {{
            cursor: pointer;
            user-select: none;
            flex: 1;
        }}
        
        .filter-actions {{
            display: flex;
            gap: 8px;
            margin-top: 10px;
            padding-top: 10px;
            border-top: 1px solid rgba(0, 0, 0, 0.1);
        }}
        
        .filter-btn {{
            flex: 1;
            padding: 6px 12px;
            font-size: 12px;
            border: 1px solid #ddd;
            border-radius: 4px;
            background: white;
            cursor: pointer;
            transition: all 0.2s;
        }}
        
        .filter-btn:hover {{
            background: #f5f5f5;
            border-color: #999;
        }}
        
        .header {{
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            z-index: 999;
            background: rgba(255, 255, 255, 0.95);
            padding: 10px 15px;
            border-bottom: 1px solid rgba(0, 0, 0, 0.1);
            font-size: 14px;
            line-height: 1.5;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
        }}
        
        .header a {{
            color: #0066cc;
            text-decoration: none;
        }}
        
        .header a:hover {{
            text-decoration: underline;
        }}
        
        #plotly-div {{
            width: 100%;
            height: 100vh;
            padding-top: 50px;
        }}
        
        /* Mobile optimizations */
        @media (max-width: 768px) {{
            .header {{
                padding: 8px 12px;
                font-size: 12px;
            }}
            
            #plotly-div {{
                padding-top: 45px;
            }}
            
            .controls {{
                bottom: 5px;
                left: 5px;
                right: 5px;
                padding: 10px;
                max-height: 55vh;
            }}
            
            .controls-header h3 {{
                font-size: 14px;
            }}
            
            .state-filter {{
                grid-template-columns: repeat(auto-fill, minmax(90px, 1fr));
                gap: 6px;
                max-height: 250px;
                font-size: 12px;
            }}
            
            .state-checkbox {{
                font-size: 12px;
            }}
            
            .filter-btn {{
                font-size: 11px;
                padding: 5px 10px;
            }}
        }}
        
        @media (max-width: 480px) {{
            .state-filter {{
                grid-template-columns: repeat(auto-fill, minmax(75px, 1fr));
            }}
        }}
        
        /* Scrollbar styling */
        .state-filter::-webkit-scrollbar {{
            width: 6px;
        }}
        
        .state-filter::-webkit-scrollbar-track {{
            background: rgba(0, 0, 0, 0.05);
            border-radius: 3px;
        }}
        
        .state-filter::-webkit-scrollbar-thumb {{
            background: rgba(0, 0, 0, 0.2);
            border-radius: 3px;
        }}
        
        .state-filter::-webkit-scrollbar-thumb:hover {{
            background: rgba(0, 0, 0, 0.3);
        }}
    </style>
</head>
<body>
    <div class="header">
        Plotting US Streets that include State Names. Back to Full Article <a href="https://dactile.net/p/state-street-names">here</a>.
    </div>
    <div class="controls collapsed" id="controls">
        <div class="controls-header" onclick="toggleControls()">
            <h3>Filter by State</h3>
            <span class="toggle-icon" id="toggle-icon">▲</span>
        </div>
        <div class="state-filter" id="state-filters"></div>
        <div class="filter-actions">
            <button class="filter-btn" onclick="selectAll()">Select All</button>
            <button class="filter-btn" onclick="selectNone()">Clear All</button>
        </div>
    </div>
    <div id="plotly-div"></div>
    
    <script>
        // Plotly figure data
        const plotlyData = {json.dumps(plotly_data['data'])};
        const plotlyLayout = {json.dumps(plotly_data['layout'])};
        const plotlyConfig = {json.dumps(config)};
        
        // States list
        const states = {json.dumps(unique_states)};
        
        // Initialize checkboxes
        const filterDiv = document.getElementById('state-filters');
        
        states.forEach(state => {{
            const label = document.createElement('label');
            label.className = 'state-checkbox';
            
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.value = state;
            checkbox.checked = true;
            checkbox.id = `state-${{state}}`;
            checkbox.addEventListener('change', updateVisibility);
            
            const labelText = document.createElement('span');
            labelText.textContent = state;
            
            label.appendChild(checkbox);
            label.appendChild(labelText);
            filterDiv.appendChild(label);
        }});
        
        // Initialize Plotly
        Plotly.newPlot('plotly-div', plotlyData, plotlyLayout, plotlyConfig);
        
        // Function to setup map interactions (bounds and double-click zoom)
        function setupMapInteractions() {{
            const gd = document.getElementById('plotly-div');
            if (gd && gd._fullLayout && gd._fullLayout.mapbox && gd._fullLayout.mapbox._subplot && gd._fullLayout.mapbox._subplot.map) {{
                const map = gd._fullLayout.mapbox._subplot.map;
                
                // Set maxBounds to restrict panning to North America
                map.setMaxBounds([
                    [-180, 15],  // Southwest corner (west, south)
                    [-50, 75]    // Northeast corner (east, north)
                ]);
                
                // Add double-click zoom (like Google Maps)
                map.on('dblclick', function(e) {{
                    // Zoom in by 1 level at the clicked location
                    map.zoomIn({{ around: e.lngLat }});
                }});
                
                return true;
            }}
            return false;
        }}
        
        // Try to setup map interactions immediately, or wait for map to be ready
        if (!setupMapInteractions()) {{
            // If map isn't ready yet, try again after a short delay
            setTimeout(function() {{
                if (!setupMapInteractions()) {{
                    // Try one more time after map loads
                    const checkInterval = setInterval(function() {{
                        if (setupMapInteractions()) {{
                            clearInterval(checkInterval);
                        }}
                    }}, 100);
                    // Stop trying after 5 seconds
                    setTimeout(function() {{ clearInterval(checkInterval); }}, 5000);
                }}
            }}, 500);
        }}
        
        // Update visibility based on checked states
        function updateVisibility() {{
            const checked = Array.from(document.querySelectorAll('input[type="checkbox"]:checked'))
                .map(cb => cb.value);
            
            // If nothing checked, show all (better UX)
            const visibility = checked.length === 0 
                ? plotlyData.map(() => true)
                : plotlyData.map((trace, i) => checked.includes(trace.name));
            
            Plotly.restyle('plotly-div', {{'visible': visibility}});
        }}
        
        // Select all states
        function selectAll() {{
            document.querySelectorAll('input[type="checkbox"]').forEach(cb => {{
                cb.checked = true;
            }});
            updateVisibility();
        }}
        
        // Clear all states
        function selectNone() {{
            document.querySelectorAll('input[type="checkbox"]').forEach(cb => {{
                cb.checked = false;
            }});
            updateVisibility();
        }}
        
        // Toggle controls panel
        function toggleControls() {{
            const controls = document.getElementById('controls');
            const icon = document.getElementById('toggle-icon');
            controls.classList.toggle('collapsed');
            icon.textContent = controls.classList.contains('collapsed') ? '▲' : '▼';
        }}
        
        // Handle window resize
        window.addEventListener('resize', function() {{
            Plotly.Plots.resize('plotly-div');
        }});
    </script>
</body>
</html>"""
    
    # Save the custom HTML
    print(f"Saving map to {output_path}...")
    output_path.write_text(html_content)
    print(f"✓ Map saved! File size: {output_path.stat().st_size / 1024 / 1024:.2f} MB")
    
    return fig


def main():
    """Generate state streets map."""
    output_dir = Path(__file__).parent.parent / "output" / "plotly_maps"
    output_dir.mkdir(exist_ok=True, parents=True)
    
    print("="*70)
    print("CREATING STATE STREETS MAP")
    print("="*70)
    
    create_state_streets_map(
        output_path=output_dir / "national_state_streets_plotly.html",
        sample_size=100000
    )
    
    print("\n" + "="*70)
    print("MAP COMPLETE!")
    print("="*70)
    print(f"Map saved to: {output_dir}")


if __name__ == "__main__":
    main()

