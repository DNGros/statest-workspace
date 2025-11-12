#!/usr/bin/env python3
"""Create interactive maps using Plotly for better performance."""

import sys
from pathlib import Path
import polars as pl
import plotly.graph_objects as go
import plotly.express as px
from typing import Optional

# Add workspace to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from load_street_df import load_state_streets_df, load_street_df
from states import USState
from state_colors import get_state_color


def create_national_map_plotly(
    output_path: Path,
    sample_size: Optional[int] = 100000,
    title: str = "State-Named Streets Across the US"
):
    """
    Create a national map of state-named streets using Plotly.
    
    Args:
        output_path: Where to save the HTML file
        sample_size: Number of streets to sample (None for all)
        title: Title for the map
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
        (pl.col("street_name") + "<br>Location: " + 
         pl.col("state").str.to_titlecase() + "<br>Named after: " + 
         pl.col("found_state") + "<br>Type: " + 
         pl.col("highway_type")).alias("hover_text")
    ])
    
    print("Creating Plotly map...")
    
    # Create the figure using scattermapbox for performance
    fig = go.Figure()
    
    # Group by found_state for better legend
    for state_name in sorted(df["found_state"].unique().to_list()):
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
            showlegend=True
        ))
    
    # Update layout - simplified UI
    fig.update_layout(
        mapbox=dict(
            style="carto-positron",
            center=dict(lat=39.8283, lon=-98.5795),
            zoom=3.5
        ),
        height=800,
        margin=dict(l=0, r=0, t=0, b=0),
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
            bgcolor="rgba(255, 255, 255, 0.8)",
            font=dict(size=10)
        ),
        hovermode='closest'
    )
    
    # Simplified config - minimal controls, double-click to zoom
    config = {
        'displayModeBar': True,
        'displaylogo': False,
        'modeBarButtonsToRemove': ['select2d', 'lasso2d', 'autoScale2d'],
        'doubleClick': 'reset+autosize',  # Double-click resets view
        'scrollZoom': True  # Enable scroll wheel zoom
    }
    
    # Save the figure with config
    print(f"Saving map to {output_path}...")
    fig.write_html(str(output_path), config=config)
    print(f"✓ Map saved! File size: {output_path.stat().st_size / 1024 / 1024:.2f} MB")
    
    return fig


def create_state_comparison_map_plotly(
    state_name: str,
    output_path: Path,
    title: Optional[str] = None
):
    """
    Create a map showing which state names appear in a given state's streets.
    
    Args:
        state_name: Name of the state to analyze
        output_path: Where to save the HTML file
        title: Optional custom title
    """
    print(f"\nLoading streets for {state_name.title()}...")
    
    # Load all streets from this state
    lf = load_street_df(state=state_name)
    df = lf.collect()
    
    # Filter to state-named streets
    from load_street_df import has_state_name_mask
    mask = has_state_name_mask()
    df = df.filter(mask)
    
    # Exclude numbered streets
    df = df.filter(~pl.col("street_name").str.contains(r"\d", literal=False))
    
    print(f"Found {len(df):,} state-named streets in {state_name.title()}")
    
    if len(df) == 0:
        print(f"No state-named streets found in {state_name}")
        return None
    
    # Identify which state name is in each street
    state_names = USState.all_names()
    
    def find_state_in_name(street_name: str) -> str:
        street_lower = street_name.lower()
        for sname in state_names:
            if sname in street_lower:
                return sname.title()
        return "Unknown"
    
    df = df.with_columns([
        pl.col("street_name").map_elements(find_state_in_name, return_dtype=pl.Utf8).alias("found_state")
    ])
    
    # Add colors
    def get_color_for_state(sname: str) -> str:
        if sname == "Unknown":
            return "#7f7f7f"
        return get_state_color(sname.lower())
    
    df = df.with_columns([
        pl.col("found_state").map_elements(get_color_for_state, return_dtype=pl.Utf8).alias("color")
    ])
    
    # Create hover text
    df = df.with_columns([
        (pl.col("street_name") + "<br>Named after: " + 
         pl.col("found_state") + "<br>Type: " + 
         pl.col("highway_type")).alias("hover_text")
    ])
    
    print("Creating Plotly map...")
    
    # Calculate center
    center_lat = df["lat"].mean()
    center_lon = df["lon"].mean()
    
    # Create figure
    fig = go.Figure()
    
    # Add traces grouped by found_state
    for found_state in sorted(df["found_state"].unique().to_list()):
        state_df = df.filter(pl.col("found_state") == found_state)
        
        fig.add_trace(go.Scattermapbox(
            lat=state_df["lat"].to_list(),
            lon=state_df["lon"].to_list(),
            mode='markers',
            marker=dict(
                size=6,
                color=state_df["color"].to_list()[0],
                opacity=0.7
            ),
            text=state_df["hover_text"].to_list(),
            hoverinfo='text',
            name=found_state,
            showlegend=True
        ))
    
    # Update layout - simplified UI
    fig.update_layout(
        mapbox=dict(
            style="carto-positron",
            center=dict(lat=center_lat, lon=center_lon),
            zoom=6
        ),
        height=800,
        margin=dict(l=0, r=0, t=0, b=0),
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
            bgcolor="rgba(255, 255, 255, 0.8)"
        ),
        hovermode='closest'
    )
    
    # Simplified config
    config = {
        'displayModeBar': True,
        'displaylogo': False,
        'modeBarButtonsToRemove': ['select2d', 'lasso2d', 'autoScale2d'],
        'doubleClick': 'reset+autosize',
        'scrollZoom': True
    }
    
    # Save
    print(f"Saving map to {output_path}...")
    fig.write_html(str(output_path), config=config)
    print(f"✓ Map saved! File size: {output_path.stat().st_size / 1024 / 1024:.2f} MB")
    
    return fig


def create_ego_score_map_plotly(
    csv_path: Path,
    output_path: Path,
    metric: str = 'ego_score',
    title: Optional[str] = None
):
    """
    Create a choropleth-style map showing state-level metrics.
    
    Args:
        csv_path: Path to CSV with state metrics (e.g., state_ego_humility.csv)
        output_path: Where to save the HTML file
        metric: Column to visualize ('ego_score', 'self_pct', 'other_pct')
        title: Optional custom title
    """
    print(f"\nCreating {metric} map...")
    
    # Load the data
    df = pl.read_csv(csv_path)
    
    # State coordinates for plotting
    STATE_COORDS = {
        'alabama': [32.806671, -86.791130],
        'alaska': [61.370716, -152.404419],
        'arizona': [33.729759, -111.431221],
        'arkansas': [34.969704, -92.373123],
        'california': [36.116203, -119.681564],
        'colorado': [39.059811, -105.311104],
        'connecticut': [41.597782, -72.755371],
        'delaware': [39.318523, -75.507141],
        'florida': [27.766279, -81.686783],
        'georgia': [33.040619, -83.643074],
        'hawaii': [21.094318, -157.498337],
        'idaho': [44.240459, -114.478828],
        'illinois': [40.349457, -88.986137],
        'indiana': [39.849426, -86.258278],
        'iowa': [42.011539, -93.210526],
        'kansas': [38.526600, -96.726486],
        'kentucky': [37.668140, -84.670067],
        'louisiana': [31.169546, -91.867805],
        'maine': [44.693947, -69.381927],
        'maryland': [39.063946, -76.802101],
        'massachusetts': [42.230171, -71.530106],
        'michigan': [43.326618, -84.536095],
        'minnesota': [45.694454, -93.900192],
        'mississippi': [32.741646, -89.678696],
        'missouri': [38.456085, -92.288368],
        'montana': [46.921925, -110.454353],
        'nebraska': [41.125370, -98.268082],
        'nevada': [38.313515, -117.055374],
        'new hampshire': [43.452492, -71.563896],
        'new jersey': [40.298904, -74.521011],
        'new mexico': [34.840515, -106.248482],
        'new york': [42.165726, -74.948051],
        'north carolina': [35.630066, -79.806419],
        'north dakota': [47.528912, -99.784012],
        'ohio': [40.388783, -82.764915],
        'oklahoma': [35.565342, -96.928917],
        'oregon': [44.572021, -122.070938],
        'pennsylvania': [40.590752, -77.209755],
        'rhode island': [41.680893, -71.511780],
        'south carolina': [33.856892, -80.945007],
        'south dakota': [44.299782, -99.438828],
        'tennessee': [35.747845, -86.692345],
        'texas': [31.054487, -97.563461],
        'utah': [40.150032, -111.862434],
        'vermont': [44.045876, -72.710686],
        'virginia': [37.769337, -78.169968],
        'washington': [47.400902, -121.490494],
        'west virginia': [38.491226, -80.954453],
        'wisconsin': [44.268543, -89.616508],
        'wyoming': [42.755966, -107.302490],
    }
    
    # Add coordinates to dataframe
    df = df.with_columns([
        pl.col("state").map_elements(lambda s: STATE_COORDS.get(s, [None, None])[0], return_dtype=pl.Float64).alias("lat"),
        pl.col("state").map_elements(lambda s: STATE_COORDS.get(s, [None, None])[1], return_dtype=pl.Float64).alias("lon")
    ])
    
    # Filter out states without coordinates
    df = df.filter(pl.col("lat").is_not_null())
    
    # Create hover text
    df = df.with_columns([
        (pl.col("state").str.to_titlecase() + 
         "<br>Ego Score: " + pl.col("ego_score").round(3).cast(pl.Utf8) +
         "<br>Self-named: " + pl.col("self_named_streets").cast(pl.Utf8) + " (" + pl.col("self_pct").round(2).cast(pl.Utf8) + "%)" +
         "<br>Other-state-named: " + pl.col("other_state_streets").cast(pl.Utf8) + " (" + pl.col("other_pct").round(2).cast(pl.Utf8) + "%)" +
         "<br>Total streets: " + pl.col("total_streets").cast(pl.Utf8)
        ).alias("hover_text")
    ])
    
    # Determine marker sizes and colors based on metric
    metric_values = df[metric].to_list()
    
    # Normalize metric for color scale
    if metric == 'ego_score':
        # Log scale for ego score (can be very large)
        import numpy as np
        color_values = [min(val, 10) for val in metric_values]  # Cap at 10 for color scale
        colorscale = 'RdYlGn_r'  # Red for high ego, green for low
        colorbar_title = "Ego Score"
    elif metric == 'self_pct':
        color_values = metric_values
        colorscale = 'Reds'
        colorbar_title = "Self-Naming %"
    else:  # other_pct
        color_values = metric_values
        colorscale = 'Greens'
        colorbar_title = "Other-State-Naming %"
    
    # Create figure
    fig = go.Figure()
    
    fig.add_trace(go.Scattermapbox(
        lat=df["lat"].to_list(),
        lon=df["lon"].to_list(),
        mode='markers+text',
        marker=dict(
            size=25,
            color=color_values,
            colorscale=colorscale,
            showscale=True,
            colorbar=dict(
                title=colorbar_title,
                thickness=15,
                len=0.7
            ),
            opacity=0.8
        ),
        text=[s[:2].upper() for s in df["state"].to_list()],
        textfont=dict(size=10, color='black'),
        textposition='middle center',
        hovertext=df["hover_text"].to_list(),
        hoverinfo='text',
        showlegend=False
    ))
    
    # Update layout - simplified UI
    fig.update_layout(
        mapbox=dict(
            style="carto-positron",
            center=dict(lat=39.8283, lon=-98.5795),
            zoom=3.2
        ),
        height=700,
        margin=dict(l=0, r=0, t=0, b=0),
        hovermode='closest'
    )
    
    # Simplified config
    config = {
        'displayModeBar': True,
        'displaylogo': False,
        'modeBarButtonsToRemove': ['select2d', 'lasso2d', 'autoScale2d'],
        'doubleClick': 'reset+autosize',
        'scrollZoom': True
    }
    
    # Save
    print(f"Saving map to {output_path}...")
    fig.write_html(str(output_path), config=config)
    print(f"✓ Map saved! File size: {output_path.stat().st_size / 1024 / 1024:.2f} MB")
    
    return fig


def main():
    """Generate all Plotly maps."""
    output_dir = Path(__file__).parent.parent / "output" / "plotly_maps"
    output_dir.mkdir(exist_ok=True, parents=True)
    
    print("="*70)
    print("CREATING PLOTLY MAPS")
    print("="*70)
    
    # 1. National map of state-named streets
    print("\n1. Creating national map...")
    create_national_map_plotly(
        output_path=output_dir / "national_state_streets_plotly.html",
        title="State-Named Streets Across the United States"
    )
    
    # 2. Individual state comparison maps
    print("\n2. Creating state comparison maps...")
    interesting_states = ['california', 'texas', 'delaware', 'wisconsin']
    for state in interesting_states:
        try:
            create_state_comparison_map_plotly(
                state_name=state,
                output_path=output_dir / f"{state}_state_streets_plotly.html"
            )
        except Exception as e:
            print(f"Error creating map for {state}: {e}")
    
    # 3. Ego score maps (if CSV exists)
    print("\n3. Creating ego score maps...")
    csv_path = Path(__file__).parent.parent / "output" / "state_ego_humility.csv"
    if csv_path.exists():
        create_ego_score_map_plotly(
            csv_path=csv_path,
            output_path=output_dir / "state_ego_score_plotly.html",
            metric='ego_score',
            title='State "Ego Score" - Self-Naming vs Other-State-Naming'
        )
        
        create_ego_score_map_plotly(
            csv_path=csv_path,
            output_path=output_dir / "state_self_pct_plotly.html",
            metric='self_pct',
            title='Percentage of Streets Named After Own State'
        )
        
        create_ego_score_map_plotly(
            csv_path=csv_path,
            output_path=output_dir / "state_other_pct_plotly.html",
            metric='other_pct',
            title='Percentage of Streets Named After Other States'
        )
    else:
        print(f"⚠ CSV not found at {csv_path}, skipping ego score maps")
        print("  Run map_all_states.py first to generate the CSV")
    
    print("\n" + "="*70)
    print("ALL PLOTLY MAPS COMPLETE!")
    print("="*70)
    print(f"Maps saved to: {output_dir}")


if __name__ == "__main__":
    main()

