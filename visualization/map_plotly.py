#!/usr/bin/env python3
"""Create an interactive Plotly map of state-named streets."""

import sys
from pathlib import Path
import polars as pl
import plotly.graph_objects as go
from typing import Optional

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
         pl.col("state").str.to_titlecase() + "<br>" + 
         pl.col("highway_type").str.to_titlecase() + " • " +
         pl.col("length_km").round(2).cast(pl.Utf8) + " km").alias("hover_text")
    ])
    
    print("Creating Plotly map...")
    
    # Create the figure using scattermapbox
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
    
    # Update layout
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
    
    # Config for interactivity
    config = {
        'displayModeBar': True,
        'displaylogo': False,
        'modeBarButtonsToRemove': ['select2d', 'lasso2d', 'autoScale2d'],
        'doubleClick': 'reset+autosize',
        'scrollZoom': True
    }
    
    # Save the figure
    print(f"Saving map to {output_path}...")
    fig.write_html(str(output_path), config=config)
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

