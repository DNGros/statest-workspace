#!/usr/bin/env python3
"""Analyze street names from processed parquet files."""

import sys
from pathlib import Path
import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt
import folium
from workspace.states import USState
from workspace.state_colors import get_state_color


def load_state_data(state_name: str, data_dir: Path = None) -> pl.DataFrame:
    """Load parquet file for a state."""
    if data_dir is None:
        data_dir = Path(__file__).parent.parent / "data" / "streetdfs"
    
    # Convert spaces to dashes to match OSM file naming convention
    state_name_file = state_name.replace(' ', '-')
    parquet_path = data_dir / f"{state_name_file}_streets.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(f"No data found for {state_name}: {parquet_path}")
    
    return pl.read_parquet(parquet_path)


def plot_top_street_names(df: pl.DataFrame, top_n: int = 5, output_path: Path = None):
    """Plot most common street names."""
    name_counts = (
        df.group_by('street_name')
        .agg(pl.len().alias('count'))
        .sort('count', descending=True)
        .head(top_n)
    )
    
    plt.figure(figsize=(10, 6))
    sns.barplot(data=name_counts.to_pandas(), x='count', y='street_name', hue='street_name', palette='viridis', legend=False)
    plt.xlabel('Number of Streets')
    plt.ylabel('Street Name')
    plt.title(f'Top {top_n} Most Common Street Names')
    plt.tight_layout()
    
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"Saved plot to {output_path}")
    else:
        plt.show()
    
    return name_counts


def filter_state_named_streets(df: pl.DataFrame, state_names: list[str] = None) -> pl.DataFrame:
    """Filter streets that contain any state name (case insensitive)."""
    if state_names is None:
        state_names = USState.all_names()
    
    # Create regex pattern: match any state name as whole word (case insensitive)
    # Use word boundaries to avoid partial matches
    pattern = '|'.join([f'(?i){name}' for name in state_names])
    
    return df.filter(pl.col('street_name').str.contains(pattern))


def analyze_state_named_streets(df: pl.DataFrame, state_names: list[str] = None):
    """Analyze distribution of state-named streets."""
    state_streets = filter_state_named_streets(df, state_names)
    
    print(f"\nTotal streets: {len(df):,}")
    print(f"State-named streets: {len(state_streets):,} ({len(state_streets)/len(df)*100:.2f}%)")
    
    # Count which state names appear most
    if state_names is None:
        state_names = USState.all_names()
    
    state_counts = {}
    for state in state_names:
        count = len(df.filter(pl.col('street_name').str.to_lowercase().str.contains(state)))
        if count > 0:
            state_counts[state] = count
    
    print("\nState names found in streets:")
    for state, count in sorted(state_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  {state.title()}: {count}")
    
    return state_streets


def map_streets(df: pl.DataFrame, street_name: str = None, output_path: Path = None, color_by_state_name: bool = False, tiles: str = 'CartoDB Positron'):
    """Create interactive map of streets (points only for now).
    
    Args:
        tiles: Map tile style. Options include:
            - 'CartoDB Positron' (clean, minimal - good default)
            - 'CartoDB Dark Matter' (dark theme)
            - 'OpenStreetMap' (standard OSM)
            - 'Stamen Toner' (high contrast B&W)
            - 'Stamen Toner Lite' (lighter B&W)
            - 'Stamen Terrain' (shows terrain)
    """
    if street_name:
        df = df.filter(pl.col('street_name') == street_name)
        title = f"Map of '{street_name}'"
    else:
        title = "Street Map"
    
    if len(df) == 0:
        print("No streets to map!")
        return None
    
    # Calculate center of map
    center_lat = df['lat'].mean()
    center_lon = df['lon'].mean()
    
    # Create map with specified tiles
    m = folium.Map(location=[center_lat, center_lon], zoom_start=9, tiles=tiles)
    
    # Add markers for each street
    for row in df.iter_rows(named=True):
        popup_text = f"{row['street_name']}<br>Segments: {row.get('num_segments', 1)}"
        
        # Determine color
        if color_by_state_name:
            # Find which state name is in this street name
            street_lower = row['street_name'].lower()
            color = '#7f7f7f'  # Default gray
            for state in USState.all_names():
                if state in street_lower:
                    color = get_state_color(state)
                    break
        else:
            color = 'red'
        
        folium.CircleMarker(
            location=[row['lat'], row['lon']],
            radius=3,
            popup=popup_text,
            color=color,
            fill=True,
            fillColor=color,
            fillOpacity=0.7
        ).add_to(m)
    
    if output_path:
        m.save(str(output_path))
        print(f"Saved map to {output_path}")
    
    return m


def main():
    """Run analysis."""
    if len(sys.argv) < 2:
        print("Usage: python analyze_streets.py <state_name>")
        print("Example: python analyze_streets.py delaware")
        sys.exit(1)
    
    state_name = sys.argv[1].lower()
    
    print(f"Loading data for {state_name}...")
    df = load_state_data(state_name)
    
    print(f"\n{'='*70}")
    print("TOP STREET NAMES")
    print('='*70)
    
    output_dir = Path(__file__).parent.parent / "output"
    output_dir.mkdir(exist_ok=True)
    
    top_names = plot_top_street_names(df, top_n=5, output_path=output_dir / f"{state_name}_top_streets.png")
    print(top_names)
    
    print(f"\n{'='*70}")
    print("STATE-NAMED STREETS")
    print('='*70)
    
    state_streets = analyze_state_named_streets(df)
    
    if len(state_streets) > 0:
        print("\nSample state-named streets:")
        print(state_streets.select(['street_name', 'state']).head(10))


if __name__ == "__main__":
    main()

