#!/usr/bin/env python3
"""Map streets across all 50 US states."""

import sys
from pathlib import Path
from typing import Optional
import polars as pl
import folium
from folium.plugins import MarkerCluster
from workspace.states import USState
from workspace.state_colors import get_state_color
from workspace.analyze_streets import load_state_data, filter_state_named_streets


def load_all_states(data_dir: Path = None) -> pl.DataFrame:
    """Load and combine data from all states."""
    if data_dir is None:
        data_dir = Path(__file__).parent / "data" / "streetdfs"
    
    # Common columns we want to keep
    common_cols = ['street_name', 'state', 'lat', 'lon', 'num_segments', 'highway_type']
    
    all_dfs = []
    missing_states = []
    
    print("Loading state data...")
    for state in USState.all_names():
        try:
            df = load_state_data(state, data_dir)
            # Select only common columns and cast to consistent types
            df = df.select([
                pl.col('street_name'),
                pl.col('state'),
                pl.col('lat'),
                pl.col('lon'),
                pl.col('num_segments').cast(pl.Int64),  # Ensure consistent type
                pl.col('highway_type')
            ])
            all_dfs.append(df)
            print(f"  ✓ {state.title()}: {len(df):,} streets")
        except FileNotFoundError:
            missing_states.append(state)
            print(f"  ✗ {state.title()}: No data found")
    
    if missing_states:
        print(f"\nWarning: Missing data for {len(missing_states)} states: {', '.join(missing_states)}")
    
    combined = pl.concat(all_dfs)
    print(f"\nTotal streets across all states: {len(combined):,}")
    return combined


def analyze_state_name_popularity(df: pl.DataFrame, output_dir: Path = None) -> pl.DataFrame:
    """Analyze which state names appear most frequently in street names across all states."""
    state_names = USState.all_names()
    
    results = []
    for state_name in state_names:
        # Count streets containing this state name
        count = len(df.filter(pl.col('street_name').str.to_lowercase().str.contains(state_name)))
        
        if count > 0:
            results.append({
                'state_name': state_name,
                'street_count': count,
            })
    
    result_df = pl.DataFrame(results).sort('street_count', descending=True)
    
    print("\n" + "="*70)
    print("STATE NAME POPULARITY IN STREET NAMES")
    print("="*70)
    print(result_df)
    
    if output_dir:
        output_path = output_dir / "state_name_popularity.csv"
        result_df.write_csv(output_path)
        print(f"\nSaved to {output_path}")
    
    return result_df


def analyze_state_ego_vs_humility(df: pl.DataFrame, output_dir: Path = None) -> pl.DataFrame:
    """Analyze how often each state names streets after itself vs other states."""
    state_names = USState.all_names()
    
    results = []
    for state in state_names:
        # Get all streets in this state
        state_df = df.filter(pl.col('state') == state)
        
        if len(state_df) == 0:
            continue
        
        # Count streets named after this state (in this state)
        self_named = len(state_df.filter(
            pl.col('street_name').str.to_lowercase().str.contains(state)
        ))
        
        # Count streets named after OTHER states (in this state)
        other_state_count = 0
        for other_state in state_names:
            if other_state != state:
                count = len(state_df.filter(
                    pl.col('street_name').str.to_lowercase().str.contains(other_state)
                ))
                other_state_count += count
        
        total_streets = len(state_df)
        self_pct = (self_named / total_streets * 100) if total_streets > 0 else 0
        other_pct = (other_state_count / total_streets * 100) if total_streets > 0 else 0
        
        # Ego score: ratio of self-named to other-named
        ego_score = self_named / other_state_count if other_state_count > 0 else float('inf')
        
        results.append({
            'state': state,
            'total_streets': total_streets,
            'self_named_streets': self_named,
            'other_state_streets': other_state_count,
            'self_pct': self_pct,
            'other_pct': other_pct,
            'ego_score': ego_score if ego_score != float('inf') else 999.0,
        })
    
    result_df = pl.DataFrame(results).sort('ego_score', descending=True)
    
    print("\n" + "="*70)
    print("STATE EGO vs HUMILITY ANALYSIS")
    print("="*70)
    print("(Ego score = self-named streets / other-state-named streets)")
    print("\nMost 'Egotistical' States (high self-naming):")
    print(result_df.head(10))
    print("\nMost 'Humble' States (high other-state naming):")
    print(result_df.tail(10))
    
    if output_dir:
        output_path = output_dir / "state_ego_humility.csv"
        result_df.write_csv(output_path)
        print(f"\nSaved to {output_path}")
    
    return result_df


def analyze_highway_type_distribution(df: pl.DataFrame, output_dir: Path = None) -> pl.DataFrame:
    """Analyze the distribution of highway types among state-named streets."""
    # Filter to state-named streets
    state_named_df = filter_state_named_streets(df)
    
    if len(state_named_df) == 0:
        print("No state-named streets found!")
        return pl.DataFrame()
    
    # Count highway types
    highway_type_counts = (
        state_named_df
        .group_by('highway_type')
        .agg(pl.len().alias('count'))
        .sort('count', descending=True)
    )
    
    # Calculate percentages
    total = len(state_named_df)
    highway_type_counts = highway_type_counts.with_columns([
        (pl.col('count') / total * 100).alias('percentage')
    ])
    
    print("\n" + "="*70)
    print("HIGHWAY TYPE DISTRIBUTION (State-Named Streets)")
    print("="*70)
    print(f"Total state-named streets: {total:,}")
    print("\nHighway type distribution:")
    print(highway_type_counts)
    
    if output_dir:
        output_path = output_dir / "highway_type_distribution.csv"
        highway_type_counts.write_csv(output_path)
        print(f"\nSaved to {output_path}")
    
    return highway_type_counts


def create_national_map(
    df: pl.DataFrame,
    output_path: Path,
    filter_state_names: bool = False,
    sample_size: Optional[int] = None,
    use_clusters: bool = True,
    tiles: str = 'CartoDB Positron'
):
    """Create a national map of streets.
    
    Args:
        df: DataFrame with street data
        output_path: Where to save the map
        filter_state_names: If True, only show streets with state names
        sample_size: If provided, randomly sample this many streets
        use_clusters: If True, use marker clustering for better performance
        tiles: Map tile style
    """
    if filter_state_names:
        df = filter_state_named_streets(df)
        print(f"Filtered to {len(df):,} state-named streets")
    
    if sample_size and len(df) > sample_size:
        df = df.sample(sample_size)
        print(f"Sampled {sample_size:,} streets for visualization")
    
    if len(df) == 0:
        print("No streets to map!")
        return None
    
    # Calculate center of map (geographic center of US)
    center_lat = 39.8283  # Geographic center of contiguous US
    center_lon = -98.5795
    
    # Create map
    m = folium.Map(location=[center_lat, center_lon], zoom_start=4, tiles=tiles)
    
    # Use marker cluster if requested
    if use_clusters:
        marker_cluster = MarkerCluster().add_to(m)
        target = marker_cluster
    else:
        target = m
    
    # Add markers
    print("Adding markers to map...")
    for i, row in enumerate(df.iter_rows(named=True)):
        if i % 10000 == 0 and i > 0:
            print(f"  Added {i:,} markers...")
        
        # Find which state name is in this street name
        street_lower = row['street_name'].lower()
        found_state = None
        for state_name in USState.all_names():
            if state_name in street_lower:
                found_state = state_name
                break
        
        # Color by the state name found in the street name
        highway_type = row.get('highway_type', 'N/A')
        if found_state:
            color = get_state_color(found_state)
            popup_text = f"{row['street_name']}<br>Location: {row['state'].title()}<br>Highway type: {highway_type}"
        else:
            color = '#7f7f7f'  # Gray for streets without state names
            popup_text = f"{row['street_name']}<br>Location: {row['state'].title()}<br>Highway type: {highway_type}"
        
        folium.CircleMarker(
            location=[row['lat'], row['lon']],
            radius=3,
            popup=popup_text,
            color=color,
            fill=True,
            fillColor=color,
            fillOpacity=0.6
        ).add_to(target)
    
    m.save(str(output_path))
    print(f"\nSaved national map to {output_path}")
    return m


def create_state_comparison_map(
    df: pl.DataFrame,
    state_name: str,
    output_path: Path,
    tiles: str = 'CartoDB Positron'
):
    """Create a map showing which OTHER state names appear in a given state's streets."""
    state_df = df.filter(pl.col('state') == state_name)
    state_named_df = filter_state_named_streets(state_df)
    
    if len(state_named_df) == 0:
        print(f"No state-named streets found in {state_name}")
        return None
    
    # Calculate center
    center_lat = state_df['lat'].mean()
    center_lon = state_df['lon'].mean()
    
    # Create map
    m = folium.Map(location=[center_lat, center_lon], zoom_start=7, tiles=tiles)
    
    # Add markers colored by which state name they contain
    for row in state_named_df.iter_rows(named=True):
        street_lower = row['street_name'].lower()
        
        # Find which state name is in this street
        found_state = None
        for other_state in USState.all_names():
            if other_state in street_lower:
                found_state = other_state
                break
        
        if found_state:
            color = get_state_color(found_state)
            highway_type = row.get('highway_type', 'N/A')
            popup_text = f"{row['street_name']}<br>Highway type: {highway_type}"
            
            folium.CircleMarker(
                location=[row['lat'], row['lon']],
                radius=4,
                popup=popup_text,
                color=color,
                fill=True,
                fillColor=color,
                fillOpacity=0.7
            ).add_to(m)
    
    m.save(str(output_path))
    print(f"Saved {state_name} comparison map to {output_path}")
    return m


def main():
    """Run comprehensive mapping analysis."""
    output_dir = Path(__file__).parent / "output"
    output_dir.mkdir(exist_ok=True)
    
    # Load all state data
    print("\n" + "="*70)
    print("LOADING ALL STATE DATA")
    print("="*70)
    df = load_all_states()
    
    # Analyze state name popularity
    analyze_state_name_popularity(df, output_dir)
    
    # Analyze state ego vs humility
    analyze_state_ego_vs_humility(df, output_dir)
    
    # Analyze highway type distribution for state-named streets
    analyze_highway_type_distribution(df, output_dir)
    
    # Create national map of state-named streets
    print("\n" + "="*70)
    print("CREATING NATIONAL MAP")
    print("="*70)
    create_national_map(
        df,
        output_path=output_dir / "national_state_streets_map.html",
        filter_state_names=True,
        sample_size=50000,  # Sample for performance
        use_clusters=False,
        tiles='CartoDB Positron'
    )
    
    # Create individual state comparison maps for interesting states
    print("\n" + "="*70)
    print("CREATING STATE COMPARISON MAPS")
    print("="*70)
    
    # Example: Create maps for a few interesting states
    interesting_states = ['california', 'texas', 'new york', 'delaware']
    for state in interesting_states:
        try:
            print(f"\nCreating map for {state.title()}...")
            create_state_comparison_map(
                df,
                state,
                output_path=output_dir / f"{state}_state_streets_comparison.html"
            )
        except Exception as e:
            print(f"Error creating map for {state}: {e}")
    
    print("\n" + "="*70)
    print("ANALYSIS COMPLETE!")
    print("="*70)
    print(f"All outputs saved to: {output_dir}")


if __name__ == "__main__":
    main()

