#!/usr/bin/env python3
"""
Process OSM PBF data into a parquet file with one row per unique street.

Groups street segments using node-based connectivity:
- Segments that share nodes are grouped together
- Disconnected segments within distance threshold are also grouped
- This handles multiple streets with the same name in the same state

Usage:
    python process_osm_to_parquet.py [state_name] [pbf_file]
    
Example:
    python process_osm_to_parquet.py delaware
    python process_osm_to_parquet.py california /path/to/california-latest.osm.pbf
"""

import sys
import re
from pathlib import Path
from collections import defaultdict
from math import sqrt
import osmium
import polars as pl


class StreetProcessor(osmium.SimpleHandler):
    """Process OSM ways to extract street information."""
    
    def __init__(self, state_name: str):
        """
        Args:
            state_name: Name of the state (for metadata)
        """
        super().__init__()
        self.state_name = state_name
        self.node_coords = {}
        self.street_segments = []  # List of all segments with names
        
    def node(self, n):
        """Store node coordinates."""
        if n.location.valid():
            self.node_coords[n.id] = (n.location.lat, n.location.lon)
    
    def way(self, w):
        """Process ways (streets) with names."""
        name = None
        tags = {}
        
        for tag in w.tags:
            key = tag.k
            value = tag.v
            tags[key] = value
            if key == 'name':
                name = value
        
        if name and 'highway' in tags:
            # Collect node IDs and coordinates for this way
            node_ids = []
            node_coords_list = []
            
            for node_ref in w.nodes:
                if node_ref.ref in self.node_coords:
                    node_ids.append(node_ref.ref)
                    lat, lon = self.node_coords[node_ref.ref]
                    node_coords_list.append((lat, lon))
            
            if node_coords_list:
                # Use first node as representative coordinates
                rep_lat, rep_lon = node_coords_list[0]
                
                segment_info = {
                    'street_name': name,
                    'state': self.state_name,
                    'rep_lat': rep_lat,
                    'rep_lon': rep_lon,
                    'way_id': w.id,
                    'node_ids': set(node_ids),  # Store as set for fast intersection
                    'node_coords': node_coords_list,  # Store all coordinates for distance checks
                    'highway_type': tags.get('highway', ''),
                    'tags': tags
                }
                
                self.street_segments.append(segment_info)


def haversine_distance_approx(lat1, lon1, lat2, lon2):
    """
    Approximate distance between two points in kilometers.
    Uses simple approximation: 1 degree ≈ 111 km.
    Good enough for grouping disconnected segments.
    """
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    return sqrt(dlat * dlat + dlon * dlon) * 111.0


def find_connected_components(segments, distance_threshold_km=0.2):
    """
    Find connected components of segments using node sharing and distance.
    
    Args:
        segments: List of segment dicts with 'node_ids' (set) and 'node_coords' (list)
        distance_threshold_km: Maximum distance to consider segments connected (default 0.2km = 200m)
    
    Returns:
        List of lists, where each inner list contains indices of connected segments
    """
    if len(segments) == 0:
        return []
    
    n = len(segments)
    # Build connectivity graph
    connections = defaultdict(set)
    
    # Step 1: Connect segments that share nodes (fast set intersection)
    for i in range(n):
        for j in range(i + 1, n):
            if segments[i]['node_ids'] & segments[j]['node_ids']:  # Share any nodes
                connections[i].add(j)
                connections[j].add(i)
    
    # Step 2: Connect disconnected segments that are within distance threshold
    for i in range(n):
        for j in range(i + 1, n):
            if j not in connections[i]:  # Not already connected
                # Check minimum distance between any nodes
                min_dist = float('inf')
                for lat1, lon1 in segments[i]['node_coords']:
                    for lat2, lon2 in segments[j]['node_coords']:
                        dist = haversine_distance_approx(lat1, lon1, lat2, lon2)
                        min_dist = min(min_dist, dist)
                
                if min_dist < distance_threshold_km:
                    connections[i].add(j)
                    connections[j].add(i)
    
    # Step 3: Find connected components using BFS
    visited = set()
    components = []
    
    for i in range(n):
        if i not in visited:
            component = []
            queue = [i]
            visited.add(i)
            
            while queue:
                current = queue.pop(0)
                component.append(current)
                for neighbor in connections[current]:
                    if neighbor not in visited:
                        visited.add(neighbor)
                        queue.append(neighbor)
            
            components.append(component)
    
    return components


def group_segments_into_streets(segments, distance_threshold_km=0.2):
    """
    Group segments into unique streets using node-based connectivity.
    
    Segments are grouped if they:
    1. Have the same name and state
    2. Share nodes OR are within distance threshold
    
    Returns:
        List of dicts, one per unique street
    """
    # First group by (name, state) to reduce comparisons
    by_name_state = defaultdict(list)
    for i, seg in enumerate(segments):
        key = (seg['street_name'], seg['state'])
        by_name_state[key].append(i)
    
    streets = []
    
    # Process each (name, state) group separately
    for (name, state), indices in by_name_state.items():
        # Get segments for this name/state
        name_segments = [segments[i] for i in indices]
        
        # Find connected components
        components = find_connected_components(name_segments, distance_threshold_km)
        
        # Create one street per connected component
        for component_indices in components:
            segs = [name_segments[i] for i in component_indices]
            
            # Use first segment's representative coordinates
            rep_lat = segs[0]['rep_lat']
            rep_lon = segs[0]['rep_lon']
            
            # Collect metadata
            highway_types = [s['highway_type'] for s in segs if s['highway_type']]
            most_common_highway = max(set(highway_types), key=highway_types.count) if highway_types else None
            
            # Collect tags that appear frequently (in at least 50% of segments)
            tag_counts = defaultdict(int)
            all_tag_keys = set()
            for seg in segs:
                for key in seg['tags']:
                    all_tag_keys.add(key)
                    tag_counts[key] += 1
            
            # Keep tags that appear in majority of segments
            common_tags = {}
            threshold = len(segs) * 0.5
            for key in all_tag_keys:
                if tag_counts[key] >= threshold:
                    # Use most common value
                    values = [s['tags'].get(key) for s in segs if key in s['tags']]
                    if values:
                        most_common_value = max(set(values), key=values.count)
                        common_tags[key] = most_common_value
            
            street_info = {
                'street_name': name,
                'state': state,
                'lat': rep_lat,
                'lon': rep_lon,
                'num_segments': len(segs),
                'highway_type': most_common_highway,
                **common_tags  # Add common tags as separate columns
            }
            
            streets.append(street_info)
    
    return streets


def process_osm_to_parquet(pbf_path: Path, state_name: str, output_path: Path = None):
    """
    Process OSM PBF file and save streets to parquet.
    
    Args:
        pbf_path: Path to OSM PBF file
        state_name: Name of the state (for metadata)
        output_path: Output parquet file path (default: data/streetdfs/{state}_streets.parquet)
    """
    if not pbf_path.exists():
        print(f"Error: File not found: {pbf_path}")
        sys.exit(1)
    
    if output_path is None:
        # Save to data/streetdfs/ directory
        script_dir = Path(__file__).parent.parent
        streetdfs_dir = script_dir / "data" / "streetdfs"
        streetdfs_dir.mkdir(parents=True, exist_ok=True)
        output_path = streetdfs_dir / f"{state_name}_streets.parquet"
    
    print(f"Processing OSM file: {pbf_path}")
    print(f"State: {state_name}")
    print("Reading OSM data...")
    
    # Process OSM file
    processor = StreetProcessor(state_name)
    processor.apply_file(str(pbf_path))
    
    print(f"Found {len(processor.street_segments):,} street segments")
    
    # Group segments into unique streets
    print("Grouping segments into unique streets...")
    streets = group_segments_into_streets(processor.street_segments)
    
    print(f"Found {len(streets):,} unique streets")
    
    # Convert to polars DataFrame
    print("Creating DataFrame...")
    df = pl.DataFrame(streets)
    
    # Show some statistics
    print("\n" + "=" * 70)
    print("SUMMARY STATISTICS")
    print("=" * 70)
    print(f"Total unique streets: {len(df):,}")
    print(f"Streets with multiple segments: {len(df.filter(pl.col('num_segments') > 1)):,}")
    
    # Show most common street names
    print("\nTop 10 street names:")
    name_counts = df.group_by('street_name').agg(pl.len().alias('count')).sort('count', descending=True)
    print(name_counts.head(10))
    
    # Show columns
    print(f"\nColumns in output: {df.columns}")
    
    # Save to parquet
    print(f"\nSaving to: {output_path}")
    df.write_parquet(output_path)
    print("Done!")
    
    return df


def main():
    """Main function."""
    if len(sys.argv) < 2:
        print("Usage: python process_osm_to_parquet.py <state_name> [pbf_file]")
        print("Example: python process_osm_to_parquet.py delaware")
        sys.exit(1)
    
    state_name = sys.argv[1].lower()
    
    if len(sys.argv) > 2:
        pbf_path = Path(sys.argv[2])
    else:
        # Default: look for state file in data/osm directory
        script_dir = Path(__file__).parent.parent
        osm_dir = script_dir / "data" / "osm"
        pbf_path = osm_dir / f"{state_name}-latest.osm.pbf"
    
    process_osm_to_parquet(pbf_path, state_name)


if __name__ == "__main__":
    main()

