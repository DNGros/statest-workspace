# Workspace for statest

This workspace contains the analysis and exploration code for the blog post about state-named streets.

## Scripts

### `download_state_osm.py`
Downloads OSM (OpenStreetMap) data for a US state from Geofabrik.

**Usage:**
```bash
python download_state_osm.py [state_name]
python download_state_osm.py california
python download_state_osm.py texas
```

Creates a symlink from `data/` to your specified data directory if it doesn't exist.

### `process_osm_to_parquet.py`
Processes OSM PBF files into parquet format with one row per unique street.

Groups street segments using node-based connectivity:
- Segments that share nodes are grouped together
- Disconnected segments within distance threshold (100m) are also grouped
- Handles multiple streets with the same name in the same state

**Usage:**
```bash
python process_osm_to_parquet.py [state_name] [pbf_file]
python process_osm_to_parquet.py delaware
python process_osm_to_parquet.py california /path/to/california-latest.osm.pbf
```

Output: `data/{state}_streets.parquet` with columns:
- `street_name`: Name of the street
- `state`: State name (from filename)
- `lat`, `lon`: Representative coordinates
- `num_segments`: Number of OSM way segments grouped into this street
- `highway_type`: Most common highway type
- Plus other common OSM tags

### `analyze_streets.py`
Analyze street name patterns and generate visualizations.

**Usage:**
```bash
python analyze_streets.py [state_name]
python analyze_streets.py delaware
```

Features:
- Plots top 5 most common street names (seaborn bar chart)
- Identifies streets containing state names (case insensitive)
- Shows distribution of which state names appear in streets
- Creates interactive Folium maps with state-colored markers
- Outputs to `output/` directory

### Supporting Files

- `states.py` - Enum of all 50 US states
- `state_colors.py` - Color scheme for states (50 maximally distinct colors via distinctipy)
- `generate_state_colors.py` - Script to regenerate state colors if needed

## Data

The `data/` directory contains:
- OSM PBF files (downloaded via `download_state_osm.py`)
- Processed parquet files (generated via `process_osm_to_parquet.py`)

Note: `data/` is gitignored - add large data files to your local `.gitignore` if needed.
