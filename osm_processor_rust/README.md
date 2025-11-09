# OSM to Parquet Processor (Rust)

A memory-efficient Rust implementation for processing OpenStreetMap PBF files into Parquet format.

## Key Improvements Over Python Version

1. **Two-pass processing**: Only loads nodes that are used by named highways (10-100x memory reduction)
2. **Parallel processing**: Uses Rayon for parallel grouping of street names
3. **Spatial indexing**: R-tree for efficient distance-based grouping
4. **Memory efficiency**: ~2-5x less memory usage than Python
5. **Speed**: ~5-10x faster than Python version

## Memory Usage Estimates

| State | OSM File Size | Python RAM | Rust RAM |
|-------|---------------|------------|----------|
| Delaware | 20 MB | ~200 MB | ~50 MB |
| California | 1.5 GB | ~12 GB | ~2 GB |
| Texas | 1.2 GB | ~10 GB | ~1.5 GB |

## Building

```bash
cd workspace/osm_processor_rust
cargo build --release
```

The release build is **much faster** (10-20x) than debug builds due to optimizations.

## Usage

```bash
# Process a state (looks for file in ../data/osm/)
./target/release/osm_processor_rust delaware

# Process with custom file path
./target/release/osm_processor_rust california /path/to/california-latest.osm.pbf

# Process with custom distance threshold (in km)
./target/release/osm_processor_rust california /path/to/ca.osm.pbf 0.2
```

## Output

Creates a Parquet file in `../data/streetdfs/{state}_streets.parquet` with columns:
- `street_name`: Name of the street
- `state`: State name
- `lat`, `lon`: Representative coordinates
- `num_segments`: Number of OSM way segments grouped together
- `highway_type`: Most common highway type (residential, primary, etc.)

## Algorithm

1. **Pass 1**: Scan OSM file to identify which nodes are used by named highways
2. **Pass 2**: 
   - Load only those nodes' coordinates
   - Extract street segments with metadata
3. **Grouping**:
   - Group segments by street name
   - Find connected components (segments sharing nodes)
   - Optionally merge nearby disconnected components (within distance threshold)
4. **Output**: Convert to Polars DataFrame and save as Parquet

## Performance Tips

- Always use `--release` builds for production
- For very large states, consider setting distance threshold to 0 (skip proximity merging)
- Monitor memory with `time -v` on Linux or Activity Monitor on macOS

## Dependencies

- `osmpbf`: Fast OSM PBF parsing
- `geo`: Geospatial calculations (Haversine distance)
- `rstar`: R-tree spatial indexing
- `polars`: DataFrame and Parquet output
- `rayon`: Parallel processing
- `anyhow`: Error handling

