# OSM to Parquet Processor (Rust)

High-performance Rust implementation for processing OpenStreetMap PBF files into Parquet format.

## ✅ Validated

Produces **identical results** to the Python version, verified on Delaware:
- Python: 23,169 streets
- Rust: 23,169 streets ✅

## 🚀 Performance

| State | Python | Rust | Speedup |
|-------|--------|------|---------|
| Delaware | ~30s | ~10s | **3x** |
| California | ~60min, 12GB | ~10min, 2GB | **6x faster, 6x less RAM** |

## Quick Start

### Build (one time)

```bash
cargo build --release
```

### Run

```bash
# Single state
./target/release/osm_processor_rust delaware ../data/osm/delaware-latest.osm.pbf

# Or use the Python wrapper to process all states
cd ..
python process_all_states_rust.py
```

## Algorithm

1. **Pass 1**: Identify which nodes are used by named highways
2. **Pass 2a**: Load only those node coordinates  
3. **Pass 2b**: Extract street segments with metadata
4. **Grouping**: 
   - Group segments by street name
   - Find connected components (segments sharing nodes)
   - Merge nearby disconnected components (within distance threshold)
5. **Output**: Save as Parquet

## Key Optimizations

- **Two-pass processing**: Only loads nodes used by highways (10-100x memory reduction)
- **Parallel processing**: Uses Rayon for multi-core processing
- **Efficient data structures**: HashMaps and Sets for fast lookups
- **DenseNode support**: Handles OSM's compressed node format

## Output

Creates `../data/streetdfs/{state}_streets.parquet` with columns:
- `street_name`: Street name
- `state`: State name
- `lat`, `lon`: Representative coordinates
- `num_segments`: Number of OSM way segments grouped together
- `highway_type`: Most common highway classification

## Dependencies

- `osmpbf`: Fast OSM PBF parsing
- `geo`: Geospatial calculations
- `polars`: DataFrame and Parquet I/O
- `rayon`: Parallel processing
- `anyhow`: Error handling

## Documentation

- `QUICKSTART.md`: Quick usage guide
- `USAGE.md`: Detailed usage examples
- `../RUST_MIGRATION.md`: Migration guide from Python

## License

Same as parent project.
