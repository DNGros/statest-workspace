# OSM to Parquet Processor (Rust)

High-performance Rust implementation for processing OpenStreetMap PBF files into Parquet format.

## ✅ Validated

Produces **identical results** to the Python version, verified on Delaware:
- Python: 23,169 streets
- Rust: 23,169 streets ✅

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

## Dependencies

- `osmpbf`: Fast OSM PBF parsing
- `geo`: Geospatial calculations
- `polars`: DataFrame and Parquet I/O
- `rayon`: Parallel processing
- `anyhow`: Error handling