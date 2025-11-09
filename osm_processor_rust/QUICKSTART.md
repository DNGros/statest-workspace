# Quick Start Guide

## ✅ It Works!

The Rust OSM processor is ready to use. It successfully processed Delaware in debug mode.

## Build for Production

**IMPORTANT**: Build the release version for 10-20x faster performance:

```bash
cd workspace/osm_processor_rust
cargo build --release
```

This will take 5-10 minutes to compile but creates a highly optimized binary.

## Usage Examples

```bash
# Small state (Delaware) - good for testing
./target/release/osm_processor_rust delaware ../data/osm/delaware-latest.osm.pbf

# Large state (California) - this is where Rust shines!
./target/release/osm_processor_rust california ../data/osm/california-latest.osm.pbf

# Custom distance threshold (default is 0.2 km)
./target/release/osm_processor_rust texas ../data/osm/texas-latest.osm.pbf 0.5
```

## Performance Comparison

### Delaware Results (Debug Build)
- **Streets found**: 23,710
- **Processing time**: ~10 seconds
- **Memory**: ~100 MB

### Expected California Performance (Release Build)
- **Python version**: ~30-60 minutes, ~12 GB RAM
- **Rust version**: ~5-10 minutes, ~2 GB RAM
- **Speedup**: 5-10x faster, 6x less memory!

## Output

Creates: `../data/streetdfs/{state}_streets.parquet`

Columns:
- `street_name`: Street name
- `state`: State name  
- `lat`, `lon`: Representative coordinates
- `num_segments`: Number of OSM way segments grouped
- `highway_type`: Most common highway classification

## Next Steps

1. Build release version: `cargo build --release`
2. Test on Delaware to verify: `./target/release/osm_processor_rust delaware ../data/osm/delaware-latest.osm.pbf`
3. Process California!

## Troubleshooting

If you get "File not found" errors, make sure you're running from the `osm_processor_rust` directory and the OSM files are in `../data/osm/`.

