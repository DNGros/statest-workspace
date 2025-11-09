# How to Use the OSM Processor (Rust)

## Build

```bash
# For testing (fast compile, slow runtime):
cargo build

# For production (slow compile, FAST runtime - 10-20x faster!):
cargo build --release
```

## Usage

After building, run the binary:

```bash
# Debug build (from workspace/osm_processor_rust/):
./target/debug/osm_processor_rust delaware

# Release build (MUCH FASTER - use this for real work):
./target/release/osm_processor_rust delaware

# With custom file path:
./target/release/osm_processor_rust california ../data/osm/california-latest.osm.pbf

# With custom distance threshold (in km):
./target/release/osm_processor_rust texas ../data/osm/texas-latest.osm.pbf 0.2
```

## Expected Performance

### Debug vs Release Build
- **Debug**: Good for testing, ~5-10x slower
- **Release**: Production use, fully optimized

### Memory Usage (Release Build)
- **Delaware** (~20 MB OSM): ~50 MB RAM
- **California** (~1.5 GB OSM): ~2 GB RAM (vs ~12 GB in Python!)
- **Texas** (~1.2 GB OSM): ~1.5 GB RAM

### Speed (Release Build)
- **Delaware**: ~10-30 seconds
- **California**: ~5-10 minutes (vs 30-60 min in Python)
- **Texas**: ~4-8 minutes

## Output

Creates parquet file in `../data/streetdfs/{state}_streets.parquet`

## Next Steps

1. Build in release mode: `cargo build --release`
2. Test on a small state first (Delaware)
3. Then try California!

## Comparing to Python

To compare results:
```python
import polars as pl

# Load both
rust_df = pl.read_parquet("data/streetdfs/delaware_streets.parquet")
python_df = pl.read_parquet("data/streetdfs/delaware_streets_python.parquet")

print(f"Rust:   {len(rust_df)} streets")
print(f"Python: {len(python_df)} streets")
```

