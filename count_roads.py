"""Quick script to count total roads after type filtering."""

from workspace.load_street_df import load_street_df
import polars as pl

# Load with default filter types
lf = load_street_df()

# Count rows efficiently using lazy evaluation
count = lf.select(pl.len()).collect().item()

print(f"Total roads after type filtering: {count:,}")
print(f"Total roads: {count:,} ({count/1_000_000:.2f}M)")

