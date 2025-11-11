"""Count total unique street names after filtering."""

import polars as pl
from workspace.load_street_df import load_street_df

def main():
    # Load all streets with default filtering (highway types only)
    lf = load_street_df()
    
    # Collect the data
    df = lf.collect()
    
    # Count unique street names
    unique_names = df.select(pl.col("street_name").n_unique()).item()
    
    # Also get total rows for context
    total_rows = len(df)
    
    print(f"Total unique street names: {unique_names:,}")
    print(f"Total street records (rows): {total_rows:,}")
    
    # Show a few examples
    print("\nSample street names:")
    sample_names = df.select("street_name").unique().head(10)
    for name in sample_names["street_name"]:
        print(f"  - {name}")

if __name__ == "__main__":
    main()

