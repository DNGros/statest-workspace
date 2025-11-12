"""Analyze how often US president surnames appear in street names."""

from pathlib import Path
import polars as pl
import sys

# Add workspace to path to import load_street_df
workspace_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(workspace_dir))

from workspace.load_street_df import load_street_df
from workspace.explore.president_surnames.presidents import PRESIDENT_SURNAMES


def has_president_surname_mask() -> pl.Expr:
    """
    Returns a boolean mask expression that identifies streets with president surnames.
    
    Similar to has_state_name_mask() from load_street_df, this checks if the street_name
    contains any US president surname as a whole word (case-insensitive).
    
    Returns:
        polars Expr that evaluates to a boolean Series
    """
    # Start with False and OR together all surname matches
    mask = pl.lit(False)
    for surname in PRESIDENT_SURNAMES:
        # Escape special regex characters and wrap with word boundaries
        escaped_name = (
            surname.replace("\\", "\\\\")
            .replace(".", "\\.")
            .replace("(", "\\(")
            .replace(")", "\\)")
        )
        pattern = r"(?i)\b" + escaped_name + r"\b"
        # Check if street_name contains the surname as a whole word (case-insensitive via (?i))
        mask = mask | pl.col("street_name").str.contains(pattern)
    
    return mask


def count_president_streets(state=None, filter_to_types=None):
    """
    Count streets with president surnames in their names.
    
    Args:
        state: State name(s) to analyze, or None for all states
        filter_to_types: Highway types to include, or None for all types
    
    Returns:
        DataFrame with counts by president surname
    """
    print("Loading street data...")
    lf = load_street_df(state=state, filter_to_types=filter_to_types)
    
    print("Filtering to streets with president surnames...")
    mask = has_president_surname_mask()
    president_streets = lf.filter(mask)
    
    print("Collecting data...")
    df = president_streets.collect()
    
    print(f"\nFound {len(df):,} streets with president surnames")
    
    # Count occurrences of each president surname
    print("\nCounting by president surname...")
    surname_counts = []
    
    for surname in PRESIDENT_SURNAMES:
        # Create case-insensitive pattern with word boundaries
        escaped_name = (
            surname.replace("\\", "\\\\")
            .replace(".", "\\.")
            .replace("(", "\\(")
            .replace(")", "\\)")
        )
        pattern = r"(?i)\b" + escaped_name + r"\b"
        
        count = df.filter(
            pl.col("street_name").str.contains(pattern)
        ).height
        
        surname_counts.append({
            "president_surname": surname,
            "street_count": count,
        })
    
    # Create DataFrame and sort by count
    result = pl.DataFrame(surname_counts).sort("street_count", descending=True)
    
    return result


def analyze_by_state():
    """
    Analyze president street names by state.
    
    Returns:
        DataFrame with counts by president surname and state
    """
    print("Loading street data for all states...")
    lf = load_street_df()
    
    print("Filtering to streets with president surnames...")
    mask = has_president_surname_mask()
    president_streets = lf.filter(mask)
    
    print("Collecting data...")
    df = president_streets.collect()
    
    print(f"\nFound {len(df):,} streets with president surnames")
    
    # For each president, count by state
    print("\nCounting by president surname and state...")
    results = []
    
    for surname in PRESIDENT_SURNAMES:
        escaped_name = (
            surname.replace("\\", "\\\\")
            .replace(".", "\\.")
            .replace("(", "\\(")
            .replace(")", "\\)")
        )
        pattern = r"(?i)\b" + escaped_name + r"\b"
        
        surname_df = df.filter(
            pl.col("street_name").str.contains(pattern)
        )
        
        # Count by state
        state_counts = (
            surname_df
            .group_by("state")
            .agg(pl.len().alias("street_count"))
            .with_columns(pl.lit(surname).alias("president_surname"))
        )
        
        results.append(state_counts)
    
    # Combine all results
    result = pl.concat(results)
    result = result.sort(["president_surname", "street_count"], descending=[False, True])
    
    return result


if __name__ == "__main__":
    print("=" * 80)
    print("US PRESIDENT SURNAMES IN STREET NAMES")
    print("=" * 80)
    
    # Overall counts
    print("\n" + "=" * 80)
    print("OVERALL COUNTS (All States)")
    print("=" * 80)
    overall = count_president_streets()
    print(overall)
    
    # Show some statistics
    print("\n" + "=" * 80)
    print("STATISTICS")
    print("=" * 80)
    print(f"Total streets with president surnames: {overall['street_count'].sum():,}")
    num_represented = overall.filter(pl.col('street_count') > 0).height
    print(f"Number of presidents represented: {num_represented}")
    
    if num_represented > 0:
        print(f"Most common: {overall.row(0, named=True)['president_surname']} ({overall.row(0, named=True)['street_count']:,} streets)")
        least_common = overall.filter(pl.col('street_count') > 0).tail(1).row(0, named=True)
        print(f"Least common (excluding zeros): {least_common['president_surname']} ({least_common['street_count']:,} streets)")
    else:
        print("No streets with president surnames found in the data.")
    
    # By state analysis
    print("\n" + "=" * 80)
    print("BY STATE ANALYSIS")
    print("=" * 80)
    print("Computing counts by state (this may take a moment)...")
    by_state = analyze_by_state()
    
    # Show top 10 president-state combinations
    print("\nTop 10 President-State combinations:")
    print(by_state.head(10))
    
    # Save results
    output_dir = Path(__file__).parent
    overall.write_csv(output_dir / "president_streets_overall.csv")
    by_state.write_csv(output_dir / "president_streets_by_state.csv")
    
    print("\n" + "=" * 80)
    print(f"Results saved to {output_dir}")
    print("  - president_streets_overall.csv")
    print("  - president_streets_by_state.csv")
    print("=" * 80)

