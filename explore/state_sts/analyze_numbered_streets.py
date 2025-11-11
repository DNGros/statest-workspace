#!/usr/bin/env python3
"""Analyze what fraction of state-named streets contain numbers (e.g., 'Virginia Route 32B')."""

import sys
import re
from pathlib import Path
import polars as pl

# Add parent directory to path to import workspace modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from workspace.load_street_df import load_state_streets_df
from workspace.states import USState

# Import from the same directory
from workspace.explore.state_sts.most_common_state_st import extract_state_names_from_street_name


def has_number(street_name: str) -> bool:
    """
    Check if a street name contains any digits.
    
    Args:
        street_name: The street name to check
        
    Returns:
        True if the street name contains at least one digit, False otherwise
    """
    return bool(re.search(r'\d', street_name))


def analyze_numbered_streets() -> dict:
    """
    Analyze state-named streets to find what fraction contain numbers.
    
    Returns:
        Dictionary with:
            - 'total': Total number of state-named streets
            - 'with_numbers': Number of streets with numbers
            - 'without_numbers': Number of streets without numbers
            - 'fraction_with_numbers': Fraction (0-1) of streets with numbers
            - 'per_state': DataFrame with counts per state name found in street names
    """
    print("Loading state-named streets from all states...")
    # Include numbered streets for this analysis
    lf = load_state_streets_df(exclude_numbered=False)
    
    # Collect to DataFrame for analysis
    print("Analyzing street names...")
    df = lf.collect()
    
    # Check which streets have numbers
    df = df.with_columns(
        pl.col("street_name").map_elements(has_number, return_dtype=pl.Boolean).alias("has_number")
    )
    
    # Total counts
    total = len(df)
    with_numbers = df.filter(pl.col("has_number")).height
    without_numbers = total - with_numbers
    fraction_with_numbers = with_numbers / total if total > 0 else 0.0
    
    # Extract state names from street names for per-state analysis
    print("Extracting state names from street names for per-state analysis...")
    state_names_list = []
    for street_name in df['street_name']:
        found_states = extract_state_names_from_street_name(street_name)
        state_names_list.append(found_states)
    
    # Add state names found in each street
    df = df.with_columns(pl.Series("found_state_names", state_names_list))
    
    # Explode so each state name gets its own row
    df_exploded = df.explode("found_state_names")
    
    # Group by state name and calculate statistics
    per_state = (
        df_exploded
        .group_by("found_state_names")
        .agg([
            pl.len().alias("total_streets"),
            pl.col("has_number").sum().alias("with_numbers"),
            (pl.col("has_number").sum() / pl.len()).alias("fraction_with_numbers")
        ])
        .sort("total_streets", descending=True)
        .rename({"found_state_names": "state_name"})
        .with_columns(
            pl.col("state_name").str.to_titlecase().alias("state_name")
        )
    )
    
    return {
        'total': total,
        'with_numbers': with_numbers,
        'without_numbers': without_numbers,
        'fraction_with_numbers': fraction_with_numbers,
        'per_state': per_state
    }


def print_summary(results: dict):
    """Print a summary of the analysis results."""
    print("\n" + "="*60)
    print("ANALYSIS: State-Named Streets with Numbers")
    print("="*60)
    print(f"\nTotal state-named streets: {results['total']:,}")
    print(f"Streets with numbers: {results['with_numbers']:,}")
    print(f"Streets without numbers: {results['without_numbers']:,}")
    print(f"\nFraction with numbers: {results['fraction_with_numbers']:.2%}")
    print(f"Fraction without numbers: {1 - results['fraction_with_numbers']:.2%}")
    
    print("\n" + "-"*60)
    print("Per-State Breakdown (sorted by total streets):")
    print("-"*60)
    print(results['per_state'])


def save_results(results: dict, output_dir: Path = None):
    """
    Save results to CSV files.
    
    Args:
        results: Results dictionary from analyze_numbered_streets()
        output_dir: Directory to save results. Defaults to main_outputs/state_sts/
    """
    if output_dir is None:
        workspace_root = Path(__file__).parent.parent.parent
        output_dir = workspace_root / "main_outputs" / "state_sts"
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save per-state breakdown
    csv_path = output_dir / "numbered_streets_per_state.csv"
    results['per_state'].write_csv(csv_path)
    print(f"\nSaved per-state breakdown to: {csv_path}")
    
    # Save summary statistics
    summary_path = output_dir / "numbered_streets_summary.txt"
    with open(summary_path, 'w') as f:
        f.write("State-Named Streets with Numbers - Summary\n")
        f.write("="*60 + "\n\n")
        f.write(f"Total state-named streets: {results['total']:,}\n")
        f.write(f"Streets with numbers: {results['with_numbers']:,}\n")
        f.write(f"Streets without numbers: {results['without_numbers']:,}\n")
        f.write(f"\nFraction with numbers: {results['fraction_with_numbers']:.2%}\n")
        f.write(f"Fraction without numbers: {1 - results['fraction_with_numbers']:.2%}\n")
    print(f"Saved summary to: {summary_path}")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Analyze what fraction of state-named streets contain numbers'
    )
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=None,
        help='Output directory for CSV files (default: main_outputs/state_sts/)'
    )
    parser.add_argument(
        '--no-save',
        action='store_true',
        help='Skip saving results to files'
    )
    
    args = parser.parse_args()
    
    # Run analysis
    results = analyze_numbered_streets()
    
    # Print summary
    print_summary(results)
    
    # Save results unless disabled
    if not args.no_save:
        save_results(results, args.output_dir)
    
    return results


if __name__ == "__main__":
    main()

