#!/usr/bin/env python3
"""Plot the most common street names across all US states."""

import sys
from pathlib import Path
import polars as pl
import matplotlib.pyplot as plt

# Add parent directory to path to import workspace modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from workspace.load_street_df import load_street_df
from workspace.plot_utils import (
    create_horizontal_bar_plot,
    save_plot,
    get_output_path_from_script
)


def normalize_street_names_polars(df: pl.DataFrame) -> pl.DataFrame:
    """
    Normalize numbered street names in a Polars DataFrame by converting written numbers to ordinal format.
    
    Uses Polars native string operations for performance. Converts:
    - "First Street" -> "1st Street"
    - "2nd Street" -> "2nd Street" (unchanged)
    - "Second Avenue" -> "2nd Avenue"
    
    Args:
        df: DataFrame with a 'street_name' column
        
    Returns:
        DataFrame with a 'normalized_name' column
    """
    # Mapping of written ordinals/cardinals to numeric ordinals
    # Order matters: longer patterns first to avoid partial matches
    replacements = [
        # Compound numbers (longest first)
        (r'\btwenty-ninth\b', '29th'),
        (r'\btwenty-eighth\b', '28th'),
        (r'\btwenty-seventh\b', '27th'),
        (r'\btwenty-sixth\b', '26th'),
        (r'\btwenty-fifth\b', '25th'),
        (r'\btwenty-fourth\b', '24th'),
        (r'\btwenty-third\b', '23rd'),
        (r'\btwenty-second\b', '22nd'),
        (r'\btwenty-first\b', '21st'),
        (r'\btwentieth\b', '20th'),
        (r'\bnineteenth\b', '19th'),
        (r'\beighteenth\b', '18th'),
        (r'\bseventeenth\b', '17th'),
        (r'\bsixteenth\b', '16th'),
        (r'\bfifteenth\b', '15th'),
        (r'\bfourteenth\b', '14th'),
        (r'\bthirteenth\b', '13th'),
        (r'\btwelfth\b', '12th'),
        (r'\beleventh\b', '11th'),
        (r'\btenth\b', '10th'),
        (r'\bninth\b', '9th'),
        (r'\beighth\b', '8th'),
        (r'\bseventh\b', '7th'),
        (r'\bsixth\b', '6th'),
        (r'\bfifth\b', '5th'),
        (r'\bfourth\b', '4th'),
        (r'\bthird\b', '3rd'),
        (r'\bsecond\b', '2nd'),
        (r'\bfirst\b', '1st'),
    ]
    
    # Apply all replacements sequentially
    # Names that already have ordinals (like "1st Street") won't match any patterns, so they remain unchanged
    normalized = pl.col('street_name')
    for pattern, replacement in replacements:
        normalized = normalized.str.replace(pattern, replacement, literal=False)
    
    return df.with_columns(normalized.alias('normalized_name'))




def plot_most_common_streets(top_n: int = 10, output_path: Path = None):
    """
    Load street data and create a horizontal bar plot of the most common street names.
    
    Args:
        top_n: Number of top street names to display (default: 10)
        output_path: Optional path to save the plot. If None, saves to main_outputs/all_streets/
    """
    print("Loading street data from all states...")
    lf = load_street_df()
    
    # Count occurrences of each street name (all in lazy mode for efficiency)
    print("Counting street name occurrences...")
    name_counts = (
        lf.group_by('street_name')
        .agg(pl.len().alias('count'))
        .sort('count', descending=True)
        .head(top_n)
        .collect()  # Materialize only the top N results
    )
    
    # Create and save plot
    fig, ax, street_names, counts = create_horizontal_bar_plot(
        name_counts,
        value_column='count',
        label_column='street_name',
        xlabel='Number of Streets',
        figsize=(4.5, 4.5)
    )
    
    # Determine output path
    if output_path is None:
        output_path = get_output_path_from_script(Path(__file__), "top_streets.svg")
    
    save_plot(fig, output_path)
    
    # Print the results
    print(f"\nTop {top_n} most common street names:")
    print(name_counts)
    
    return name_counts


def plot_most_common_streets_merged(top_n: int = 10, output_path: Path = None):
    """
    Load street data, merge numbered street variants (e.g., "1st" and "First"), 
    and create a horizontal bar plot of the most common street names.
    
    Args:
        top_n: Number of top street names to display (default: 10)
        output_path: Optional path to save the plot. If None, saves to main_outputs/all_streets/
    """
    print("Loading street data from all states...")
    lf = load_street_df()
    
    # Normalize street names to merge numbered variants
    # Note: normalize_street_names_polars requires a DataFrame, so we collect here
    print("Normalizing numbered street names...")
    df_normalized = normalize_street_names_polars(lf.collect())
    
    # Count occurrences of each normalized street name (back to lazy for efficiency)
    print("Counting normalized street name occurrences...")
    name_counts = (
        df_normalized.lazy()
        .group_by('normalized_name')
        .agg(pl.len().alias('count'))
        .sort('count', descending=True)
        .head(top_n)
        .collect()
    )
    
    # Create and save plot
    fig, ax, street_names, counts = create_horizontal_bar_plot(
        name_counts,
        value_column='count',
        label_column='normalized_name',
        xlabel='Number of Streets',
        figsize=(4.5, 4.5)
    )
    
    # Determine output path
    if output_path is None:
        output_path = get_output_path_from_script(Path(__file__), "top_streets_merged.svg")
    
    save_plot(fig, output_path)
    
    # Print the results
    print(f"\nTop {top_n} most common street names (with numbered variants merged):")
    print(name_counts)
    
    return name_counts


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Plot most common street names')
    parser.add_argument(
        '--top-n',
        type=int,
        default=10,
        help='Number of top street names to display (default: 10)'
    )
    parser.add_argument(
        '--output',
        type=Path,
        default=None,
        help='Output path for the original plot (default: saves to main_outputs/all_streets/top_streets.svg)'
    )
    parser.add_argument(
        '--output-merged',
        type=Path,
        default=None,
        help='Output path for the merged plot (default: saves to main_outputs/all_streets/top_streets_merged.svg)'
    )
    parser.add_argument(
        '--merged-only',
        action='store_true',
        help='Only generate the merged plot (skip the original plot)'
    )
    parser.add_argument(
        '--original-only',
        action='store_true',
        help='Only generate the original plot (skip the merged plot)'
    )
    
    args = parser.parse_args()
    
    # Generate original plot unless --merged-only is specified
    if not args.merged_only:
        plot_most_common_streets(top_n=args.top_n, output_path=args.output)
    
    # Generate merged plot unless --original-only is specified
    if not args.original_only:
        plot_most_common_streets_merged(top_n=args.top_n, output_path=args.output_merged)


if __name__ == "__main__":
    main()

