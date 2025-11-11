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
    
    # Create and save plot using shared utilities
    fig, ax, street_names, counts = create_horizontal_bar_plot(
        name_counts,
        value_column='count',
        label_column='street_name',
        xlabel='Number of Streets',
        figsize=(3, 3)
    )
    
    # Determine output path
    if output_path is None:
        # For this script directly in explore/, we need to handle the path differently
        workspace_dir = Path(__file__).parent.parent
        output_dir = workspace_dir / "main_outputs" / "all_streets"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "top_streets.svg"
    
    # Save with transparent background (no bbox_inches='tight' to maintain aspect ratio)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, format='svg', 
                facecolor='none', edgecolor='none', transparent=True)
    print(f"Saved plot to {output_path}")
    
    # Print the results
    print(f"\nTop {top_n} most common street names:")
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
        help='Output path for the plot (default: saves to main_outputs/all_streets/top_streets.svg)'
    )
    
    args = parser.parse_args()
    
    plot_most_common_streets(top_n=args.top_n, output_path=args.output)


if __name__ == "__main__":
    main()

