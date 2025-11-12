#!/usr/bin/env python3
"""Create a compact version of the most common streets plot for social media images."""

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


def plot_social_streets(output_path: Path = None):
    """
    Create a compact horizontal bar plot of the top 3 most common street names
    for use in social media images.
    
    Args:
        output_path: Optional path to save the plot. If None, saves to main_outputs/social/
    """
    print("Loading street data from all states...")
    lf = load_street_df()
    
    # Count occurrences of each street name (all in lazy mode for efficiency)
    print("Counting street name occurrences...")
    name_counts = (
        lf.group_by('street_name')
        .agg(pl.len().alias('count'))
        .sort('count', descending=True)
        .head(3)  # Only top 3 for social image
        .collect()
    )
    
    # Create plot with 2in x 2in size
    fig, ax, street_names, counts = create_horizontal_bar_plot(
        name_counts,
        value_column='count',
        label_column='street_name',
        xlabel='Number of Streets',
        figsize=(2, 2)  # 2in x 2in for social media
    )
    
    # Adjust font sizes for smaller plot
    ax.tick_params(labelsize=8)
    ax.set_xlabel('Number of Streets', fontsize=9)
    
    # Determine output path
    if output_path is None:
        workspace_dir = Path(__file__).parent.parent
        output_dir = workspace_dir / "main_outputs" / "social"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "top_3_streets.svg"
    
    # Save with transparent background
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, format='svg', 
                facecolor='none', edgecolor='none', transparent=True)
    print(f"Saved plot to {output_path}")
    
    # Print the results
    print(f"\nTop 3 most common street names:")
    print(name_counts)
    
    return name_counts


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Create compact plot of top 3 street names for social media'
    )
    parser.add_argument(
        '--output',
        type=Path,
        default=None,
        help='Output path for the plot (default: saves to main_outputs/social/top_3_streets.svg)'
    )
    
    args = parser.parse_args()
    
    plot_social_streets(output_path=args.output)


if __name__ == "__main__":
    main()


