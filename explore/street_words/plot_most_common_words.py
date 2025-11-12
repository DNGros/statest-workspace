#!/usr/bin/env python3
"""Plot the most common words in street names across all US states."""

import sys
from pathlib import Path
import polars as pl
import matplotlib.pyplot as plt

# Add parent directory to path to import workspace modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
from workspace.load_street_df import load_street_df
from workspace.plot_utils import (
    create_horizontal_bar_plot,
    save_plot,
    get_output_path_from_script,
    get_color_palette
)


def plot_most_common_words(top_n: int = 8, output_path: Path = None):
    """
    Load street data and create a horizontal bar plot of the most common words in street names.
    
    Args:
        top_n: Number of top words to display (default: 8)
        output_path: Optional path to save the plot. If None, saves to main_outputs/street_words/
    """
    print("Loading street data from all states...")
    lf = load_street_df()
    
    # Split street names into words and count occurrences (all in lazy mode for efficiency)
    print("Splitting street names into words and counting occurrences...")
    word_counts = (
        lf
        .select("street_name")
        .with_columns(
            # Split street names by whitespace into a list of words
            pl.col("street_name").str.split(" ").alias("words")
        )
        .explode("words")  # Expand each word into its own row
        .filter(
            # Filter out empty strings and common street type suffixes
            (pl.col("words") != "") &
            (pl.col("words").str.len_chars() > 0)
        )
        .with_columns(
            # Convert to lowercase for consistent counting
            pl.col("words").str.to_lowercase().alias("word")
        )
        .group_by("word")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
        .head(top_n)
        .collect()  # Materialize only the top N results
    )
    
    # Create and save plot using shared utilities
    # Use accent color (muted coral) to distinguish from full street name plot
    accent_color = get_color_palette()['accent']
    
    fig, ax, words, counts = create_horizontal_bar_plot(
        word_counts,
        value_column='count',
        label_column='word',
        xlabel='Word Occurrences',
        figsize=(3, 3),
        bar_color=accent_color
    )
    
    # Determine output path
    if output_path is None:
        workspace_dir = Path(__file__).parent.parent.parent
        output_dir = workspace_dir / "main_outputs" / "street_words"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "top_words.svg"
    
    # Save with transparent background (no bbox_inches='tight' to maintain aspect ratio)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, format='svg', 
                facecolor='none', edgecolor='none', transparent=True)
    print(f"Saved plot to {output_path}")
    
    # Print the results
    print(f"\nTop {top_n} most common words in street names:")
    print(word_counts)
    
    return word_counts


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Plot most common words in street names')
    parser.add_argument(
        '--top-n',
        type=int,
        default=8,
        help='Number of top words to display (default: 8)'
    )
    parser.add_argument(
        '--output',
        type=Path,
        default=None,
        help='Output path for the plot (default: saves to main_outputs/street_words/top_words.svg)'
    )
    
    args = parser.parse_args()
    
    plot_most_common_words(top_n=args.top_n, output_path=args.output)


if __name__ == "__main__":
    main()

