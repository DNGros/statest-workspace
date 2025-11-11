#!/usr/bin/env python3
"""Plot the distribution of highway/street types across all US states."""

import sys
from pathlib import Path
import polars as pl
import matplotlib.pyplot as plt
import matplotlib

# Add parent directory to path to import workspace modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from workspace.load_street_df import load_street_df


def get_default_output_dir() -> Path:
    """
    Get the default output directory based on the script's location in explore/.
    
    Maps explore/subdir/script.py -> main_outputs/subdir/
    """
    script_path = Path(__file__)
    # Get the explore directory (parent of parent if in explore/subdir/)
    explore_dir = script_path.parent.parent
    workspace_dir = explore_dir.parent
    
    # Get the subdirectory name (e.g., 'street_types')
    subdir_name = script_path.parent.name
    
    # Create corresponding main_outputs directory
    output_dir = workspace_dir / "main_outputs" / subdir_name
    output_dir.mkdir(parents=True, exist_ok=True)
    
    return output_dir


def plot_street_types(top_n: int = 15, output_path: Path = None):
    """
    Load street data and create a horizontal bar plot of highway/street types.
    
    Args:
        top_n: Number of top street types to display (default: 15)
        output_path: Optional path to save the plot. If None, saves to main_outputs/street_types/
    """
    print("Loading street data from all states...")
    lf = load_street_df()
    
    # Count occurrences of each highway type (all in lazy mode for efficiency)
    print("Counting highway type occurrences...")
    type_counts = (
        lf.group_by('highway_type')
        .agg(pl.len().alias('count'))
        .sort('count', descending=True)
        .head(top_n)
        .collect()  # Materialize only the top N results
    )
    
    # Convert to list for easier indexing (reverse for top-to-bottom display)
    type_counts_list = type_counts.to_dicts()
    type_counts_list.reverse()  # Reverse so highest is at top
    
    # Set up Tufte-style plot
    matplotlib.rcParams['font.family'] = 'serif'
    matplotlib.rcParams['font.serif'] = ['Palatino', 'Palatino Linotype', 'Book Antiqua', 'Georgia', 'serif']
    matplotlib.rcParams['font.size'] = 11
    matplotlib.rcParams['axes.labelsize'] = 11
    matplotlib.rcParams['axes.titlesize'] = 12
    matplotlib.rcParams['xtick.labelsize'] = 10
    matplotlib.rcParams['ytick.labelsize'] = 10
    matplotlib.rcParams['axes.linewidth'] = 0.5
    matplotlib.rcParams['axes.edgecolor'] = '#333333'
    matplotlib.rcParams['axes.labelcolor'] = '#111111'
    matplotlib.rcParams['text.color'] = '#111111'
    matplotlib.rcParams['xtick.color'] = '#333333'
    matplotlib.rcParams['ytick.color'] = '#333333'
    matplotlib.rcParams['grid.color'] = '#cccccc'
    matplotlib.rcParams['grid.linewidth'] = 0.5
    matplotlib.rcParams['grid.alpha'] = 0.3
    
    # Create figure with transparent background
    fig, ax = plt.subplots(figsize=(4, 4), facecolor='none')
    fig.patch.set_facecolor('none')
    fig.patch.set_alpha(0.0)
    ax.set_facecolor('none')
    ax.patch.set_alpha(0.0)
    
    # Extract data
    highway_types = [item['highway_type'] for item in type_counts_list]
    counts = [item['count'] for item in type_counts_list]
    
    # Create horizontal bar plot with subtle color
    # Use a muted blue-teal for a clean, professional look
    bar_color = '#5A9B8E'  # Muted blue-teal - pretty but not flashy
    bars = ax.barh(range(len(highway_types)), counts, height=0.7, color=bar_color, 
                   edgecolor='none', linewidth=0, alpha=0.85)
    
    # Set y-axis labels
    ax.set_yticks(range(len(highway_types)))
    ax.set_yticklabels(highway_types)
    
    # Clean axis styling - minimal chartjunk
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#cccccc')
    ax.spines['bottom'].set_color('#cccccc')
    ax.spines['left'].set_linewidth(0.5)
    ax.spines['bottom'].set_linewidth(0.5)
    
    # Very subtle grid only on x-axis
    ax.grid(True, axis='x', linestyle='-', linewidth=0.5, alpha=0.2, color='#cccccc')
    ax.set_axisbelow(True)
    
    # Labels
    ax.set_xlabel('Number of Streets', labelpad=8)
    ax.set_ylabel('')  # No y-label needed for horizontal bars
    
    # Add value labels on bars (clean, readable)
    max_count = max(counts)
    for i, count in enumerate(counts):
        # Position label at end of bar with small padding
        ax.text(count + max_count * 0.015, i, 
                f"{int(count):,}", 
                va='center', ha='left',
                fontsize=10, color='#111111')
    
    # Clean up x-axis ticks
    ax.tick_params(axis='x', length=4, width=0.5, colors='#333333')
    ax.tick_params(axis='y', length=0, width=0, colors='#333333', pad=8)
    
    # Set x-axis to start at 0 for clean baseline
    ax.set_xlim(left=0, right=max_count * 1.15)  # Extra space for labels
    
    plt.tight_layout()
    
    # Determine output path - save to workspace/main_outputs
    if output_path is None:
        output_dir = get_default_output_dir()
        output_path = output_dir / "street_types.svg"
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Save as SVG with transparent background
    plt.savefig(output_path, format='svg', 
                facecolor='none', edgecolor='none', transparent=True)
    print(f"Saved plot to {output_path}")
    
    # Print the results
    print(f"\nTop {top_n} most common highway types:")
    print(type_counts)
    
    return type_counts


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Plot distribution of street types')
    parser.add_argument(
        '--top-n',
        type=int,
        default=15,
        help='Number of top street types to display (default: 15)'
    )
    parser.add_argument(
        '--output',
        type=Path,
        default=None,
        help='Output path for the plot (default: saves to main_outputs/street_types/street_types.svg)'
    )
    
    args = parser.parse_args()
    
    plot_street_types(top_n=args.top_n, output_path=args.output)


if __name__ == "__main__":
    main()

