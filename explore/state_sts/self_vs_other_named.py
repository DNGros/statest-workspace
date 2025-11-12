#!/usr/bin/env python3
"""Plot self-named vs other-named streets for each physical state.

For each state, counts:
- Self-named: Streets in that state that are named after that state
- Other-named: Streets in that state that are named after other states
"""

import sys
from pathlib import Path
from typing import Optional
import polars as pl
import matplotlib.pyplot as plt
import numpy as np

# Add parent directory to path to import workspace modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from workspace.load_street_df import load_state_streets_df
from workspace.explore.state_sts.most_common_state_st import extract_state_names_from_street_name
from workspace.plot_utils import (
    setup_tufte_style,
    save_plot,
    get_output_path_from_script,
)
from workspace.states import USState


def calculate_self_vs_other_named(lf: pl.LazyFrame) -> pl.DataFrame:
    """
    Calculate self-named vs other-named streets for each physical state.
    
    For each physical state:
    - Self-named: Streets in that state that are named after that state
    - Other-named: Streets in that state that are named after other states
    
    Args:
        lf: LazyFrame containing state-named streets with 'street_name' and 'state' columns
        
    Returns:
        DataFrame with columns: 'state_name', 'self_named', 'other_named', 'total', 'fraction'
        sorted by fraction descending
    """
    print("Calculating self-named vs other-named streets per physical state...")
    
    # Collect to DataFrame for string processing
    df = lf.collect()
    
    # Process each street
    records = []
    for row in df.iter_rows(named=True):
        street_name = row['street_name']
        # Normalize physical state: convert dashes to spaces and lowercase
        physical_state = row['state'].lower().replace("-", " ")
        
        # Extract state names from street name
        found_states = extract_state_names_from_street_name(street_name)
        
        if not found_states:
            # Shouldn't happen if we're using load_state_streets_df, but handle it
            continue
        
        # Check if any extracted state name matches the physical state
        is_self_named = any(
            state_name.lower() == physical_state 
            for state_name in found_states
        )
        
        records.append({
            'physical_state': physical_state,
            'is_self_named': is_self_named
        })
    
    # Convert to DataFrame
    records_df = pl.DataFrame(records)
    
    # Group by physical state and count
    state_counts = (
        records_df
        .with_columns([
            (~pl.col("is_self_named")).alias("is_other_named")
        ])
        .group_by("physical_state")
        .agg([
            pl.sum("is_self_named").alias("self_named"),
            pl.sum("is_other_named").alias("other_named")
        ])
        .with_columns([
            (pl.col("self_named") + pl.col("other_named")).alias("total")
        ])
        .with_columns([
            (pl.col("self_named") / pl.col("total")).alias("fraction")
        ])
        .sort("fraction", descending=True)
        .rename({"physical_state": "state_name"})
    )
    
    return state_counts


def create_grouped_horizontal_bar_plot(
    data_df: pl.DataFrame,
    self_column: str,
    other_column: str,
    label_column: str,
    figsize: tuple[float, float] = (5.5, 4.5),
    xlabel: str = 'Number of Streets',
    ylabel: str = '',
    show_value_labels: bool = True,
    reverse_order: bool = True,
    bar_width: float = 0.35,
    bar_spacing: float = 0.1,
    top_n: int = 0,
    bottom_n: int = 0,
    rank_column: str = None,
    break_gap: float = 1.0
) -> tuple[plt.Figure, plt.Axes]:
    """
    Create a grouped horizontal bar plot with side-by-side bars.
    Supports both simple mode (no break) and break mode (with axis break).
    
    Args:
        data_df: Polars DataFrame with data to plot (must be sorted by fraction descending for break mode)
        self_column: Column name for self-named values
        other_column: Column name for other-named values
        label_column: Column name for bar labels (y-axis)
        figsize: Figure size as (width, height) in inches
        xlabel: Label for x-axis
        ylabel: Label for y-axis
        show_value_labels: Whether to show numeric labels on bars
        reverse_order: Whether to reverse order (highest at top)
        bar_width: Width of each bar (default: 0.35)
        bar_spacing: Spacing between grouped bars (default: 0.1)
        top_n: Number of top items to show (0 = simple mode, >0 = break mode)
        bottom_n: Number of bottom items to show (only used when top_n > 0)
        rank_column: Column name for rank numbers (only used when top_n > 0)
        break_gap: Gap size in axis units between top and bottom sections (only used when top_n > 0)
        
    Returns:
        Tuple of (figure, axes) for further customization
    """
    # Set up style
    setup_tufte_style()
    
    # Import draw_axis_break from most_common_state_st
    from workspace.explore.state_sts.most_common_state_st import draw_axis_break
    
    # Color scheme: warm yellow-gold for self-named, cool green-teal for other-named
    self_color = '#E3B778'  # Warm yellow-gold for self-named
    other_color = '#91C9A8'  # Cool green-teal for other-named
    
    # Determine if we're in break mode
    use_break_mode = top_n > 0 and bottom_n > 0
    
    if use_break_mode:
        # Break mode: select top N and bottom N
        top_items = data_df.head(top_n)
        bottom_items = data_df.tail(bottom_n)
        plot_data = pl.concat([top_items, bottom_items])
        data_list = plot_data.to_dicts()
        
        # Extract data
        labels = [item[label_column] for item in data_list]
        self_values = [item[self_column] for item in data_list]
        other_values = [item[other_column] for item in data_list]
        
        if rank_column:
            ranks = [item[rank_column] for item in data_list]
            display_labels = [f"{rank}. {label}" for rank, label in zip(ranks, labels)]
        else:
            display_labels = labels
        
        # Create non-contiguous y-positions
        top_positions = list(range(top_n))
        bottom_start = int(top_n + break_gap)
        bottom_positions = list(range(bottom_start, bottom_start + bottom_n))
        y_positions = top_positions + bottom_positions
        num_items = len(y_positions)
    else:
        # Simple mode: use all data
        data_list = data_df.to_dicts()
        if reverse_order:
            data_list.reverse()  # Highest at top
        
        # Extract data
        labels = [item[label_column] for item in data_list]
        self_values = [item[self_column] for item in data_list]
        other_values = [item[other_column] for item in data_list]
        display_labels = labels
        
        # Create contiguous y-positions
        num_items = len(labels)
        y_positions = np.arange(num_items)
    
    # Create figure with transparent background for SVG
    fig, ax = plt.subplots(figsize=figsize, facecolor='none')
    fig.patch.set_facecolor('none')
    ax.set_facecolor('none')
    
    # Offset positions for side-by-side bars
    self_positions = np.array(y_positions) - bar_width / 2 - bar_spacing / 2
    other_positions = np.array(y_positions) + bar_width / 2 + bar_spacing / 2
    
    # Create grouped horizontal bar plot
    bars_self = ax.barh(
        self_positions,
        self_values,
        height=bar_width,
        color=self_color,
        edgecolor='none',
        linewidth=0,
        label='Self-Named'
    )
    
    bars_other = ax.barh(
        other_positions,
        other_values,
        height=bar_width,
        color=other_color,
        edgecolor='none',
        linewidth=0,
        label='Other-Named'
    )
    
    # Set y-axis labels at center of each group
    ax.set_yticks(y_positions)
    ax.set_yticklabels(display_labels)
    
    # Handle axis styling based on mode
    if use_break_mode:
        # Draw axis break markers
        break_y_start = top_n - 0.5
        break_y_end = bottom_start - 0.5
        draw_axis_break(ax, break_y_start, break_y_end, x_pos=0, break_width=0.3)
        
        # Clean axis styling - minimal chartjunk
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['bottom'].set_color('#cccccc')
        ax.spines['bottom'].set_linewidth(0.5)
        
        # Hide the left spine (we'll draw it manually)
        ax.spines['left'].set_visible(False)
        
        # Set y-axis limits and invert so highest values (rank 1) appear at top
        ax.set_ylim(bottom=-0.5, top=bottom_start + bottom_n - 0.5)
        ax.invert_yaxis()
        
        # Draw left spine in two parts (above and below break)
        ylim = ax.get_ylim()
        # Top part of spine (above break) - from break to top of chart
        ax.plot([0, 0], [break_y_end, ylim[0]], color='#cccccc', linewidth=0.5, 
                transform=ax.get_yaxis_transform(), clip_on=False)
        # Bottom part of spine (below break) - from bottom of chart to break
        ax.plot([0, 0], [ylim[1], break_y_start], color='#cccccc', linewidth=0.5, 
                transform=ax.get_yaxis_transform(), clip_on=False)
    else:
        # Simple mode: standard axis styling
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
    ax.set_xlabel(xlabel, labelpad=8)
    ax.set_ylabel(ylabel)
    
    # Add legend
    ax.legend(
        loc='lower right',
        frameon=False,
        fontsize=10,
        handlelength=1.0,
        handletextpad=0.5
    )
    
    # Add value labels on bars if requested
    if show_value_labels:
        max_value = max(max(self_values), max(other_values)) if self_values and other_values else 0
        
        for i, (self_val, other_val) in enumerate(zip(self_values, other_values)):
            total_val = self_val + other_val
            
            # Label for self-named bar
            if self_val > 0:
                ax.text(
                    self_val + max_value * 0.015,
                    self_positions[i],
                    f"{int(self_val):,}",
                    va='center',
                    ha='left',
                    fontsize=9,
                    color='#111111'
                )
            
            # Label for other-named bar
            if other_val > 0:
                ax.text(
                    other_val + max_value * 0.015,
                    other_positions[i],
                    f"{int(other_val):,}",
                    va='center',
                    ha='left',
                    fontsize=9,
                    color='#111111'
                )
            
            # Add percentage annotation after both bars
            if total_val > 0:
                self_pct = (self_val / total_val) * 100
                # Position after the rightmost bar (other-named) with more spacing
                x_position = other_val + max_value * 0.10
                ax.text(
                    x_position,
                    y_positions[i],
                    f"({self_pct:.0f}%)",
                    va='center',
                    ha='left',
                    fontsize=9,
                    color='#666666',
                    style='italic'
                )
        
        # Extra space for labels and percentage annotations
        if max_value > 0:
            ax.set_xlim(left=0, right=max_value * 1.30)
    else:
        ax.set_xlim(left=0)
    
    # Set y-axis limits to accommodate grouped bars (only if not in break mode)
    if not use_break_mode:
        ax.set_ylim(bottom=-0.5, top=num_items - 0.5)
    
    # Clean up ticks
    ax.tick_params(axis='x', length=4, width=0.5, colors='#333333')
    ax.tick_params(axis='y', length=0, width=0, colors='#333333', pad=8)
    
    plt.tight_layout(pad=0.1)
    
    return fig, ax


def plot_self_vs_other_named(
    top_n: int = 7,
    bottom_n: int = 3,
    width: Optional[float] = None,
    height: Optional[float] = None,
    output_path: Optional[Path] = None
) -> pl.DataFrame:
    """
    Plot self-named vs other-named streets for top N and bottom N states.
    
    For each physical state, shows:
    - Self-named: Streets in that state that are named after that state
    - Other-named: Streets in that state that are named after other states
    
    States are sorted by fraction of self-named streets (self_named / total).
    
    Args:
        top_n: Number of top states to display (default: 7)
        bottom_n: Number of bottom states to display (default: 3). If 0, shows only top_n.
        width: Figure width in inches. If None, uses default (4.5)
        height: Figure height in inches. If None, auto-calculates based on number of items
        output_path: Optional path to save the plot. If None, saves to main_outputs/state_sts/
        
    Returns:
        DataFrame with the plotted data
    """
    print("Loading state-named streets from all states...")
    lf = load_state_streets_df()
    
    # Calculate self vs other named
    state_counts = calculate_self_vs_other_named(lf)
    
    # Add rank column
    state_counts = state_counts.with_row_index("rank").with_columns([
        (pl.col("rank") + 1).alias("rank")  # Convert to 1-based ranking
    ])
    
    # Capitalize state names for display (title case) - do this before passing to plotting function
    state_counts = state_counts.with_columns(
        pl.col("state_name").str.to_titlecase().alias("state_name")
    )
    
    # Determine mode
    use_break_mode = bottom_n > 0
    
    if use_break_mode:
        # Break mode: select top N and bottom N
        plot_data = pl.concat([state_counts.head(top_n), state_counts.tail(bottom_n)])
    else:
        # Simple mode: just top N
        plot_data = state_counts.head(top_n)
    
    # Determine figure size
    num_items = top_n + bottom_n if use_break_mode else top_n
    if width is None:
        width = 4.5
    if height is None:
        # Calculate height based on number of items
        from workspace.explore.state_sts.most_common_state_st import calculate_figure_height
        height = calculate_figure_height(
            num_items, 
            min_height=4.5, 
            inches_per_item=0.3,  # Slightly more space for grouped bars
            extra_space=2.0
        )
    
    # Create and save plot
    fig, ax = create_grouped_horizontal_bar_plot(
        state_counts if use_break_mode else plot_data,  # Pass full data for break mode selection
        self_column='self_named',
        other_column='other_named',
        label_column='state_name',
        xlabel='Number of Streets',
        figsize=(width, height),
        top_n=top_n if use_break_mode else 0,
        bottom_n=bottom_n if use_break_mode else 0,
        rank_column='rank' if use_break_mode else None
    )
    
    # Determine output path
    if output_path is None:
        if use_break_mode:
            filename = "self_vs_other_named_break.svg"
        else:
            filename = "self_vs_other_named.svg"
        output_path = get_output_path_from_script(
            Path(__file__), 
            filename
        )
    
    save_plot(fig, output_path)
    
    # Print the results
    if use_break_mode:
        print(f"\nTop {top_n} states by fraction of self-named streets:")
        print(state_counts.head(top_n))
        print(f"\nBottom {bottom_n} states by fraction of self-named streets:")
        print(state_counts.tail(bottom_n))
    else:
        print(f"\nTop {top_n} states by fraction of self-named streets:")
        print(plot_data)
    
    return plot_data


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Plot self-named vs other-named streets for each physical state'
    )
    parser.add_argument(
        '--top-n',
        type=int,
        default=7,
        help='Number of top states to display (default: 7)'
    )
    parser.add_argument(
        '--bottom-n',
        type=int,
        default=3,
        help='Number of bottom states to display (default: 3, use 0 for simple mode)'
    )
    parser.add_argument(
        '--width',
        type=float,
        default=None,
        help='Figure width in inches (default: auto, 4.5)'
    )
    parser.add_argument(
        '--height',
        type=float,
        default=None,
        help='Figure height in inches (default: auto-calculated based on number of items)'
    )
    parser.add_argument(
        '--output',
        type=Path,
        default=None,
        help='Output path for the plot (default: saves to main_outputs/state_sts/)'
    )
    
    args = parser.parse_args()
    
    plot_self_vs_other_named(
        top_n=args.top_n,
        bottom_n=args.bottom_n,
        width=args.width,
        height=args.height,
        output_path=args.output
    )


if __name__ == "__main__":
    main()

