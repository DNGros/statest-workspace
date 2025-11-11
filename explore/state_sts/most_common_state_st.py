#!/usr/bin/env python3
"""Plot the most common state names found in street names across all US states.
Supports both simple (non-stacked) and stacked (in-state vs out-of-state) visualizations."""

import sys
import re
from pathlib import Path
from typing import Optional, Tuple
import polars as pl
import matplotlib.pyplot as plt

# Add parent directory to path to import workspace modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from workspace.load_street_df import load_state_streets_df, load_street_df
from workspace.states import USState
from workspace.plot_utils import (
    create_horizontal_bar_plot,
    save_plot,
    get_output_path_from_script,
    setup_tufte_style
)


def extract_state_names_from_street_name(street_name: str) -> list[str]:
    """
    Extract state names that appear in a street name, returning only the longest match.
    
    Uses word boundaries to match whole words only (e.g., "Maine" won't match "Jermaine").
    
    When multiple state names are found in a street name, only the longest one is returned.
    This avoids duplicate counting issues when mapping streets (e.g., a street named
    "West Virginia Ave" will only count as "West Virginia", not both "West Virginia" 
    and "Virginia").
    
    For the rare case where a street contains multiple distinct state names 
    (e.g., "New York New Jersey St"), only the longest one is kept. This is a 
    deterministic approach that works well for mapping purposes.
    
    Args:
        street_name: The street name to search
        
    Returns:
        List containing at most one state name (the longest match), or empty list if none found
    """
    street_lower = street_name.lower()
    found_states = []
    
    # Get all state names and separate into multi-word and single-word
    all_state_names = USState.all_names()
    multi_word_states = [s for s in all_state_names if ' ' in s]
    single_word_states = [s for s in all_state_names if ' ' not in s]
    
    # Sort multi-word states by length (longest first) to prioritize longer matches
    multi_word_states.sort(key=len, reverse=True)
    
    # Track character positions that have been consumed by multi-word matches
    # This prevents "Virginia" from matching when it's part of "West Virginia"
    multi_word_matches = []  # Store (state_name, start_pos, end_pos)
    
    # First, find all multi-word state name matches and track their positions
    for state_name in multi_word_states:
        escaped_name = re.escape(state_name)
        pattern = r'\b' + escaped_name + r'\b'
        for match in re.finditer(pattern, street_lower):
            found_states.append(state_name)
            # Track the character positions consumed by this match
            start, end = match.span()
            multi_word_matches.append((state_name, start, end))
    
    # Then check single-word state names
    # Only add if the match doesn't overlap with any multi-word match
    for state_name in single_word_states:
        escaped_name = re.escape(state_name)
        pattern = r'\b' + escaped_name + r'\b'
        for match in re.finditer(pattern, street_lower):
            start, end = match.span()
            # Check if this match overlaps with any consumed positions
            if not any(start < end_pos and end > start_pos 
                      for _, start_pos, end_pos in multi_word_matches):
                found_states.append(state_name)
    
    # Return only the longest state name found (if any)
    if found_states:
        longest_state = max(found_states, key=len)
        return [longest_state]
    else:
        return []


def count_state_names_in_streets(
    lf: pl.LazyFrame,
    top_n: int = 10
) -> pl.DataFrame:
    """
    Count how many times each state name appears in street names (non-stacked version).
    
    For each street, extracts all state names that appear in its name and counts
    occurrences. If a street name contains multiple state names, each is counted
    separately. If the same state name appears in a street name multiple times,
    it's only counted once per street.
    
    Note: Each row in the input represents a unique street (with a 'state' column
    indicating which state the street is physically located in). If "Texas St"
    appears in both California and Texas, these are counted as two separate
    occurrences of "Texas" in street names. This gives us the total number of
    streets named after each state, regardless of where those streets are located.
    
    Args:
        lf: LazyFrame containing state-named streets with 'street_name' column
        top_n: Number of top state names to return
        
    Returns:
        DataFrame with columns: 'state_name' and 'count', sorted by count descending
    """
    print("Extracting state names from street names...")
    
    # Collect to DataFrame for string processing
    df = lf.collect()
    
    # Extract state names for each street
    state_names_list = []
    for street_name in df['street_name']:
        found_states = extract_state_names_from_street_name(street_name)
        state_names_list.append(found_states)
    
    # Add as a new column
    df = df.with_columns(pl.Series("found_state_names", state_names_list))
    
    # Explode the list so each state name gets its own row
    df_exploded = df.explode("found_state_names")
    
    # Group by state name and count occurrences
    state_counts = (
        df_exploded
        .group_by("found_state_names")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
        .head(top_n)
        .rename({"found_state_names": "state_name"})
    )
    
    return state_counts


def count_all_state_names_by_location(
    lf: pl.LazyFrame
) -> pl.DataFrame:
    """
    Count how many times each state name appears in street names, broken down by location.
    Returns ALL state names with ranks, not just top N.
    
    For each street, extracts the state name from its street name and determines whether
    the street is physically located in that state (in-state) or in a different state (out-of-state).
    
    Args:
        lf: LazyFrame containing state-named streets with 'street_name' and 'state' columns
        
    Returns:
        DataFrame with columns: 'state_name', 'in_state', 'out_of_state', 'total', 'rank'
        sorted by total descending, with rank column (1-based)
    """
    print("Extracting state names from street names and categorizing by location...")
    
    # Collect to DataFrame for string processing
    df = lf.collect()
    
    # Extract state names for each street and determine if in-state or out-of-state
    records = []
    for row in df.iter_rows(named=True):
        street_name = row['street_name']
        # Normalize physical state: convert dashes to spaces and lowercase
        # (data stores states as "new-york" but state names are "new york")
        physical_state = row['state'].lower().replace("-", " ")
        
        found_states = extract_state_names_from_street_name(street_name)
        
        # For each state name found in the street name, determine if it's in-state or out-of-state
        for state_name_in_street in found_states:
            is_in_state = (state_name_in_street.lower() == physical_state)
            
            records.append({
                'state_name': state_name_in_street,
                'is_in_state': is_in_state
            })
    
    # Convert to DataFrame
    records_df = pl.DataFrame(records)
    
    # Group by state name and count in-state vs out-of-state
    state_counts = (
        records_df
        .with_columns([
            (~pl.col("is_in_state")).alias("is_out_of_state")
        ])
        .group_by("state_name")
        .agg([
            pl.sum("is_in_state").alias("in_state"),
            pl.sum("is_out_of_state").alias("out_of_state")
        ])
        .with_columns([
            (pl.col("in_state") + pl.col("out_of_state")).alias("total")
        ])
        .sort("total", descending=True)
        .with_row_index("rank")
        .with_columns([
            (pl.col("rank") + 1).alias("rank")  # Convert to 1-based ranking
        ])
    )
    
    return state_counts


def count_state_names_by_location_all_states(
    lf: pl.LazyFrame
) -> pl.DataFrame:
    """
    Count how many times each state name appears in street names, broken down by location.
    Includes ALL states, even if they have zero occurrences.
    
    For each street, extracts the state name from its street name and determines whether
    the street is physically located in that state (in-state) or in a different state (out-of-state).
    
    Args:
        lf: LazyFrame containing state-named streets with 'street_name' and 'state' columns
        
    Returns:
        DataFrame with columns: 'state_name', 'in_state', 'out_of_state', 'total'
        sorted by total descending, but includes all states
    """
    print("Extracting state names from street names and categorizing by location (all states)...")
    
    # Collect to DataFrame for string processing
    df = lf.collect()
    
    # Extract state names for each street and determine if in-state or out-of-state
    records = []
    for row in df.iter_rows(named=True):
        street_name = row['street_name']
        # Normalize physical state: convert dashes to spaces and lowercase
        physical_state = row['state'].lower().replace("-", " ")
        
        found_states = extract_state_names_from_street_name(street_name)
        
        # For each state name found in the street name, determine if it's in-state or out-of-state
        for state_name_in_street in found_states:
            is_in_state = (state_name_in_street.lower() == physical_state)
            
            records.append({
                'state_name': state_name_in_street,
                'is_in_state': is_in_state
            })
    
    # Convert to DataFrame
    records_df = pl.DataFrame(records)
    
    # Group by state name and count in-state vs out-of-state
    state_counts = (
        records_df
        .with_columns([
            (~pl.col("is_in_state")).alias("is_out_of_state")
        ])
        .group_by("state_name")
        .agg([
            pl.sum("is_in_state").alias("in_state"),
            pl.sum("is_out_of_state").alias("out_of_state")
        ])
        .with_columns([
            (pl.col("in_state") + pl.col("out_of_state")).alias("total")
        ])
        .sort("total", descending=True)
    )
    
    # Ensure all states are included (even if they have zero counts)
    all_state_names = USState.all_names()
    existing_state_names = set(state_counts['state_name'].to_list())
    existing_state_names_lower = {name.lower() for name in existing_state_names}
    
    # Add missing states with zero counts
    missing_states = []
    for state_name in all_state_names:
        if state_name.lower() not in existing_state_names_lower:
            missing_states.append({
                'state_name': state_name,
                'in_state': 0,
                'out_of_state': 0,
                'total': 0
            })
    
    if missing_states:
        missing_df = pl.DataFrame(missing_states)
        # Get the schema from state_counts to ensure exact match
        schema = state_counts.schema
        # Cast missing_df columns to match the schema
        missing_df = missing_df.with_columns([
            pl.col("in_state").cast(schema["in_state"]),
            pl.col("out_of_state").cast(schema["out_of_state"]),
            pl.col("total").cast(schema["total"])
        ])
        state_counts = pl.concat([state_counts, missing_df])
    
    # Re-sort by total descending
    state_counts = state_counts.sort("total", descending=True)
    
    return state_counts


def draw_axis_break(ax, y_break_start: float, y_break_end: float, 
                    x_pos: float = 0, break_width: float = 0.15):
    """
    Add ellipsis indicator for axis break (zigzag removed).
    
    Args:
        ax: Matplotlib axes
        y_break_start: Y position where break starts (bottom of gap)
        y_break_end: Y position where break ends (top of gap)
        x_pos: X position for the break markers (default: 0, at left spine)
        break_width: Width parameter (kept for compatibility, not used)
    """
    # Add ellipsis text centered in the gap to indicate omitted data
    # Position it just inside the plot area (at x=0.01 in axes coordinates) 
    # so it doesn't extend outside and affect bbox calculations
    gap_center_y = (y_break_start + y_break_end) / 2
    ax.text(
        0.01,  # Position just inside the plot area (axes coordinates)
        gap_center_y,
        '...',
        va='center',
        ha='left',
        fontsize=12,
        color='#999999',
        clip_on=True,  # Clip to plot area
        transform=ax.get_yaxis_transform(),  # Use y-axis transform for x=0.01, y in data coords
        zorder=10
    )


def create_stacked_horizontal_bar_plot(
    data_df: pl.DataFrame,
    bottom_column: str,
    top_column: str,
    label_column: str,
    bottom_label: str,
    top_label: str,
    figsize: tuple[float, float] = (4.5, 4.5),
    xlabel: str = 'Number of Streets',
    ylabel: str = '',
    show_value_labels: bool = True,
    reverse_order: bool = True,
    bottom_n: int = 0,
    rank_column: str = None,
    break_gap: float = 1.0,
    bar_height: float = 0.7
) -> tuple[plt.Figure, plt.Axes]:
    """
    Create a stacked horizontal bar plot with consistent Tufte styling.
    Supports both simple mode (no break) and break mode (with axis break).
    
    Args:
        data_df: Polars DataFrame with data to plot
        bottom_column: Column name for bottom stack values
        top_column: Column name for top stack values
        label_column: Column name for bar labels (y-axis)
        bottom_label: Label for bottom stack (for legend)
        top_label: Label for top stack (for legend)
        figsize: Figure size as (width, height) in inches
        xlabel: Label for x-axis
        ylabel: Label for y-axis
        show_value_labels: Whether to show numeric labels on bars
        reverse_order: Whether to reverse order (highest at top)
        bottom_n: Number of bottom items to show (0 = simple mode, >0 = break mode)
        rank_column: Column name for rank numbers (only used when bottom_n > 0)
        break_gap: Gap size in axis units between top and bottom sections (only used when bottom_n > 0)
        bar_height: Height of each bar (default: 0.7)
        
    Returns:
        Tuple of (figure, axes) for further customization
    """
    # Set up style
    setup_tufte_style()
    
    # Color scheme: warm yellow-gold for in-state, cool green-teal for out-of-state
    bottom_color = '#E3B778'  # Warm yellow-gold for in-state
    top_color = '#91C9A8'     # Cool green-teal for out-of-state
    
    # Convert to list for easier indexing
    data_list = data_df.to_dicts()
    
    # Determine if we're in break mode
    use_break_mode = bottom_n > 0
    
    if use_break_mode:
        # Break mode: data comes in as [top N items, bottom N items] sorted descending
        # Extract data
        labels = [item[label_column] for item in data_list]
        ranks = [item[rank_column] for item in data_list]
        bottom_values = [item[bottom_column] for item in data_list]
        top_values = [item[top_column] for item in data_list]
        
        # Format labels with rank numbers
        display_labels = [f"{rank}. {label}" for rank, label in zip(ranks, labels)]
        
        # Create non-contiguous y-positions
        top_n = len(data_list) - bottom_n
        top_positions = list(range(top_n))
        bottom_start = int(top_n + break_gap)
        bottom_positions = list(range(bottom_start, bottom_start + bottom_n))
        y_positions = top_positions + bottom_positions
    else:
        # Simple mode: just top N items
        if reverse_order:
            data_list.reverse()  # Highest at top
        
        # Extract data
        labels = [item[label_column] for item in data_list]
        bottom_values = [item[bottom_column] for item in data_list]
        top_values = [item[top_column] for item in data_list]
        display_labels = labels
        
        # Create contiguous y-positions
        y_positions = range(len(labels))
    
    # Create figure with transparent background for SVG
    fig, ax = plt.subplots(figsize=figsize, facecolor='none')
    fig.patch.set_facecolor('none')
    ax.set_facecolor('none')
    
    # Create stacked horizontal bar plot
    # Bottom stack (in-state)
    bars_bottom = ax.barh(
        y_positions,
        bottom_values,
        height=bar_height,
        color=bottom_color,
        edgecolor='none',
        linewidth=0,
        label=bottom_label
    )
    
    # Top stack (out-of-state) - starts where bottom ends
    bars_top = ax.barh(
        y_positions,
        top_values,
        height=bar_height,
        left=bottom_values,  # Stack on top of bottom values
        color=top_color,
        edgecolor='none',
        linewidth=0,
        label=top_label
    )
    
    # Set y-axis labels
    ax.set_yticks(y_positions)
    ax.set_yticklabels(display_labels)
    
    if use_break_mode:
        # Draw axis break markers
        top_n = len(data_list) - bottom_n
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
        total_values = [b + t for b, t in zip(bottom_values, top_values)]
        max_value = max(total_values) if total_values else 0
        
        # Threshold for small bars - when total is less than this percentage of max, shift labels further right
        small_bar_threshold = max_value * 0.03
        
        for i, (y_pos, bottom_val, top_val) in enumerate(zip(y_positions, bottom_values, top_values)):
            total_val = bottom_val + top_val
            
            # Detect if this is a small bar that needs extra spacing
            is_small_bar = total_val < small_bar_threshold
            
            # Determine offsets based on bar size
            if is_small_bar:
                # For small bars, use larger offsets to prevent overlap
                total_label_offset = max(max_value * 0.07, total_val * 0.35)
                pct_label_offset = max(max_value * 0.02, total_val * 0.1)
            else:
                # Standard offsets for normal bars
                total_label_offset = max_value * 0.015
                pct_label_offset = max_value * 0.005
            
            # Label for total at the end of the bar
            if total_val > 0:  # Only show labels for non-zero values
                ax.text(
                    total_val + total_label_offset,
                    y_pos,
                    f"{int(total_val):,}",
                    va='center',
                    ha='left',
                    fontsize=10 if not use_break_mode else 10,
                    color='#111111'
                )
                
                # Add percentage label - positioned just to the right of the in-state bar
                # Show percentage even when 0% (bottom_val == 0) for break mode, otherwise only if > 0
                if total_val > 0:
                    in_state_pct = (bottom_val / total_val) * 100
                    # Position just to the right of the in-state bar (at the boundary)
                    # When bottom_val is 0, position at the start with offset
                    x_position = bottom_val + pct_label_offset
                    # Use smaller font for very small bars
                    font_size = 7 if bottom_val < max_value * 0.05 else 8
                    # Only show percentage if bottom_val > 0 or in break mode
                    if bottom_val > 0 or use_break_mode:
                        ax.text(
                            x_position,
                            y_pos,
                            f"{in_state_pct:.0f}%",
                            va='center',
                            ha='left',
                            fontsize=font_size,
                            color='#333333',
                            weight='normal'
                        )
        
        # Extra space for labels - increase padding for small bars
        if max_value > 0:
            # Check if we have any small bars that need extra space
            has_small_bars = any(tv < small_bar_threshold for tv in total_values)
            padding_multiplier = 1.30 if has_small_bars else 1.15
            ax.set_xlim(left=0, right=max_value * padding_multiplier)
    else:
        ax.set_xlim(left=0)
    
    # Clean up ticks
    ax.tick_params(axis='x', length=4, width=0.5, colors='#333333')
    ax.tick_params(axis='y', length=0, width=0, colors='#333333', pad=8)
    
    plt.tight_layout(pad=0.1)
    
    return fig, ax


def calculate_figure_height(num_items: int, min_height: float = 12.0, 
                           inches_per_item: float = 0.25, 
                           extra_space: float = 2.0) -> float:
    """
    Calculate figure height based on number of items to display.
    
    Args:
        num_items: Number of items to display
        min_height: Minimum height in inches
        inches_per_item: Height per item in inches
        extra_space: Extra space for legend and labels
        
    Returns:
        Calculated height in inches
    """
    return max(min_height, num_items * inches_per_item + extra_space)


def calculate_in_state_percentage(lf: pl.LazyFrame) -> pl.DataFrame:
    """
    Calculate the percentage of streets with each state name that are actually in that state.
    
    Args:
        lf: LazyFrame containing state-named streets with 'street_name' and 'state' columns
        
    Returns:
        DataFrame with columns: 'state_name', 'in_state', 'total', 'percentage', 'rank'
        sorted by percentage descending, with rank column (1-based)
    """
    print("Calculating in-state percentage for each state name...")
    
    # Get counts by location
    state_counts = count_all_state_names_by_location(lf)
    
    # Calculate percentage
    result = (
        state_counts
        .drop("rank")  # Drop old rank since we're re-ranking by percentage
        .with_columns([
            (pl.col("in_state") / pl.col("total") * 100).alias("percentage")
        ])
        .filter(pl.col("total") > 0)  # Only include states with at least one occurrence
        .sort("percentage", descending=True)
        .with_row_index("rank")
        .with_columns([
            (pl.col("rank") + 1).alias("rank")  # Convert to 1-based ranking
        ])
    )
    
    return result


def get_total_streets_per_state() -> pl.DataFrame:
    """
    Get total street counts per state from all streets data.
    
    Returns:
        DataFrame with columns: 'state' and 'total_streets'
    """
    print("Loading all streets to get total counts per state...")
    
    # Load all streets (not just state-named)
    lf = load_street_df()
    
    # Count streets per state
    total_counts = (
        lf
        .group_by("state")
        .agg(pl.len().alias("total_streets"))
        .collect()
    )
    
    return total_counts


def calculate_state_fraction_of_all_streets() -> pl.DataFrame:
    """
    Calculate what fraction of all streets in each state are named after that state.
    
    Returns:
        DataFrame with columns: 'state_name', 'streets_named_after_state', 'total_streets', 
        'percentage', 'rank' sorted by percentage descending, with rank column (1-based)
    """
    print("Calculating fraction of all streets named after each state...")
    
    # Get total streets per state
    total_streets_df = get_total_streets_per_state()
    
    # Load state-named streets
    state_streets_lf = load_state_streets_df()
    
    # Count how many streets named after each state are in that state
    # Collect for string processing
    df = state_streets_lf.collect()
    
    records = []
    for row in df.iter_rows(named=True):
        street_name = row['street_name']
        # Normalize physical state: convert dashes to spaces and lowercase
        physical_state = row['state'].lower().replace("-", " ")
        
        found_states = extract_state_names_from_street_name(street_name)
        
        # For each state name found in the street name, check if it's in-state
        for state_name_in_street in found_states:
            is_in_state = (state_name_in_street.lower() == physical_state)
            
            if is_in_state:
                records.append({
                    'state_name': state_name_in_street,
                    'physical_state': physical_state
                })
    
    # Count streets named after each state that are in that state
    state_named_counts = (
        pl.DataFrame(records)
        .group_by("state_name")
        .agg(pl.len().alias("streets_named_after_state"))
    )
    
    # Merge with total street counts
    # Need to match state names to state identifiers in total_streets_df
    # total_streets_df has 'state' column with values like "new-york", "california"
    # state_named_counts has 'state_name' with values like "new york", "california"
    
    # Create a mapping DataFrame from state name to state identifier
    state_mapping_records = []
    for state_name in USState.all_names():
        state_id = state_name.lower().replace(" ", "-")
        state_mapping_records.append({
            'state_name_lower': state_name.lower(),
            'state_id': state_id
        })
    state_mapping_df = pl.DataFrame(state_mapping_records)
    
    # Add state identifier column to state_named_counts by joining with mapping
    state_named_counts = (
        state_named_counts
        .with_columns([
            pl.col("state_name").str.to_lowercase().alias("state_name_lower")
        ])
        .join(
            state_mapping_df,
            on="state_name_lower",
            how="left"
        )
    )
    
    # Merge with total_streets_df to include all states
    # This ensures states with zero streets named after themselves are included
    # First, join total_streets_df with state_mapping_df to get state_name for all states
    # Then join with state_named_counts to get counts (left join keeps all states)
    result = (
        total_streets_df
        .join(
            state_mapping_df,
            left_on="state",
            right_on="state_id",
            how="left"
        )
        .join(
            state_named_counts.select(["state_id", "streets_named_after_state"]),
            left_on="state",  # Use "state" from total_streets_df which matches "state_id"
            right_on="state_id",
            how="left"
        )
        .with_columns([
            pl.col("streets_named_after_state").fill_null(0),  # Fill nulls with 0 for states with no matches
            # Convert state_name_lower to title case, with fallback to deriving from state if null
            pl.coalesce([
                pl.col("state_name_lower").str.to_titlecase(),
                pl.col("state").str.replace("-", " ").str.to_titlecase()
            ]).alias("state_name")
        ])
        .with_columns([
            (pl.col("streets_named_after_state") / pl.col("total_streets") * 100).alias("percentage")
        ])
        .select([
            "state_name",
            "streets_named_after_state",
            "total_streets",
            "percentage"
        ])
        .sort("percentage", descending=True)
        .with_row_index("rank")
        .with_columns([
            (pl.col("rank") + 1).alias("rank")  # Convert to 1-based ranking
        ])
    )
    
    return result


def create_percentage_bar_plot(
    data_df: pl.DataFrame,
    value_column: str,
    label_column: str,
    numerator_column: str,
    denominator_column: str,
    figsize: tuple[float, float] = (4.5, 4.5),
    xlabel: str = 'Percentage (%)',
    ylabel: str = '',
    top_n: int = 7,
    bottom_n: int = 3,
    rank_column: str = None,
    break_gap: float = 1.0,
    bar_height: float = 0.7,
    bar_color: str = '#5A9B8E',
    xlim_max: Optional[float] = None
) -> tuple[plt.Figure, plt.Axes]:
    """
    Create a horizontal bar plot showing percentages with break mode (top N + bottom N).
    
    Args:
        data_df: Polars DataFrame with data to plot (must be sorted by value_column descending)
        value_column: Column name for bar values (percentage)
        label_column: Column name for bar labels (y-axis)
        numerator_column: Column name for numerator in annotation (X in "X / Y (Z%)")
        denominator_column: Column name for denominator in annotation (Y in "X / Y (Z%)")
        figsize: Figure size as (width, height) in inches
        xlabel: Label for x-axis
        ylabel: Label for y-axis
        top_n: Number of top items to show
        bottom_n: Number of bottom items to show
        rank_column: Column name for rank numbers
        break_gap: Gap size in axis units between top and bottom sections
        bar_height: Height of each bar (default: 0.7)
        bar_color: Color for bars (default: muted blue-teal)
        xlim_max: Optional maximum x-axis limit. If None, uses max_value * 1.15
        
    Returns:
        Tuple of (figure, axes) for further customization
    """
    # Set up style
    setup_tufte_style()
    
    # Select top N and bottom N
    top_items = data_df.head(top_n)
    bottom_items = data_df.tail(bottom_n)
    plot_data = pl.concat([top_items, bottom_items])
    
    # Convert to list for easier indexing
    data_list = plot_data.to_dicts()
    
    # Extract data
    labels = [item[label_column] for item in data_list]
    values = [item[value_column] for item in data_list]
    numerators = [item[numerator_column] for item in data_list]
    denominators = [item[denominator_column] for item in data_list]
    
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
    
    # Create figure with transparent background for SVG
    fig, ax = plt.subplots(figsize=figsize, facecolor='none')
    fig.patch.set_facecolor('none')
    ax.set_facecolor('none')
    
    # Create horizontal bar plot
    bars = ax.barh(
        y_positions,
        values,
        height=bar_height,
        color=bar_color,
        edgecolor='none',
        linewidth=0
    )
    
    # Set y-axis labels
    ax.set_yticks(y_positions)
    ax.set_yticklabels(display_labels)
    
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
    
    # Very subtle grid only on x-axis
    ax.grid(True, axis='x', linestyle='-', linewidth=0.5, alpha=0.2, color='#cccccc')
    ax.set_axisbelow(True)
    
    # Labels
    ax.set_xlabel(xlabel, labelpad=8)
    ax.set_ylabel(ylabel)
    
    # Add annotations: "X / Y (Z%)"
    max_value = max(values) if values else 0
    
    for i, (y_pos, value, num, denom) in enumerate(zip(y_positions, values, numerators, denominators)):
        # Format annotation
        annotation = f"{int(num):,} / {int(denom):,} ({value:.1f}%)"
        
        # Position annotation at end of bar with padding
        label_offset = max_value * 0.02 if max_value > 0 else value * 0.1
        ax.text(
            value + label_offset,
            y_pos,
            annotation,
            va='center',
            ha='left',
            fontsize=10,
            color='#111111'
        )
    
    # Set x-axis limits with padding for labels
    if max_value > 0:
        if xlim_max is not None:
            # Use specified maximum, but ensure it's at least as large as max_value
            xlim_right = max(xlim_max, max_value * 1.15)
        else:
            xlim_right = max_value * 1.15
        ax.set_xlim(left=0, right=xlim_right)
    else:
        ax.set_xlim(left=0)
    
    # Clean up ticks
    ax.tick_params(axis='x', length=4, width=0.5, colors='#333333')
    ax.tick_params(axis='y', length=0, width=0, colors='#333333', pad=8)
    
    # Use tight_layout to prevent cutoff while maintaining figure size
    plt.tight_layout(pad=1.5)
    
    return fig, ax


def plot_state_names(
    stacked: bool = False,
    top_n: int = 10,
    bottom_n: int = 0,
    width: Optional[float] = None,
    height: Optional[float] = None,
    output_path: Optional[Path] = None
):
    """
    Load state-named streets and create a plot of the most common state names.
    
    Args:
        stacked: If True, show stacked bars (in-state vs out-of-state). If False, show simple bars.
        top_n: Number of top state names to display. If >= 50, shows all states (stacked only).
        bottom_n: Number of bottom state names to display (only used when stacked=True and top_n < 50).
                  If > 0, creates break mode showing top N + bottom N with axis break.
        width: Figure width in inches. If None, uses defaults (4.5 for simple, 5.5 for stacked).
        height: Figure height in inches. If None, auto-calculates based on number of items.
        output_path: Optional path to save the plot. If None, saves to main_outputs/state_sts/
    """
    print("Loading state-named streets from all states...")
    lf = load_state_streets_df()
    
    if not stacked:
        # Non-stacked mode: simple bar chart
        print("Counting state name occurrences...")
        state_counts = count_state_names_in_streets(lf, top_n=top_n)
        
        # Capitalize state names for display (title case)
        state_counts = state_counts.with_columns(
            pl.col("state_name").str.to_titlecase().alias("state_name")
        )
        
        # Determine figure size
        if width is None:
            width = 4.5
        if height is None:
            height = calculate_figure_height(len(state_counts), min_height=4.5, 
                                            inches_per_item=0.25, extra_space=2.0)
        
        # Create and save plot using shared utilities
        fig, ax, state_names, counts = create_horizontal_bar_plot(
            state_counts,
            value_column='count',
            label_column='state_name',
            xlabel='Number of Streets',
            figsize=(width, height)
        )
        
        # Determine output path
        if output_path is None:
            output_path = get_output_path_from_script(
                Path(__file__), 
                "most_common_state_names.svg"
            )
        
        save_plot(fig, output_path)
        
        # Print the results
        print(f"\nTop {top_n} most common state names in street names:")
        print(state_counts)
        
        return state_counts
    
    else:
        # Stacked mode: in-state vs out-of-state breakdown
        show_all = top_n >= 50
        
        if show_all:
            # Show all states mode
            print("Counting state name occurrences by location (all states)...")
            state_counts = count_state_names_by_location_all_states(lf)
            
            # Capitalize state names for display (title case)
            state_counts = state_counts.with_columns(
                pl.col("state_name").str.to_titlecase().alias("state_name")
            )
            
            num_states = len(state_counts)
            
            # Determine figure size
            if width is None:
                width = 6.0  # Wider to accommodate legend
            if height is None:
                height = calculate_figure_height(num_states, min_height=12.0, 
                                                inches_per_item=0.25, extra_space=2.0)
            
            # Create and save plot
            fig, ax = create_stacked_horizontal_bar_plot(
                state_counts,
                bottom_column='in_state',
                top_column='out_of_state',
                label_column='state_name',
                bottom_label='In-State',
                top_label='Out-of-State',
                xlabel='Number of Streets',
                figsize=(width, height),
                bottom_n=0,  # No break mode for all states
                bar_height=0.6  # Slightly smaller bars for better spacing
            )
            
            # Determine output path
            if output_path is None:
                output_path = get_output_path_from_script(
                    Path(__file__), 
                    "most_common_state_names_stacked_all.svg"
                )
            
            save_plot(fig, output_path)
            
            # Print summary statistics
            print(f"\nAll {num_states} states in street names (in-state vs out-of-state):")
            print(f"States with zero occurrences: {len(state_counts.filter(pl.col('total') == 0))}")
            print(f"States with at least one occurrence: {len(state_counts.filter(pl.col('total') > 0))}")
            
            return state_counts
        
        else:
            # Top N mode (with optional break mode)
            print("Counting state name occurrences by location (all states)...")
            all_state_counts = count_all_state_names_by_location(lf)
            
            # Capitalize state names for display (title case)
            all_state_counts = all_state_counts.with_columns(
                pl.col("state_name").str.to_titlecase().alias("state_name")
            )
            
            # Determine mode and prepare data
            use_break_mode = bottom_n > 0
            
            if use_break_mode:
                # Break mode: select top N and bottom N
                top_states = all_state_counts.head(top_n)
                bottom_states = all_state_counts.tail(bottom_n)
                plot_data = pl.concat([top_states, bottom_states])
                num_items = top_n + bottom_n
            else:
                # Simple mode: just top N
                plot_data = all_state_counts.head(top_n)
                num_items = top_n
            
            # Determine figure size
            if width is None:
                width = 5.5  # Slightly wider to accommodate legend
            if height is None:
                height = calculate_figure_height(num_items, min_height=4.5, 
                                                inches_per_item=0.25, extra_space=2.0)
            
            # Create and save plot
            fig, ax = create_stacked_horizontal_bar_plot(
                plot_data,
                bottom_column='in_state',
                top_column='out_of_state',
                label_column='state_name',
                bottom_label='In-State',
                top_label='Out-of-State',
                xlabel='Number of Streets',
                figsize=(width, height),
                bottom_n=bottom_n,
                rank_column='rank' if use_break_mode else None
            )
            
            # Determine output path
            if output_path is None:
                if use_break_mode:
                    filename = "most_common_state_names_stacked_with_break.svg"
                else:
                    filename = "most_common_state_names_stacked.svg"
                output_path = get_output_path_from_script(
                    Path(__file__), 
                    filename
                )
            
            save_plot(fig, output_path)
            
            # Print the results
            if use_break_mode:
                print(f"\nTop {top_n} most common state names:")
                print(all_state_counts.head(top_n))
                print(f"\nBottom {bottom_n} state names:")
                print(all_state_counts.tail(bottom_n))
            else:
                print(f"\nTop {top_n} most common state names in street names (in-state vs out-of-state):")
                print(plot_data)
            
            return plot_data


def plot_in_state_percentage(
    top_n: int = 7,
    bottom_n: int = 3,
    width: Optional[float] = None,
    height: Optional[float] = None,
    output_path: Optional[Path] = None
):
    """
    Plot top N and bottom N states ranked by % of streets with that state name that are in-state.
    
    Args:
        top_n: Number of top states to show (default: 7)
        bottom_n: Number of bottom states to show (default: 3)
        width: Figure width in inches. If None, uses default (4.5)
        height: Figure height in inches. If None, auto-calculates based on number of items
        output_path: Optional path to save the plot. If None, saves to main_outputs/state_sts/
    """
    print("Loading state-named streets from all states...")
    lf = load_state_streets_df()
    
    # Calculate percentages
    percentage_df = calculate_in_state_percentage(lf)
    
    # Capitalize state names for display (title case)
    percentage_df = percentage_df.with_columns(
        pl.col("state_name").str.to_titlecase().alias("state_name")
    )
    
    # Determine figure size
    num_items = top_n + bottom_n
    if width is None:
        width = 4.5
    if height is None:
        height = calculate_figure_height(num_items, min_height=4.5, 
                                        inches_per_item=0.25, extra_space=2.0)
    
    # Create and save plot
    fig, ax = create_percentage_bar_plot(
        percentage_df,
        value_column='percentage',
        label_column='state_name',
        numerator_column='in_state',
        denominator_column='total',
        figsize=(width, height),
        xlabel='Percentage In-State (%)',
        top_n=top_n,
        bottom_n=bottom_n,
        rank_column='rank',
        bar_color='#E3B778'  # Warm yellow-gold - matches in-state color from stacked plot
    )
    
    # Determine output path
    if output_path is None:
        output_path = get_output_path_from_script(
            Path(__file__), 
            "in_state_percentage_break.svg"
        )
    
    save_plot(fig, output_path)
    
    # Print the results
    print(f"\nTop {top_n} states by in-state percentage:")
    print(percentage_df.head(top_n))
    print(f"\nBottom {bottom_n} states by in-state percentage:")
    print(percentage_df.tail(bottom_n))
    
    return percentage_df


def plot_state_fraction_of_all_streets(
    top_n: int = 7,
    bottom_n: int = 3,
    width: Optional[float] = None,
    height: Optional[float] = None,
    output_path: Optional[Path] = None
):
    """
    Plot top N and bottom N states ranked by fraction of all streets named after that state.
    
    Args:
        top_n: Number of top states to show (default: 7)
        bottom_n: Number of bottom states to show (default: 3)
        width: Figure width in inches. If None, uses default (4.5)
        height: Figure height in inches. If None, auto-calculates based on number of items
        output_path: Optional path to save the plot. If None, saves to main_outputs/state_sts/
    """
    # Calculate fractions
    fraction_df = calculate_state_fraction_of_all_streets()
    
    # Capitalize state names for display (title case)
    fraction_df = fraction_df.with_columns(
        pl.col("state_name").str.to_titlecase().alias("state_name")
    )
    
    # Determine figure size
    num_items = top_n + bottom_n
    if width is None:
        width = 4.5
    if height is None:
        height = calculate_figure_height(num_items, min_height=4.5, 
                                        inches_per_item=0.25, extra_space=2.0)
    
    # Create and save plot
    fig, ax = create_percentage_bar_plot(
        fraction_df,
        value_column='percentage',
        label_column='state_name',
        numerator_column='streets_named_after_state',
        denominator_column='total_streets',
        figsize=(width, height),
        xlabel='Percent of All Streets Any Name',
        top_n=top_n,
        bottom_n=bottom_n,
        rank_column='rank',
        bar_color='#f280bf',
    )
    
    # Determine output path
    if output_path is None:
        output_path = get_output_path_from_script(
            Path(__file__), 
            "state_fraction_of_all_streets_break.svg"
        )
    
    save_plot(fig, output_path)
    
    # Print the results
    print(f"\nTop {top_n} states by fraction of all streets named after state:")
    print(fraction_df.head(top_n))
    print(f"\nBottom {bottom_n} states by fraction of all streets named after state:")
    print(fraction_df.tail(bottom_n))
    
    return fraction_df


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Plot most common state names in street names'
    )
    parser.add_argument(
        '--stacked',
        action='store_true',
        help='Show stacked bars (in-state vs out-of-state) instead of simple bars'
    )
    parser.add_argument(
        '--in-state-percentage',
        action='store_true',
        help='Plot in-state percentage (top 7 and bottom 3)'
    )
    parser.add_argument(
        '--state-fraction',
        action='store_true',
        help='Plot fraction of all streets named after state (top 7 and bottom 3)'
    )
    parser.add_argument(
        '--top-n',
        type=int,
        default=10,
        help='Number of top state names to display (default: 10). If >= 50 and --stacked, shows all states.'
    )
    parser.add_argument(
        '--bottom-n',
        type=int,
        default=0,
        help='Number of bottom state names to display (default: 0, use >0 for break mode with --stacked)'
    )
    parser.add_argument(
        '--width',
        type=float,
        default=None,
        help='Figure width in inches (default: auto, 4.5 for simple, 5.5 for stacked)'
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
    
    # Determine which plot to generate
    if args.in_state_percentage:
        plot_in_state_percentage(
            top_n=args.top_n if args.top_n != 10 else 7,
            bottom_n=args.bottom_n if args.bottom_n != 0 else 3,
            width=args.width,
            height=args.height,
            output_path=args.output
        )
    elif args.state_fraction:
        plot_state_fraction_of_all_streets(
            top_n=args.top_n if args.top_n != 10 else 7,
            bottom_n=args.bottom_n if args.bottom_n != 0 else 3,
            width=args.width,
            height=args.height,
            output_path=args.output
        )
    else:
        # Default: original plot_state_names behavior
        plot_state_names(
            stacked=args.stacked,
            top_n=args.top_n,
            bottom_n=args.bottom_n,
            width=args.width,
            height=args.height,
            output_path=args.output
        )


if __name__ == "__main__":
    main()
