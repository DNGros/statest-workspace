#!/usr/bin/env python3
"""Plot the most common state names found in street names across all US states."""

import sys
import re
from pathlib import Path
import polars as pl
import matplotlib.pyplot as plt

# Add parent directory to path to import workspace modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from workspace.load_street_df import load_state_streets_df
from workspace.states import USState
from workspace.plot_utils import (
    create_horizontal_bar_plot,
    save_plot,
    get_output_path_from_script
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
    Count how many times each state name appears in street names.
    
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
    # We need to process each street name to extract state names
    df = lf.collect()
    
    # Extract state names for each street
    # This creates a list of state names found in each street name
    state_names_list = []
    for street_name in df['street_name']:
        found_states = extract_state_names_from_street_name(street_name)
        state_names_list.append(found_states)
    
    # Add as a new column
    df = df.with_columns(pl.Series("found_state_names", state_names_list))
    
    # Explode the list so each state name gets its own row
    # This way, if "New York New Jersey St" exists, it creates two rows
    df_exploded = df.explode("found_state_names")
    
    # Group by state name and count occurrences
    # Each row represents one street containing that state name
    state_counts = (
        df_exploded
        .group_by("found_state_names")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
        .head(top_n)
        .rename({"found_state_names": "state_name"})
    )
    
    return state_counts


def plot_most_common_state_names(top_n: int = 10, output_path: Path = None):
    """
    Load state-named streets and create a horizontal bar plot of the most common state names.
    
    Args:
        top_n: Number of top state names to display (default: 10)
        output_path: Optional path to save the plot. If None, saves to main_outputs/state_sts/
    """
    print("Loading state-named streets from all states...")
    lf = load_state_streets_df()
    
    # Count occurrences of each state name in street names
    print("Counting state name occurrences...")
    state_counts = count_state_names_in_streets(lf, top_n=top_n)
    
    # Capitalize state names for display (title case)
    state_counts = state_counts.with_columns(
        pl.col("state_name").str.to_titlecase().alias("state_name")
    )
    
    # Create and save plot using shared utilities
    fig, ax, state_names, counts = create_horizontal_bar_plot(
        state_counts,
        value_column='count',
        label_column='state_name',
        xlabel='Number of Streets',
        figsize=(4.5, 4.5)
    )
    
    # Determine output path
    if output_path is None:
        output_path = get_output_path_from_script(Path(__file__), "most_common_state_names.svg")
    
    save_plot(fig, output_path)
    
    # Print the results
    print(f"\nTop {top_n} most common state names in street names:")
    print(state_counts)
    
    return state_counts


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Plot most common state names in street names')
    parser.add_argument(
        '--top-n',
        type=int,
        default=10,
        help='Number of top state names to display (default: 10)'
    )
    parser.add_argument(
        '--output',
        type=Path,
        default=None,
        help='Output path for the plot (default: saves to main_outputs/state_sts/most_common_state_names.svg)'
    )
    
    args = parser.parse_args()
    
    plot_most_common_state_names(top_n=args.top_n, output_path=args.output)


if __name__ == "__main__":
    main()

