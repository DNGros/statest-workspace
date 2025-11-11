#!/usr/bin/env python3
"""Generate a combined table with three ego/humble metrics and their average rank.

Combines:
1. In-state percentage: Percentage of streets with a state name that are in that state
2. State fraction: Fraction of all streets in a state that are named after that state
3. Self vs other: Fraction of state-named streets in a state that are self-named

Ranks each state for each metric (1 = best/highest), then averages the ranks.
Sorts by average rank (lower = more egotistical).
"""

import sys
from pathlib import Path
from typing import Optional
import polars as pl

# Add parent directory to path to import workspace modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from workspace.load_street_df import load_state_streets_df
from workspace.explore.state_sts.most_common_state_st import (
    calculate_in_state_percentage,
    calculate_state_fraction_of_all_streets
)
from workspace.explore.state_sts.self_vs_other_named import calculate_self_vs_other_named
from workspace.plot_utils import get_output_path_from_script


def escape_html(text):
    """Escape HTML special characters."""
    if not isinstance(text, str):
        text = str(text)
    text = text.replace('&', '&amp;')
    text = text.replace('<', '&lt;')
    text = text.replace('>', '&gt;')
    text = text.replace('"', '&quot;')
    text = text.replace("'", '&#x27;')
    return text


def generate_html_table(rows, table_class=""):
    """Generate a simple HTML table from list of dictionaries."""
    html = []
    
    if not rows:
        return ""
    
    # Get column names from first row
    columns = list(rows[0].keys())
    
    # Table element
    table_classes = f' class="{table_class}"' if table_class else ''
    html.append(f'<table{table_classes}>')
    
    # Header
    html.append('  <thead>')
    html.append('    <tr>')
    for col in columns:
        html.append(f'      <th>{escape_html(col)}</th>')
    html.append('    </tr>')
    html.append('  </thead>')
    
    # Body
    html.append('  <tbody>')
    for row in rows:
        html.append('    <tr>')
        for col in columns:
            html.append(f'      <td>{escape_html(str(row.get(col, "")))}</td>')
        html.append('    </tr>')
    html.append('  </tbody>')
    
    html.append('</table>')
    
    return '\n'.join(html)


def generate_combined_metrics_table(output_path: Optional[Path] = None) -> pl.DataFrame:
    """
    Generate a combined table with all three metrics and their average.
    
    Args:
        output_path: Optional path to save HTML table. If None, saves to main_outputs/state_sts/
        
    Returns:
        DataFrame with combined metrics, sorted by average rank (ascending, lower = more egotistical)
    """
    print("Loading state-named streets...")
    lf = load_state_streets_df()
    
    print("Calculating in-state percentage...")
    in_state_pct_df = calculate_in_state_percentage(lf)
    
    print("Calculating state fraction of all streets...")
    state_fraction_df = calculate_state_fraction_of_all_streets()
    
    print("Calculating self vs other named...")
    self_vs_other_df = calculate_self_vs_other_named(lf)
    
    # Normalize state names to lowercase for joining
    in_state_pct_df = in_state_pct_df.with_columns(
        pl.col("state_name").str.to_lowercase().alias("state_name_lower")
    ).select(["state_name_lower", "percentage"]).rename({"percentage": "in_state_pct"})
    
    state_fraction_df = state_fraction_df.with_columns(
        pl.col("state_name").str.to_lowercase().alias("state_name_lower")
    ).select(["state_name_lower", "percentage"]).rename({"percentage": "state_fraction_pct"})
    
    self_vs_other_df = self_vs_other_df.with_columns(
        pl.col("state_name").str.to_lowercase().alias("state_name_lower")
    ).select(["state_name_lower", "fraction"]).rename({"fraction": "self_named_fraction"})
    
    # Join all three metrics (using full outer join to include all states)
    # Join first two
    temp = in_state_pct_df.join(state_fraction_df, on="state_name_lower", how="full")
    # Then join with third, selecting only the columns we want to avoid duplicates
    combined = (
        temp.select(["state_name_lower", "in_state_pct", "state_fraction_pct"])
        .join(
            self_vs_other_df.select(["state_name_lower", "self_named_fraction"]),
            on="state_name_lower",
            how="full"
        )
    )
    
    # Fill nulls with 0 (for states that don't appear in one of the metrics)
    combined = combined.with_columns([
        pl.col("in_state_pct").fill_null(0),
        pl.col("state_fraction_pct").fill_null(0),
        pl.col("self_named_fraction").fill_null(0)
    ])
    
    # Filter out rows with null or empty state names
    combined = combined.filter(pl.col("state_name_lower").is_not_null() & (pl.col("state_name_lower") != ""))
    
    # Calculate ranks for each metric (1 = best/highest, higher number = worse)
    # For all metrics: higher is better, so rank descending
    combined = combined.with_columns([
        pl.col("in_state_pct").rank(descending=True, method="average").alias("rank_in_state"),
        pl.col("state_fraction_pct").rank(descending=True, method="average").alias("rank_state_fraction"),
        pl.col("self_named_fraction").rank(descending=True, method="average").alias("rank_self_named")
    ])
    
    # Calculate average rank (lower is better, so we'll sort ascending)
    combined = combined.with_columns([
        ((pl.col("rank_in_state") + pl.col("rank_state_fraction") + pl.col("rank_self_named")) / 3).alias("avg_rank")
    ])
    
    # Sort by average rank ascending (lower rank = more egotistical)
    combined = combined.sort("avg_rank", descending=False)
    
    # Add title case state name for display
    combined = combined.with_columns(
        pl.col("state_name_lower").str.to_titlecase().alias("state_name")
    )
    
    # Select final columns in display order
    result = combined.select([
        "state_name",
        "in_state_pct",
        "state_fraction_pct",
        "self_named_fraction",
        "rank_in_state",
        "rank_state_fraction",
        "rank_self_named",
        "avg_rank"
    ])
    
    # Generate HTML table with ranks and base values in parentheses
    rows = []
    for row in result.iter_rows(named=True):
        rows.append({
            "State": row["state_name"],
            "In-State %": f"{int(row['rank_in_state'])} ({row['in_state_pct']:.1f}%)",
            "State Fraction %": f"{int(row['rank_state_fraction'])} ({row['state_fraction_pct']:.3f}%)",
            "Self-Named Fraction": f"{int(row['rank_self_named'])} ({row['self_named_fraction'] * 100:.1f}%)",
            "Avg Rank": f"{row['avg_rank']:.1f}"
        })
    
    html_table = generate_html_table(rows, table_class="combined-metrics-table")
    
    # Wrap table in a scrollable container div
    html_table = f'<div class="table-scroll-wrapper">{html_table}</div>'
    
    # Determine output path
    if output_path is None:
        output_path = get_output_path_from_script(
            Path(__file__),
            "combined_metrics_table.html"
        )
    
    # Save HTML table
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        f.write(html_table)
    
    print(f"\nHTML table saved to: {output_path}")
    print(f"\nTop 10 states by average rank (lower = more egotistical):")
    print(result.head(10))
    
    return result


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Generate combined table with three ego/humble metrics'
    )
    parser.add_argument(
        '--output',
        type=Path,
        default=None,
        help='Output path for HTML table (default: saves to main_outputs/state_sts/)'
    )
    
    args = parser.parse_args()
    
    generate_combined_metrics_table(output_path=args.output)


if __name__ == "__main__":
    main()

