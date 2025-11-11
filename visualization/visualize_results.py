#!/usr/bin/env python3
"""Create visualizations from the all-states analysis."""

import sys
from pathlib import Path
import polars as pl
import matplotlib.pyplot as plt
import seaborn as sns


def plot_state_name_popularity(csv_path: Path, output_dir: Path):
    """Create bar chart of state name popularity."""
    df = pl.read_csv(csv_path)
    
    # Plot top 15 and bottom 15
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    
    # Top 15
    top_15 = df.head(15).to_pandas()
    sns.barplot(data=top_15, y='state_name', x='street_count', ax=ax1, palette='viridis')
    ax1.set_xlabel('Number of Streets Named After This State', fontsize=12)
    ax1.set_ylabel('State Name', fontsize=12)
    ax1.set_title('Top 15 Most Popular State Names in Streets', fontsize=14, fontweight='bold')
    
    # Bottom 15
    bottom_15 = df.tail(15).to_pandas()
    sns.barplot(data=bottom_15, y='state_name', x='street_count', ax=ax2, palette='magma')
    ax2.set_xlabel('Number of Streets Named After This State', fontsize=12)
    ax2.set_ylabel('State Name', fontsize=12)
    ax2.set_title('Bottom 15 Least Popular State Names in Streets', fontsize=14, fontweight='bold')
    
    plt.tight_layout()
    output_path = output_dir / 'state_name_popularity.png'
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    print(f"Saved state name popularity chart to {output_path}")
    plt.close()


def plot_ego_humility(csv_path: Path, output_dir: Path):
    """Create visualizations of state ego vs humility."""
    df = pl.read_csv(csv_path)
    
    # Filter out states with very low counts for cleaner visualization
    df_filtered = df.filter(pl.col('other_state_streets') > 50)
    
    # Plot 1: Top 10 most egotistical states
    fig, ax = plt.subplots(figsize=(12, 8))
    top_10 = df_filtered.sort('ego_score', descending=True).head(10).to_pandas()
    
    colors = ['#d62728' if score > 0.5 else '#ff7f0e' if score > 0.3 else '#2ca02c' 
              for score in top_10['ego_score']]
    
    bars = ax.barh(top_10['state'], top_10['ego_score'], color=colors)
    ax.set_xlabel('Ego Score (Self-Named / Other-State-Named)', fontsize=12)
    ax.set_ylabel('State', fontsize=12)
    ax.set_title('Top 10 Most "Egotistical" States\n(High ratio of self-named to other-state-named streets)', 
                 fontsize=14, fontweight='bold')
    ax.invert_yaxis()
    
    # Add value labels
    for i, (bar, val) in enumerate(zip(bars, top_10['ego_score'])):
        ax.text(val + 0.02, bar.get_y() + bar.get_height()/2, 
                f'{val:.2f}', va='center', fontsize=10)
    
    plt.tight_layout()
    output_path = output_dir / 'top_egotistical_states.png'
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    print(f"Saved egotistical states chart to {output_path}")
    plt.close()
    
    # Plot 2: Top 10 most humble states
    fig, ax = plt.subplots(figsize=(12, 8))
    bottom_10 = df_filtered.sort('ego_score').head(10).to_pandas()
    
    colors = ['#2ca02c' if score < 0.15 else '#ff7f0e' if score < 0.25 else '#d62728' 
              for score in bottom_10['ego_score']]
    
    bars = ax.barh(bottom_10['state'], bottom_10['ego_score'], color=colors)
    ax.set_xlabel('Ego Score (Self-Named / Other-State-Named)', fontsize=12)
    ax.set_ylabel('State', fontsize=12)
    ax.set_title('Top 10 Most "Humble" States\n(Low ratio of self-named to other-state-named streets)', 
                 fontsize=14, fontweight='bold')
    ax.invert_yaxis()
    
    # Add value labels
    for i, (bar, val) in enumerate(zip(bars, bottom_10['ego_score'])):
        ax.text(val + 0.002, bar.get_y() + bar.get_height()/2, 
                f'{val:.3f}', va='center', fontsize=10)
    
    plt.tight_layout()
    output_path = output_dir / 'top_humble_states.png'
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    print(f"Saved humble states chart to {output_path}")
    plt.close()
    
    # Plot 3: Scatter plot of self vs other naming
    fig, ax = plt.subplots(figsize=(12, 10))
    df_plot = df_filtered.to_pandas()
    
    scatter = ax.scatter(df_plot['other_pct'], df_plot['self_pct'], 
                        s=100, alpha=0.6, c=df_plot['ego_score'],
                        cmap='RdYlGn_r', edgecolors='black', linewidth=0.5)
    
    # Add state labels
    for idx, row in df_plot.iterrows():
        ax.annotate(row['state'], 
                   (row['other_pct'], row['self_pct']),
                   fontsize=8, alpha=0.7,
                   xytext=(5, 5), textcoords='offset points')
    
    ax.set_xlabel('% Streets Named After OTHER States', fontsize=12)
    ax.set_ylabel('% Streets Named After OWN State', fontsize=12)
    ax.set_title('State Ego vs Humility: Self-Naming vs Other-State-Naming', 
                 fontsize=14, fontweight='bold')
    
    # Add diagonal line (equal self and other naming)
    max_val = max(df_plot['other_pct'].max(), df_plot['self_pct'].max())
    ax.plot([0, max_val], [0, max_val], 'k--', alpha=0.3, label='Equal naming')
    
    plt.colorbar(scatter, label='Ego Score', ax=ax)
    plt.legend()
    plt.tight_layout()
    
    output_path = output_dir / 'ego_vs_humility_scatter.png'
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    print(f"Saved ego vs humility scatter plot to {output_path}")
    plt.close()


def create_summary_report(output_dir: Path):
    """Create a text summary report."""
    popularity_df = pl.read_csv(output_dir / 'state_name_popularity.csv')
    ego_df = pl.read_csv(output_dir / 'state_ego_humility.csv')
    
    report = []
    report.append("="*80)
    report.append("COMPREHENSIVE STREET NAME ANALYSIS ACROSS 40 US STATES")
    report.append("="*80)
    report.append("")
    
    # Total statistics
    total_streets = ego_df['total_streets'].sum()
    total_state_named = popularity_df['street_count'].sum()
    
    report.append(f"Total streets analyzed: {total_streets:,}")
    report.append(f"Total state-named streets: {total_state_named:,}")
    report.append(f"Percentage of state-named streets: {total_state_named/total_streets*100:.2f}%")
    report.append("")
    
    # Most popular state names
    report.append("-"*80)
    report.append("TOP 10 MOST POPULAR STATE NAMES IN STREET NAMES")
    report.append("-"*80)
    for row in popularity_df.head(10).iter_rows(named=True):
        report.append(f"  {row['state_name'].title():20s}: {row['street_count']:6,} streets")
    report.append("")
    
    # Least popular state names
    report.append("-"*80)
    report.append("BOTTOM 10 LEAST POPULAR STATE NAMES IN STREET NAMES")
    report.append("-"*80)
    for row in popularity_df.tail(10).iter_rows(named=True):
        report.append(f"  {row['state_name'].title():20s}: {row['street_count']:6,} streets")
    report.append("")
    
    # Most egotistical states
    report.append("-"*80)
    report.append("TOP 10 MOST 'EGOTISTICAL' STATES")
    report.append("(States that name streets after themselves more than other states)")
    report.append("-"*80)
    ego_filtered = ego_df.filter(pl.col('other_state_streets') > 50)
    for row in ego_filtered.sort('ego_score', descending=True).head(10).iter_rows(named=True):
        report.append(f"  {row['state'].title():20s}: Ego Score = {row['ego_score']:.3f} "
                     f"({row['self_named_streets']:,} self / {row['other_state_streets']:,} other)")
    report.append("")
    
    # Most humble states
    report.append("-"*80)
    report.append("TOP 10 MOST 'HUMBLE' STATES")
    report.append("(States that name streets after other states more than themselves)")
    report.append("-"*80)
    for row in ego_filtered.sort('ego_score').head(10).iter_rows(named=True):
        report.append(f"  {row['state'].title():20s}: Ego Score = {row['ego_score']:.3f} "
                     f"({row['self_named_streets']:,} self / {row['other_state_streets']:,} other)")
    report.append("")
    
    # Interesting findings
    report.append("-"*80)
    report.append("INTERESTING FINDINGS")
    report.append("-"*80)
    
    # Washington is most popular
    most_popular = popularity_df.row(0, named=True)
    report.append(f"• '{most_popular['state_name'].title()}' is the most popular state name, appearing in "
                 f"{most_popular['street_count']:,} street names across the country.")
    
    # Vermont is most egotistical
    most_ego = ego_filtered.sort('ego_score', descending=True).row(0, named=True)
    report.append(f"• {most_ego['state'].title()} is the most 'egotistical' state with an ego score of "
                 f"{most_ego['ego_score']:.2f}, meaning it names streets after itself "
                 f"{most_ego['ego_score']:.1f}x more often than after other states.")
    
    # Massachusetts is most humble
    most_humble = ego_filtered.sort('ego_score').row(0, named=True)
    report.append(f"• {most_humble['state'].title()} is the most 'humble' state with an ego score of "
                 f"{most_humble['ego_score']:.3f}, naming streets after other states "
                 f"{1/most_humble['ego_score']:.1f}x more often than after itself.")
    
    report.append("")
    report.append("="*80)
    report.append("GENERATED FILES")
    report.append("="*80)
    report.append("• state_name_popularity.csv - Raw data on state name popularity")
    report.append("• state_ego_humility.csv - Raw data on state ego vs humility")
    report.append("• state_name_popularity.png - Visualization of popular/unpopular state names")
    report.append("• top_egotistical_states.png - Chart of most egotistical states")
    report.append("• top_humble_states.png - Chart of most humble states")
    report.append("• ego_vs_humility_scatter.png - Scatter plot of self vs other naming")
    report.append("• national_state_streets_map.html - Interactive map of all state-named streets")
    report.append("• *_state_streets_comparison.html - State-specific comparison maps")
    report.append("")
    
    report_text = "\n".join(report)
    
    # Print to console
    print(report_text)
    
    # Save to file
    output_path = output_dir / 'analysis_summary.txt'
    with open(output_path, 'w') as f:
        f.write(report_text)
    print(f"\nSaved summary report to {output_path}")


def main():
    """Generate all visualizations."""
    output_dir = Path(__file__).parent / "output"
    
    print("="*80)
    print("GENERATING VISUALIZATIONS")
    print("="*80)
    print()
    
    # Create charts
    plot_state_name_popularity(output_dir / 'state_name_popularity.csv', output_dir)
    plot_ego_humility(output_dir / 'state_ego_humility.csv', output_dir)
    
    print()
    print("="*80)
    print("GENERATING SUMMARY REPORT")
    print("="*80)
    print()
    
    # Create summary report
    create_summary_report(output_dir)
    
    print()
    print("="*80)
    print("ALL VISUALIZATIONS COMPLETE!")
    print("="*80)


if __name__ == "__main__":
    main()

