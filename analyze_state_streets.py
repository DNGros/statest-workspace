#!/usr/bin/env python3
"""Analyze which states name streets after themselves vs other states."""

from pathlib import Path
import polars as pl
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from workspace.states import USState


def load_all_state_data(data_dir: Path = None) -> dict[str, pl.DataFrame]:
    """Load all available state parquet files."""
    if data_dir is None:
        data_dir = Path(__file__).parent / "data/streetdfs"
    
    state_data = {}
    for parquet_file in data_dir.glob("*_streets.parquet"):
        state_name = parquet_file.stem.replace("_streets", "")
        state_data[state_name] = pl.read_parquet(parquet_file)
        print(f"Loaded {state_name}: {len(state_data[state_name]):,} streets")
    
    return state_data


def count_state_mentions_in_streets(df: pl.DataFrame, state_names: list[str] = None) -> dict[str, int]:
    """Count how many times each state name appears in street names."""
    if state_names is None:
        state_names = USState.all_names()
    
    state_counts = {}
    for state in state_names:
        # Case-insensitive search for state name as whole word
        count = len(df.filter(
            pl.col('street_name').str.to_lowercase().str.contains(f'(?i)\\b{state}\\b')
        ))
        if count > 0:
            state_counts[state] = count
    
    return state_counts


def analyze_state_ego_vs_humility(state_data: dict[str, pl.DataFrame]) -> pd.DataFrame:
    """
    Analyze which states are 'egotistical' (name streets after themselves)
    vs 'humble' (name streets after other states).
    
    Returns DataFrame with columns:
    - state: state name
    - total_streets: total number of streets
    - self_named: streets named after this state (within this state)
    - other_named: streets named after other states (within this state)
    - ego_score: self_named / total_streets (higher = more egotistical)
    - humility_score: other_named / total_streets (higher = more humble)
    """
    results = []
    
    for state_name, df in state_data.items():
        total_streets = len(df)
        
        # Count streets named after this state itself
        self_named = len(df.filter(
            pl.col('street_name').str.to_lowercase().str.contains(f'(?i)\\b{state_name}\\b')
        ))
        
        # Count streets named after OTHER states
        other_states = [s for s in USState.all_names() if s != state_name]
        other_named = 0
        for other_state in other_states:
            count = len(df.filter(
                pl.col('street_name').str.to_lowercase().str.contains(f'(?i)\\b{other_state}\\b')
            ))
            other_named += count
        
        results.append({
            'state': state_name,
            'total_streets': total_streets,
            'self_named': self_named,
            'other_named': other_named,
            'ego_score': self_named / total_streets * 100 if total_streets > 0 else 0,
            'humility_score': other_named / total_streets * 100 if total_streets > 0 else 0,
        })
    
    return pd.DataFrame(results).sort_values('ego_score', ascending=False)


def plot_ego_vs_humility(df: pd.DataFrame, output_path: Path = None):
    """Create scatter plot of ego vs humility scores."""
    fig, ax = plt.subplots(figsize=(12, 8))
    
    scatter = ax.scatter(
        df['ego_score'], 
        df['humility_score'],
        s=df['total_streets'] / 100,  # Size by number of streets
        alpha=0.6,
        c=range(len(df)),
        cmap='viridis'
    )
    
    # Label each point with state name
    for _, row in df.iterrows():
        ax.annotate(
            row['state'].title(),
            (row['ego_score'], row['humility_score']),
            fontsize=9,
            alpha=0.8
        )
    
    ax.set_xlabel('Ego Score (% streets named after self)', fontsize=12)
    ax.set_ylabel('Humility Score (% streets named after other states)', fontsize=12)
    ax.set_title('State Street Naming: Ego vs Humility', fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"Saved plot to {output_path}")
    else:
        plt.show()


def plot_top_egotistical_states(df: pd.DataFrame, top_n: int = 10, output_path: Path = None):
    """Bar chart of most egotistical states."""
    top_df = df.nlargest(top_n, 'ego_score')
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    bars = ax.barh(
        range(len(top_df)),
        top_df['ego_score'],
        color=sns.color_palette('Reds_r', len(top_df))
    )
    
    ax.set_yticks(range(len(top_df)))
    ax.set_yticklabels([s.title() for s in top_df['state']])
    ax.set_xlabel('Ego Score (% of streets named after self)', fontsize=12)
    ax.set_title(f'Top {top_n} Most "Egotistical" States', fontsize=14, fontweight='bold')
    ax.invert_yaxis()
    
    # Add value labels
    for i, (_, row) in enumerate(top_df.iterrows()):
        ax.text(
            row['ego_score'] + 0.01,
            i,
            f"{row['ego_score']:.2f}% ({row['self_named']:,} streets)",
            va='center',
            fontsize=9
        )
    
    plt.tight_layout()
    
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"Saved plot to {output_path}")
    else:
        plt.show()


def plot_top_humble_states(df: pd.DataFrame, top_n: int = 10, output_path: Path = None):
    """Bar chart of most humble states."""
    top_df = df.nlargest(top_n, 'humility_score')
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    bars = ax.barh(
        range(len(top_df)),
        top_df['humility_score'],
        color=sns.color_palette('Blues_r', len(top_df))
    )
    
    ax.set_yticks(range(len(top_df)))
    ax.set_yticklabels([s.title() for s in top_df['state']])
    ax.set_xlabel('Humility Score (% of streets named after other states)', fontsize=12)
    ax.set_title(f'Top {top_n} Most "Humble" States', fontsize=14, fontweight='bold')
    ax.invert_yaxis()
    
    # Add value labels
    for i, (_, row) in enumerate(top_df.iterrows()):
        ax.text(
            row['humility_score'] + 0.01,
            i,
            f"{row['humility_score']:.2f}% ({row['other_named']:,} streets)",
            va='center',
            fontsize=9
        )
    
    plt.tight_layout()
    
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"Saved plot to {output_path}")
    else:
        plt.show()


def analyze_which_states_are_popular_in_street_names(state_data: dict[str, pl.DataFrame]) -> pd.DataFrame:
    """
    Count how many times each state name appears in street names across ALL states.
    This tells us which states are most popular to name streets after.
    """
    all_state_names = USState.all_names()
    popularity_counts = {state: 0 for state in all_state_names}
    
    # Count across all states
    for state_name, df in state_data.items():
        state_counts = count_state_mentions_in_streets(df, all_state_names)
        for state, count in state_counts.items():
            popularity_counts[state] += count
    
    # Convert to DataFrame
    df = pd.DataFrame([
        {'state': state, 'total_mentions': count}
        for state, count in popularity_counts.items()
        if count > 0
    ]).sort_values('total_mentions', ascending=False)
    
    return df


def plot_most_popular_state_names(df: pd.DataFrame, top_n: int = 15, output_path: Path = None):
    """Bar chart of which state names appear most in street names."""
    top_df = df.head(top_n)
    
    fig, ax = plt.subplots(figsize=(10, 8))
    
    bars = ax.barh(
        range(len(top_df)),
        top_df['total_mentions'],
        color=sns.color_palette('viridis', len(top_df))
    )
    
    ax.set_yticks(range(len(top_df)))
    ax.set_yticklabels([s.title() for s in top_df['state']])
    ax.set_xlabel('Number of Streets Named After This State', fontsize=12)
    ax.set_title(f'Top {top_n} Most Popular State Names in Streets', fontsize=14, fontweight='bold')
    ax.invert_yaxis()
    
    # Add value labels
    for i, (_, row) in enumerate(top_df.iterrows()):
        ax.text(
            row['total_mentions'] + max(top_df['total_mentions']) * 0.01,
            i,
            f"{row['total_mentions']:,}",
            va='center',
            fontsize=9
        )
    
    plt.tight_layout()
    
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"Saved plot to {output_path}")
    else:
        plt.show()


def main():
    """Run comprehensive state street analysis."""
    print("="*70)
    print("STATE STREET ANALYSIS")
    print("="*70)
    
    # Load all available state data
    print("\nLoading state data...")
    state_data = load_all_state_data()
    
    if not state_data:
        print("No state data found! Run download_state_osm.py and process_osm_to_parquet.py first.")
        return
    
    print(f"\nAnalyzing {len(state_data)} states...")
    
    # Create output directory
    output_dir = Path(__file__).parent / "output"
    output_dir.mkdir(exist_ok=True)
    
    # Analysis 1: Ego vs Humility
    print("\n" + "="*70)
    print("EGO VS HUMILITY ANALYSIS")
    print("="*70)
    
    ego_df = analyze_state_ego_vs_humility(state_data)
    print("\nResults:")
    print(ego_df.to_string(index=False))
    
    # Save results
    ego_df.to_csv(output_dir / "state_ego_humility.csv", index=False)
    print(f"\nSaved results to {output_dir / 'state_ego_humility.csv'}")
    
    # Create visualizations
    if len(state_data) > 1:
        plot_ego_vs_humility(ego_df, output_dir / "ego_vs_humility_scatter.png")
    
    plot_top_egotistical_states(ego_df, top_n=min(10, len(ego_df)), 
                                output_path=output_dir / "top_egotistical_states.png")
    
    plot_top_humble_states(ego_df, top_n=min(10, len(ego_df)),
                          output_path=output_dir / "top_humble_states.png")
    
    # Analysis 2: Most popular state names
    print("\n" + "="*70)
    print("MOST POPULAR STATE NAMES IN STREETS")
    print("="*70)
    
    popularity_df = analyze_which_states_are_popular_in_street_names(state_data)
    print("\nTop 20 most popular state names:")
    print(popularity_df.head(20).to_string(index=False))
    
    popularity_df.to_csv(output_dir / "state_name_popularity.csv", index=False)
    
    plot_most_popular_state_names(popularity_df, top_n=15,
                                  output_path=output_dir / "most_popular_state_names.png")
    
    print("\n" + "="*70)
    print("ANALYSIS COMPLETE!")
    print("="*70)
    print(f"\nAll outputs saved to: {output_dir}")


if __name__ == "__main__":
    main()

