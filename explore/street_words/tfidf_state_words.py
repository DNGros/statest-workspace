#!/usr/bin/env python3
"""Compute TF-IDF scores for words in street names by state to find distinctive regional terms."""

import sys
from pathlib import Path
import polars as pl
import numpy as np
from typing import Optional

# Add parent directory to path to import workspace modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
from workspace.load_street_df import load_street_df
from workspace.states import USState


def compute_tfidf_by_state(
    min_word_freq: int = 10,
    top_n_per_state: int = 3,
    output_path: Optional[Path] = None,
    filter_stop_words: bool = True,
    num_stop_words: int = 25
) -> pl.DataFrame:
    """
    Compute TF-IDF scores for words in street names by state.
    
    Uses street-as-document approach:
    - Each individual street is a "document"
    - TF-IDF identifies words that are rare across all streets nationwide
    - Sum TF-IDF scores by state to find which states use these rare words most
    
    This finds words that are both:
    1. Uncommon in street names generally (high IDF)
    2. Concentrated in particular states (high summed TF-IDF)
    
    Args:
        min_word_freq: Minimum number of times a word must appear in a state to be considered
        top_n_per_state: Number of top TF-IDF words to return per state
        output_path: Optional path to save results as CSV
        filter_stop_words: Whether to filter out common street type words (like "road", "street", "drive")
        num_stop_words: Number of most common words to filter out (default: 25)
        
    Returns:
        DataFrame with columns: state, word, tfidf_score, word_count, num_streets_with_word
    """
    print("Loading street data from all states...")
    lf = load_street_df()
    
    # First, get total number of streets for IDF calculation
    print("Counting total streets...")
    total_streets = lf.select(pl.len()).collect().item()
    print(f"Total streets: {total_streets:,}")
    
    # Identify stop words (most common words) if filtering is enabled
    stop_words = set()
    if filter_stop_words:
        print(f"Identifying top {num_stop_words} most common words to filter...")
        stop_words_df = (
            lf
            .select("street_name")
            .with_columns(pl.col("street_name").str.split(" ").alias("words"))
            .explode("words")
            .filter((pl.col("words") != "") & (pl.col("words").str.len_chars() > 0))
            .with_columns(pl.col("words").str.to_lowercase().alias("word"))
            .group_by("word")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
            .head(num_stop_words)
            .collect()
        )
        stop_words = set(stop_words_df["word"].to_list())
        print(f"Filtering out: {', '.join(sorted(stop_words))}")
    
    # Split street names into words, keeping track of which street each word came from
    print("Extracting words from street names...")
    
    # Create a unique street identifier (state + street_name)
    streets_with_words = (
        lf
        .select(["street_name", "state"])
        .with_columns(
            # Create unique street ID
            (pl.col("state") + ":" + pl.col("street_name")).alias("street_id")
        )
        .with_columns(
            # Split street names by whitespace into a list of words
            pl.col("street_name").str.split(" ").alias("words")
        )
        .explode("words")  # Expand each word into its own row
        .filter(
            # Filter out empty strings
            (pl.col("words") != "") &
            (pl.col("words").str.len_chars() > 0)
        )
        .with_columns(
            # Convert to lowercase for consistent counting
            pl.col("words").str.to_lowercase().alias("word")
        )
        .select(["street_id", "state", "word"])
        .unique()  # Each word appears at most once per street (TF = 1 or 0)
        .collect()
    )
    
    # Filter out stop words if enabled
    if filter_stop_words and stop_words:
        print(f"Filtering out {len(stop_words)} stop words...")
        streets_with_words = streets_with_words.filter(~pl.col("word").is_in(list(stop_words)))
    
    print(f"Extracted {len(streets_with_words):,} (street, word) pairs")
    
    # Calculate document frequency: how many streets contain each word
    print("Computing document frequencies (number of streets per word)...")
    word_df = (
        streets_with_words
        .group_by("word")
        .agg(pl.col("street_id").n_unique().alias("num_streets_with_word"))
    )
    
    # Calculate IDF for each word: log(total_streets / num_streets_with_word)
    word_df = word_df.with_columns([
        (total_streets / pl.col("num_streets_with_word")).log().alias("idf")
    ])
    
    print(f"Computed IDF for {len(word_df):,} unique words")
    
    # Join IDF back to street-word pairs
    streets_with_words = streets_with_words.join(word_df, on="word", how="left")
    
    # Calculate TF-IDF for each (street, word) pair
    # Since each word appears at most once per street, TF = 1, so TF-IDF = IDF
    streets_with_words = streets_with_words.with_columns([
        pl.col("idf").alias("tfidf")  # TF=1, so TF-IDF = IDF
    ])
    
    # Sum TF-IDF scores by state and word
    print("Aggregating TF-IDF scores by state...")
    state_word_tfidf = (
        streets_with_words
        .group_by(["state", "word"])
        .agg([
            pl.col("tfidf").sum().alias("tfidf_score"),  # Sum of TF-IDF across all streets in state
            pl.col("street_id").n_unique().alias("word_count"),  # Number of streets with this word in state
            pl.col("num_streets_with_word").first().alias("num_streets_with_word")  # Total streets with word nationwide
        ])
    )
    
    # Filter to words that appear at least min_word_freq times in a state
    print(f"Filtering to words with at least {min_word_freq} occurrences per state...")
    state_word_tfidf = state_word_tfidf.filter(pl.col("word_count") >= min_word_freq)
    print(f"After filtering: {len(state_word_tfidf):,} (state, word) pairs remain")
    
    # Get top N words per state by TF-IDF score
    print(f"Extracting top {top_n_per_state} words per state...")
    top_words = (
        state_word_tfidf
        .sort(["state", "tfidf_score"], descending=[False, True])
        .group_by("state")
        .agg([
            pl.col("word").head(top_n_per_state).alias("words"),
            pl.col("tfidf_score").head(top_n_per_state).alias("tfidf_scores"),
            pl.col("word_count").head(top_n_per_state).alias("word_counts"),
            pl.col("num_streets_with_word").head(top_n_per_state).alias("num_streets"),
        ])
        .sort("state")
    )
    
    # Explode for easier viewing
    top_words_exploded = (
        top_words
        .explode(["words", "tfidf_scores", "word_counts", "num_streets"])
        .rename({"words": "word", "tfidf_scores": "tfidf_score", 
                 "word_counts": "word_count", "num_streets": "num_streets_with_word"})
    )
    
    # Save to CSV if output path provided
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        top_words_exploded.write_csv(output_path)
        print(f"\nSaved results to {output_path}")
    
    return top_words_exploded


def print_results(df: pl.DataFrame, states_to_show: Optional[list[str]] = None):
    """
    Pretty print TF-IDF results.
    
    Args:
        df: DataFrame with TF-IDF results
        states_to_show: Optional list of states to display. If None, shows all states.
    """
    if states_to_show:
        df = df.filter(pl.col("state").is_in(states_to_show))
    
    print("\n" + "="*80)
    print("TOP TF-IDF WORDS BY STATE (Street-as-Document)")
    print("="*80)
    
    for state in df["state"].unique().sort():
        state_data = df.filter(pl.col("state") == state)
        print(f"\n{state.upper()}")
        print("-" * 60)
        
        for row in state_data.iter_rows(named=True):
            print(f"  {row['word']:20s} | "
                  f"TF-IDF: {row['tfidf_score']:8.2f} | "
                  f"In State: {row['word_count']:5d} streets | "
                  f"Nationwide: {row['num_streets_with_word']:6d} streets")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Compute TF-IDF scores for street name words by state'
    )
    parser.add_argument(
        '--min-freq',
        type=int,
        default=10,
        help='Minimum word frequency per state (default: 10)'
    )
    parser.add_argument(
        '--top-n',
        type=int,
        default=3,
        help='Number of top words per state (default: 3)'
    )
    parser.add_argument(
        '--output',
        type=Path,
        default=None,
        help='Output CSV path (default: main_outputs/street_words/tfidf_by_state.csv)'
    )
    parser.add_argument(
        '--states',
        nargs='+',
        default=None,
        help='Optional: only show results for specific states (e.g., california texas)'
    )
    parser.add_argument(
        '--no-filter-stop-words',
        action='store_true',
        help='Disable filtering of common street type words'
    )
    parser.add_argument(
        '--num-stop-words',
        type=int,
        default=25,
        help='Number of most common words to filter out (default: 25)'
    )
    
    args = parser.parse_args()
    
    # Set default output path if not provided
    if args.output is None:
        workspace_dir = Path(__file__).parent.parent.parent
        output_dir = workspace_dir / "main_outputs" / "street_words"
        output_dir.mkdir(parents=True, exist_ok=True)
        args.output = output_dir / "tfidf_by_state.csv"
    
    # Compute TF-IDF
    results = compute_tfidf_by_state(
        min_word_freq=args.min_freq,
        top_n_per_state=args.top_n,
        output_path=args.output,
        filter_stop_words=not args.no_filter_stop_words,
        num_stop_words=args.num_stop_words
    )
    
    # Print results
    print_results(results, states_to_show=args.states)
    
    print(f"\n\nTotal states analyzed: {results['state'].n_unique()}")
    print(f"Total unique words in top-{args.top_n}: {results['word'].n_unique()}")


if __name__ == "__main__":
    main()

