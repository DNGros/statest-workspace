#!/usr/bin/env python3
"""Create a tile grid map of top TF-IDF words by state."""

import sys
from pathlib import Path
import polars as pl
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np

# Add parent directory to path to import workspace modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
from workspace.explore.street_words.tfidf_state_words import compute_tfidf_by_state
from workspace.explore.street_words.word_colors import get_word_color
from workspace.plot_utils import setup_tufte_style, get_color_palette

# Standard US state grid layout (based on common geofacet layouts)
# Format: state_code -> (row, col, abbreviation)
US_STATE_GRID = {
    'alaska': (0, 1, 'AK'),
    'maine': (0, 10, 'ME'),
    'wisconsin': (1, 6, 'WI'),
    'vermont': (1, 9, 'VT'),
    'new-hampshire': (1, 10, 'NH'),
    'washington': (2, 1, 'WA'),
    'idaho': (2, 2, 'ID'),
    'montana': (2, 3, 'MT'),
    'north-dakota': (2, 4, 'ND'),
    'minnesota': (2, 5, 'MN'),
    'michigan': (2, 7, 'MI'),
    'new-york': (2, 8, 'NY'),
    'massachusetts': (2, 9, 'MA'),
    'rhode-island': (2, 10, 'RI'),
    'oregon': (3, 1, 'OR'),
    'nevada': (3, 2, 'NV'),
    'wyoming': (3, 3, 'WY'),
    'south-dakota': (3, 4, 'SD'),
    'iowa': (3, 5, 'IA'),
    'illinois': (3, 6, 'IL'),
    'indiana': (3, 7, 'IN'),
    'ohio': (3, 8, 'OH'),
    'pennsylvania': (3, 9, 'PA'),
    'connecticut': (3, 10, 'CT'),
    'california': (4, 1, 'CA'),
    'utah': (4, 2, 'UT'),
    'colorado': (4, 3, 'CO'),
    'nebraska': (4, 4, 'NE'),
    'missouri': (4, 5, 'MO'),
    'kentucky': (4, 6, 'KY'),
    'west-virginia': (4, 7, 'WV'),
    'virginia': (4, 8, 'VA'),
    'maryland': (4, 9, 'MD'),
    'new-jersey': (4, 10, 'NJ'),
    'arizona': (5, 2, 'AZ'),
    'new-mexico': (5, 3, 'NM'),
    'kansas': (5, 4, 'KS'),
    'arkansas': (5, 5, 'AR'),
    'tennessee': (5, 6, 'TN'),
    'north-carolina': (5, 7, 'NC'),
    'south-carolina': (5, 8, 'SC'),
    'district-of-columbia': (5, 9, 'DC'),
    'delaware': (5, 10, 'DE'),
    'oklahoma': (6, 4, 'OK'),
    'louisiana': (6, 5, 'LA'),
    'mississippi': (6, 6, 'MS'),
    'alabama': (6, 7, 'AL'),
    'georgia': (6, 8, 'GA'),
    'hawaii': (7, 1, 'HI'),
    'texas': (7, 4, 'TX'),
    'florida': (7, 9, 'FL'),
}


def create_tfidf_tile_grid(
    tfidf_data: pl.DataFrame,
    top_n: int = 3,
    output_path: Path = None,
    figsize: tuple = (22, 16)
):
    """
    Create a tile grid map showing top TF-IDF words for each state.
    
    Args:
        tfidf_data: DataFrame with columns: state, word, tfidf_score, word_count
        top_n: Number of top words to show per state
        output_path: Path to save the plot
        figsize: Figure size in inches
    """
    setup_tufte_style()
    
    # Get color palette
    colors = get_color_palette()
    bar_color = colors['primary']
    
    # Determine grid dimensions
    max_row = max(pos[0] for pos in US_STATE_GRID.values())
    max_col = max(pos[1] for pos in US_STATE_GRID.values())
    n_rows = max_row + 1
    n_cols = max_col + 1
    
    # Create figure and grid
    fig = plt.figure(figsize=figsize, facecolor='none')
    fig.patch.set_facecolor('none')
    
    # Create grid spec with some spacing
    gs = gridspec.GridSpec(
        n_rows, n_cols,
        figure=fig,
        hspace=0.4,
        wspace=0.3,
        left=0.05,
        right=0.95,
        top=0.95,
        bottom=0.05
    )
    
    # Find global max score for shared x-axis
    global_max_score = tfidf_data['tfidf_score'].max()
    
    # Process data for each state
    for state, (row, col, abbrev) in US_STATE_GRID.items():
        # Get data for this state
        state_data = tfidf_data.filter(pl.col('state') == state).head(top_n)
        
        if len(state_data) == 0:
            continue
        
        # Create subplot for this state
        ax = fig.add_subplot(gs[row, col])
        ax.set_facecolor('none')
        
        # Extract words and scores
        words = state_data['word'].to_list()
        scores = state_data['tfidf_score'].to_list()
        
        # Reverse order so highest is at top
        words = words[::-1]
        scores = scores[::-1]
        
        # Create horizontal bar chart with word-specific colors
        y_pos = np.arange(len(words))
        # Get color for each word
        bar_colors = [get_word_color(word) for word in words]
        bars = ax.barh(y_pos, scores, height=0.7, color=bar_colors, edgecolor='none', alpha=0.6)
        
        # Add word labels centered in the subplot (not on the bar)
        for i, word in enumerate(words):
            ax.text(
                0.5,  # Center of subplot (in axis coordinates)
                i,
                word,
                ha='center',
                va='center',
                fontsize=18,
                fontweight='bold',
                color='#111111',
                transform=ax.get_yaxis_transform()  # Use data coords for y, axis coords for x
            )
        
        # Remove y-axis (no need since words are on bars)
        ax.set_yticks([])
        
        # Remove x-axis labels and ticks (too cluttered)
        ax.set_xticks([])
        ax.set_xlabel('')
        
        # Clean up spines
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_visible(False)
        ax.spines['bottom'].set_visible(False)
        
        # Add state abbreviation as title (much larger)
        ax.set_title(abbrev, fontsize=20, fontweight='bold', pad=5)
        
        # Set limits - use global max for comparable bars across states
        ax.set_xlim(0, global_max_score * 1.05)
        ax.set_ylim(-0.5, len(words) - 0.5)
        
        # Remove tick marks
        ax.tick_params(axis='both', length=0, pad=2)
    
    # Save if output path provided
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(
            output_path,
            format='svg',
            facecolor='none',
            edgecolor='none',
            transparent=True,
            bbox_inches='tight',
            pad_inches=0.2
        )
        print(f"Saved tile grid map to {output_path}")
    
    return fig


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Create tile grid map of TF-IDF words by state'
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
        help='Output path for the plot (default: main_outputs/street_words/tfidf_grid.svg)'
    )
    parser.add_argument(
        '--figsize',
        nargs=2,
        type=float,
        default=[22, 16],
        help='Figure size in inches (width height, default: 22 16)'
    )
    
    args = parser.parse_args()
    
    # Set default output path if not provided
    if args.output is None:
        workspace_dir = Path(__file__).parent.parent.parent
        output_dir = workspace_dir / "main_outputs" / "street_words"
        output_dir.mkdir(parents=True, exist_ok=True)
        args.output = output_dir / "tfidf_grid.svg"
    
    # Compute TF-IDF data
    print("Computing TF-IDF scores...")
    tfidf_data = compute_tfidf_by_state(
        min_word_freq=args.min_freq,
        top_n_per_state=args.top_n,
        output_path=None,  # Don't save CSV here
        filter_stop_words=True,
        num_stop_words=25
    )
    
    # Create tile grid visualization
    print("Creating tile grid map...")
    create_tfidf_tile_grid(
        tfidf_data=tfidf_data,
        top_n=args.top_n,
        output_path=args.output,
        figsize=tuple(args.figsize)
    )
    
    print("Done!")


if __name__ == "__main__":
    main()

