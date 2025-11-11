#!/usr/bin/env python3
"""
Shared plotting utilities for consistent, DRY visualization across the project.

Provides:
- Tufte-style matplotlib configuration
- Consistent color palettes
- Reusable plotting functions (horizontal bar charts, etc.)
- Standard save/output path handling
"""

from pathlib import Path
from typing import Optional, Tuple, List
import matplotlib
import matplotlib.pyplot as plt
import polars as pl


# ============================================================================
# Style Configuration
# ============================================================================

def setup_tufte_style():
    """
    Configure matplotlib with Tufte-style minimal chartjunk settings.
    
    Call this once before creating plots to ensure consistent styling.
    Uses serif fonts (Palatino family) and muted colors for a clean,
    professional appearance.
    """
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


# ============================================================================
# Color Palettes
# ============================================================================

def get_color_palette(name: str = 'default') -> dict:
    """
    Get a consistent color palette for plots.
    
    Args:
        name: Palette name ('default', 'categorical', etc.)
        
    Returns:
        Dictionary of color names to hex codes
    """
    palettes = {
        'default': {
            'primary': '#5A9B8E',      # Muted blue-teal
            'secondary': '#8B7E74',    # Muted brown
            'accent': '#C17767',       # Muted coral
            'neutral': '#cccccc',      # Light gray
            'text': '#111111',         # Near black
            'text_secondary': '#333333' # Dark gray
        },
        'categorical': [
            '#5A9B8E',  # Muted blue-teal
            '#C17767',  # Muted coral
            '#8B7E74',  # Muted brown
            '#7B9E89',  # Muted sage
            '#A67C8E',  # Muted mauve
        ]
    }
    return palettes.get(name, palettes['default'])


# ============================================================================
# Path Handling
# ============================================================================

def get_output_path_from_script(
    script_path: Path,
    filename: str,
    workspace_root: Optional[Path] = None
) -> Path:
    """
    Get output path based on script location in explore/ directory.
    
    Maps: workspace/explore/subdir/script.py -> workspace/main_outputs/subdir/filename
    
    Args:
        script_path: Path to the calling script (use Path(__file__))
        filename: Output filename (e.g., 'plot.svg')
        workspace_root: Optional workspace root (auto-detected if None)
        
    Returns:
        Full path to output file
    """
    if workspace_root is None:
        # Auto-detect: go up from explore/subdir/ to workspace/
        workspace_root = script_path.parent.parent.parent
    
    # Get subdirectory name (e.g., 'all_streets' from 'explore/all_streets/script.py')
    subdir_name = script_path.parent.name
    
    # Create output directory
    output_dir = workspace_root / "main_outputs" / subdir_name
    output_dir.mkdir(parents=True, exist_ok=True)
    
    return output_dir / filename


# ============================================================================
# Plotting Functions
# ============================================================================

def create_horizontal_bar_plot(
    data_df: pl.DataFrame,
    value_column: str,
    label_column: str,
    figsize: Tuple[float, float] = (4.5, 4.5),
    bar_color: Optional[str] = None,
    xlabel: str = 'Count',
    ylabel: str = '',
    show_value_labels: bool = True,
    reverse_order: bool = True
) -> Tuple[plt.Figure, plt.Axes, List[str], List[float]]:
    """
    Create a horizontal bar plot with consistent Tufte styling.
    
    Args:
        data_df: Polars DataFrame with data to plot
        value_column: Column name for bar values (x-axis)
        label_column: Column name for bar labels (y-axis)
        figsize: Figure size as (width, height) in inches
        bar_color: Hex color for bars (uses default palette if None)
        xlabel: Label for x-axis
        ylabel: Label for y-axis
        show_value_labels: Whether to show numeric labels on bars
        reverse_order: Whether to reverse order (highest at top)
        
    Returns:
        Tuple of (figure, axes, labels, values) for further customization
    """
    # Set up style
    setup_tufte_style()
    
    # Get default color if not specified
    if bar_color is None:
        bar_color = get_color_palette()['primary']
    
    # Convert to list for easier indexing
    data_list = data_df.to_dicts()
    if reverse_order:
        data_list.reverse()  # Highest at top
    
    # Extract data
    labels = [item[label_column] for item in data_list]
    values = [item[value_column] for item in data_list]
    
    # Create figure with transparent background for SVG
    fig, ax = plt.subplots(figsize=figsize, facecolor='none')
    fig.patch.set_facecolor('none')
    ax.set_facecolor('none')
    
    # Create horizontal bar plot
    bars = ax.barh(
        range(len(labels)),
        values,
        height=0.7,
        color=bar_color,
        edgecolor='none',
        linewidth=0
    )
    
    # Set y-axis labels
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels)
    
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
    ax.set_xlabel(xlabel, labelpad=8)
    ax.set_ylabel(ylabel)
    
    # Add value labels on bars if requested
    if show_value_labels and values:
        max_value = max(values)
        for i, value in enumerate(values):
            # Position label at end of bar with small padding
            ax.text(
                value + max_value * 0.015,
                i,
                f"{int(value):,}",
                va='center',
                ha='left',
                fontsize=10,
                color='#111111'
            )
        # Extra space for labels
        ax.set_xlim(left=0, right=max_value * 1.15)
    else:
        ax.set_xlim(left=0)
    
    # Clean up ticks
    ax.tick_params(axis='x', length=4, width=0.5, colors='#333333')
    ax.tick_params(axis='y', length=0, width=0, colors='#333333', pad=8)
    
    plt.tight_layout(pad=0.1)
    
    return fig, ax, labels, values


# ============================================================================
# Save Functions
# ============================================================================

def save_plot(
    fig: plt.Figure,
    output_path: Path,
    format: Optional[str] = None,
    dpi: int = 150,
    transparent: bool = True,
    verbose: bool = True
) -> None:
    """
    Save a matplotlib figure with consistent settings.
    
    Args:
        fig: Matplotlib figure to save
        output_path: Path to save the plot
        format: Output format ('svg', 'png', etc.). Auto-detected from path if None
        dpi: DPI for raster formats (ignored for SVG)
        transparent: Whether to use transparent background
        verbose: Whether to print save confirmation
    """
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Auto-detect format from extension if not specified
    if format is None:
        format = output_path.suffix.lstrip('.')
    
    # Save with appropriate settings
    if format.lower() == 'svg':
        plt.savefig(
            output_path,
            format='svg',
            bbox_inches=None,  # Use None to preserve full figure size (fixed width)
            pad_inches=0.1,    # Small padding
            transparent=transparent
        )
    else:
        plt.savefig(
            output_path,
            dpi=dpi,
            bbox_inches='tight',
            pad_inches=0.01,
            facecolor='white' if not transparent else 'none',
            transparent=transparent
        )
    
    if verbose:
        print(f"Saved plot to {output_path}")

