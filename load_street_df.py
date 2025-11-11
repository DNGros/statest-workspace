"""Clean interface for loading street data from processed parquet files."""

from pathlib import Path
from typing import Optional, Union
import polars as pl
from workspace.states import USState
from workspace.cache_utils import FileCache

DEFAULT_FILTER_TYPES = [
    # From the main roads in https://wiki.openstreetmap.org/wiki/Key:highway
    "motorway",
    "trunk",
    "primary",
    "secondary",
    "tertiary",
    "unclassified",
    "residential",
]

script_dir = Path(__file__).parent
DEFAULT_DATA_DIR = script_dir / "data" / "streetdfs_1mi"
DEFAULT_CACHE_DIR = script_dir / "data" / "cache"


def _get_parquet_paths(
    state: Optional[Union[str, list[str]]],
    data_dir: Path,
) -> list[Path]:
    """
    Get list of parquet file paths to load.
    
    Args:
        state: State name(s) or None for all states
        data_dir: Directory containing parquet files
        
    Returns:
        List of parquet file paths
        
    Raises:
        FileNotFoundError: If data directory or requested states not found
    """
    if not data_dir.exists():
        raise FileNotFoundError(
            f"Data directory not found: {data_dir}\n"
            "Please run data processing scripts first to generate parquet files."
        )
    
    # Determine which states to load
    if state is None:
        # Load all available states
        parquet_files = sorted(data_dir.glob("*_streets.parquet"))
        if not parquet_files:
            raise FileNotFoundError(
                f"No parquet files found in {data_dir}\n"
                "Please run data processing scripts first."
            )
        return parquet_files
    
    # Convert to list if single state
    state_names = [state] if isinstance(state, str) else state
    
    # Build paths and check for missing states
    parquet_paths = []
    missing_states = []
    
    for state_name in state_names:
        # Convert state name to filename format (spaces to dashes)
        state_filename = state_name.replace(" ", "-")
        parquet_path = data_dir / f"{state_filename}_streets.parquet"
        
        if not parquet_path.exists():
            missing_states.append(state_name)
        else:
            parquet_paths.append(parquet_path)
    
    if missing_states:
        available = sorted([f.stem.replace("_streets", "") for f in data_dir.glob("*_streets.parquet")])
        raise FileNotFoundError(
            f"Data not found for states: {', '.join(missing_states)}\n"
            f"Available states: {', '.join(available)}"
        )
    
    return parquet_paths


def load_street_df(
    state: Optional[Union[str, list[str]]] = None,
    data_dir: Optional[Path] = None,
    filter_to_types: Optional[list[str]] = DEFAULT_FILTER_TYPES,
) -> pl.LazyFrame:
    """
    Load street data from processed parquet files as a LazyFrame.
    
    Returns a LazyFrame for optimal performance. Call .collect() to materialize
    into a DataFrame when needed.
    
    Args:
        state: State name(s) to load. Can be:
            - None: Load all available states
            - str: Single state name (e.g., "california", "new york")
            - list[str]: Multiple state names
        data_dir: Directory containing parquet files. Defaults to workspace/data/streetdfs_1mi
        filter_to_types: List of highway types to include. Set to None to include all types.
            Defaults to main road types (motorway, trunk, primary, secondary, tertiary, 
            unclassified, residential). See https://wiki.openstreetmap.org/wiki/Key:highway
    
    Returns:
        polars LazyFrame with columns:
            - street_name: Name of the street
            - state: State where the street is located
            - lat, lon: Representative coordinates
            - num_segments: Number of OSM way segments grouped into this street
            - highway_type: Most common highway type for the street
            - length_km: Total length of the street in kilometers
    
    Examples:
        >>> # Load all states (returns LazyFrame)
        >>> lf = load_street_df()
        >>> df = lf.collect()  # Materialize to DataFrame
        
        >>> # Load single state and filter
        >>> df = load_street_df("california").filter(pl.col("length_km") > 10).collect()
        
        >>> # Load multiple states
        >>> lf = load_street_df(["california", "texas", "new york"])
        
        >>> # Include all highway types (no filtering)
        >>> lf = load_street_df("california", filter_to_types=None)
        
        >>> # Chain operations efficiently
        >>> result = (
        ...     load_street_df("california")
        ...     .filter(pl.col("length_km") > 5)
        ...     .group_by("highway_type")
        ...     .agg(pl.len())
        ...     .collect()
        ... )
    """
    if data_dir is None:
        data_dir = DEFAULT_DATA_DIR
    
    # Get list of parquet files to load
    parquet_paths = _get_parquet_paths(state, data_dir)
    
    # Build lazy frame by scanning and concatenating all parquet files
    lazy_frames = [pl.scan_parquet(path) for path in parquet_paths]
    
    # Concatenate all lazy frames
    if len(lazy_frames) == 1:
        lf = lazy_frames[0]
    else:
        lf = pl.concat(lazy_frames, how="diagonal_relaxed")
    
    # Apply highway type filter if specified
    if filter_to_types is not None:
        lf = lf.filter(pl.col("highway_type").is_in(filter_to_types))
    
    return lf


def get_available_states(data_dir: Optional[Path] = None) -> list[str]:
    """
    Get list of states with available data.
    
    Args:
        data_dir: Directory containing parquet files. Defaults to workspace/data/streetdfs
    
    Returns:
        List of state names (lowercase, with spaces)
    """
    if data_dir is None:
        script_dir = Path(__file__).parent
        data_dir = script_dir / "data" / "streetdfs"
    
    if not data_dir.exists():
        return []
    
    parquet_files = sorted(data_dir.glob("*_streets.parquet"))
    return [f.stem.replace("_streets", "") for f in parquet_files]


def has_state_name_mask() -> pl.Expr:
    """
    Returns a boolean mask expression that identifies streets with state names in their name.
    
    The mask checks if the street_name (case-insensitive) contains any US state name as a whole word.
    Uses word boundaries to avoid false matches (e.g., "Jermaine" won't match "Maine").
    This can be used with polars filter operations.
    
    Returns:
        polars Expr that evaluates to a boolean Series when applied to a DataFrame
    
    Examples:
        >>> df = load_street_df()
        >>> mask = has_state_name_mask()
        >>> state_streets = df.filter(mask)
        
        >>> # Or use directly in filter
        >>> state_streets = df.filter(has_state_name_mask())
    """
    state_names = USState.all_names()
    
    # Create a case-insensitive check for each state name with word boundaries
    # Start with False and OR together all state name matches
    mask = pl.lit(False)
    for state_name in state_names:
        # Escape special regex characters in state name and wrap with word boundaries
        # Use \b for word boundaries to match whole words only
        escaped_name = state_name.replace("\\", "\\\\").replace(".", "\\.").replace("(", "\\(").replace(")", "\\)")
        pattern = r"\b" + escaped_name + r"\b"
        # Check if lowercase street_name contains the state name as a whole word
        mask = mask | pl.col("street_name").str.to_lowercase().str.contains(pattern, literal=False)
    
    return mask


def load_state_streets_df(
    state: Optional[Union[str, list[str]]] = None,
    data_dir: Optional[Path] = None,
    filter_to_types: Optional[list[str]] = DEFAULT_FILTER_TYPES,
    exclude_numbered: bool = True,
    use_cache: bool = True,
) -> pl.LazyFrame:
    """
    Load street data and filter to only streets with state names in their name.
    
    This function loads streets using load_street_df() and then filters to only
    those streets whose name contains a US state name (e.g., "Texas St", "California Ave").
    By default, excludes streets with numbers in their name (e.g., "Virginia Route 32B").
    Returns a LazyFrame - call .collect() to materialize into a DataFrame.
    
    Caching: By default, results are cached based on input parameters and source data
    modification times. This significantly speeds up repeated calls with the same parameters.
    
    Args:
        state: State name(s) to load data from. Can be:
            - None: Load all available states
            - str: Single state name (e.g., "california", "new york")
            - list[str]: Multiple state names
        data_dir: Directory containing parquet files. Defaults to workspace/data/streetdfs_1mi
        filter_to_types: List of highway types to include. Set to None to include all types.
        exclude_numbered: If True (default), exclude streets with numbers in their name.
            Set to False to include numbered streets like "Virginia Route 32B".
        use_cache: If True (default), use cached results when available. Set to False
            to force recomputation.
    
    Returns:
        polars LazyFrame containing only streets with state names in their street_name
    
    Examples:
        >>> # Load all state-named streets from all states (excluding numbered)
        >>> lf = load_state_streets_df()
        >>> df = lf.collect()
        
        >>> # Load state-named streets from California only
        >>> df = load_state_streets_df("california").collect()
        
        >>> # Include numbered streets (e.g., "Nevada Route 50")
        >>> lf = load_state_streets_df(exclude_numbered=False)
        
        >>> # Force recomputation (ignore cache)
        >>> lf = load_state_streets_df(use_cache=False)
        
        >>> # Chain with other operations
        >>> result = (
        ...     load_state_streets_df("california")
        ...     .filter(pl.col("length_km") > 1)
        ...     .collect()
        ... )
    """
    if data_dir is None:
        data_dir = DEFAULT_DATA_DIR
    
    # If caching is disabled, use the original implementation
    if not use_cache:
        lf = load_street_df(state=state, data_dir=data_dir, filter_to_types=filter_to_types)
        mask = has_state_name_mask()
        lf = lf.filter(mask)
        
        if exclude_numbered:
            lf = lf.filter(~pl.col("street_name").str.contains(r"\d", literal=False))
        
        return lf
    
    # Use caching
    cache = FileCache(cache_dir=DEFAULT_CACHE_DIR)
    
    # Build cache parameters
    params = {
        "state": state if isinstance(state, str) else (tuple(sorted(state)) if state else None),
        "data_dir": str(data_dir),
        "filter_to_types": tuple(filter_to_types) if filter_to_types else None,
        "exclude_numbered": exclude_numbered,
    }
    
    # Get list of source parquet files as dependencies
    dependencies = _get_parquet_paths(state, data_dir)
    
    # Define the computation function
    def compute():
        lf = load_street_df(state=state, data_dir=data_dir, filter_to_types=filter_to_types)
        mask = has_state_name_mask()
        lf = lf.filter(mask)
        
        if exclude_numbered:
            lf = lf.filter(~pl.col("street_name").str.contains(r"\d", literal=False))
        
        # Collect to DataFrame for caching
        return lf.collect()
    
    # Get or compute the result
    df = cache.get_or_compute(
        key="state_streets",
        params=params,
        dependencies=dependencies,
        compute_fn=compute,
    )
    
    # Return as LazyFrame for consistency with the API
    return df.lazy()