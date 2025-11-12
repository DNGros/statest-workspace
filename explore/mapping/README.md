# State Rankings Map

Interactive choropleth map visualizing state ego/humble rankings based on street naming patterns.

## What it does

Creates an interactive US map colored by average rank (lower = more egotistical), showing:
- **In-State %**: Percentage of streets with a state name that are in that state
- **State Fraction**: Fraction of all streets in a state named after that state
- **Self-Named Fraction**: Fraction of state-named streets in a state that are self-named

## Usage

```bash
conda run -n statest python -m workspace.explore.mapping.state_rankings_map
```

Or with custom output path:
```bash
conda run -n statest python -m workspace.explore.mapping.state_rankings_map --output path/to/output.html
```

## Output

- Interactive HTML map saved to `workspace/main_outputs/mapping/state_rankings_map.html`
- Hover over states to see detailed metrics
- Clean, minimal UI with no toolbars or legends

## Color Scale

- **Gold (#C89F3F)**: Most egotistical (low average rank)
- **Teal (#5FAF8A)**: Most humble (high average rank)

Lower average rank = state names more streets after itself relative to other states.

## Caching

The combined metrics calculation is cached for faster subsequent runs:
- First run: Computes all metrics and caches the result
- Subsequent runs: Uses cached data (much faster!)
- Cache automatically invalidates when source data changes

The cache is shared with the `combined_metrics_table.py` script.

