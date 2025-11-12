# US President Surnames in Street Names

This directory contains an analysis of how often US president surnames appear in street names across the United States.

## Files

- **`presidents.py`** - List of all 40 US president surnames
- **`analyze_president_streets.py`** - Main analysis script that counts occurrences
- **`visualize_results.py`** - Pretty-printed summary of results
- **`query_president_streets.py`** - Interactive query tool for exploring the data
- **`president_streets_overall.csv`** - Counts by president surname
- **`president_streets_by_state.csv`** - Counts by president surname and state

## Usage

### Run the full analysis:
```bash
conda run -n statest python -m workspace.explore.president_surnames.analyze_president_streets
```

This will:
- Load all street data from the US (6+ million streets)
- Filter to streets containing president surnames
- Count occurrences by president and by state
- Save results to CSV files

### View a summary:
```bash
conda run -n statest python -m workspace.explore.president_surnames.visualize_results
```

### Query specific presidents or states:
```bash
# See all states with Washington streets
conda run -n statest python -m workspace.explore.president_surnames.query_president_streets president washington

# See all president streets in California
conda run -n statest python -m workspace.explore.president_surnames.query_president_streets state california

# Compare multiple presidents
conda run -n statest python -m workspace.explore.president_surnames.query_president_streets compare washington lincoln obama trump
```

### Create visualization:
```bash
# Generate interactive HTML plot (works on desktop and mobile)
conda run -n statest python -m workspace.explore.president_surnames.plot_president_streets
```

This creates:
- `president_streets_plot.html` - Interactive Plotly chart (responsive, works on mobile)
- `president_streets_plot.png` - Static PNG image for reference

## Key Findings

From the analysis of 6+ million US streets:

### Top 10 Most Common President Surnames:
1. **Washington** - 9,958 streets (8.49%)
2. **Jackson** - 8,010 streets (6.83%)
3. **Lincoln** - 7,771 streets (6.62%)
4. **Johnson** - 7,239 streets (6.17%)
5. **Wilson** - 6,703 streets (5.71%)
6. **Jefferson** - 6,270 streets (5.34%)
7. **Taylor** - 6,052 streets (5.16%)
8. **Adams** - 5,939 streets (5.06%)
9. **Madison** - 5,857 streets (4.99%)
10. **Grant** - 5,051 streets (4.31%)

### Least Common:
- **Biden** - 7 streets
- **Obama** - 11 streets
- **Trump** - 79 streets
- **Reagan** - 456 streets
- **Nixon** - 476 streets

### Interesting Facts:
- **Total streets with president surnames**: 117,319 (1.9% of all US streets)
- **Founding Fathers** (Washington, Jefferson, Adams, Madison): 28,024 streets (23.9% of president streets)
- **Recent presidents** (Obama, Trump, Biden): Only 97 streets combined
- **States with most president streets**: Illinois (6,359), Texas (5,689), Pennsylvania (5,085)

## Methodology

The analysis:
1. Uses OpenStreetMap data processed into parquet files
2. Filters to main road types (residential, primary, secondary, etc.)
3. Uses case-insensitive regex matching with word boundaries to find president surnames
4. Counts each street only once per location (even if it has multiple segments)

## Notes

- Some surnames like "Washington", "Jackson", and "Lincoln" are very common and may include streets named after other people with those surnames (not just the presidents)
- Recent presidents (Obama, Trump, Biden) have very few streets named after them, likely because:
  - Street naming often happens years/decades after a presidency
  - Many places have policies against naming streets after living people
  - The data reflects current street names, and new streets are added gradually

