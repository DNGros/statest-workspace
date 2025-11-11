#!/usr/bin/env python3
"""Check Wisconsin's ranking in state street names."""

import sys
from pathlib import Path
import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from workspace.explore.state_sts.most_common_state_st import count_all_state_names_by_location, load_state_streets_df

lf = load_state_streets_df()
counts = count_all_state_names_by_location(lf)

wisconsin = counts.filter(pl.col('state_name').str.to_lowercase() == 'wisconsin')
print('Wisconsin ranking and counts:')
print(wisconsin)

print('\n' + '='*80)
print('Top 10 states by total count:')
print('='*80)
print(counts.head(10))

