#!/usr/bin/env python3
"""Check Wisconsin's ranking by in-state percentage."""

import sys
from pathlib import Path
import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from workspace.explore.state_sts.most_common_state_st import calculate_in_state_percentage, load_state_streets_df

lf = load_state_streets_df()
percentages = calculate_in_state_percentage(lf)

wisconsin = percentages.filter(pl.col('state_name').str.to_lowercase() == 'wisconsin')
print('Wisconsin ranking by in-state percentage:')
print(wisconsin)

print('\n' + '='*80)
print('Top 10 states by in-state percentage:')
print('='*80)
print(percentages.head(10))

print('\n' + '='*80)
print('Bottom 10 states by in-state percentage:')
print('='*80)
print(percentages.tail(10))

