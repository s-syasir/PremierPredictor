#!/usr/bin/env bash
# Recomputes form/standings enrichment and merges xG into the master CSV.
# Run this after fetch_seasons.sh and fetch_xg.sh.
# Usage: ./scripts/fetch_form.sh

set -euo pipefail
cd "$(dirname "$0")/.."

python3 - <<'PYEOF'
import sys
sys.path.insert(0, 'scripts')
import pandas as pd
from fetch_data import compute_form_and_standings, merge_xg
from pathlib import Path

DATA_DIR = Path('data')

print('[1] Loading all seasons...')
fd = pd.read_csv(DATA_DIR / 'all_seasons.csv', parse_dates=['Date'])
fd = fd.dropna(subset=['HomeTeam','AwayTeam','FTHG','FTAG'])

print('[2] Computing form & standings at time of match...')
enriched = compute_form_and_standings(fd)
enriched.to_csv(DATA_DIR / 'fd_enriched.csv', index=False)
print(f'  → {len(enriched)} rows saved to data/fd_enriched.csv')

print('[3] Merging xG...')
xg = pd.read_csv(DATA_DIR / 'understat_matches_all.csv')
final = merge_xg(enriched, xg)
final.to_csv(DATA_DIR / 'matches_full.csv', index=False)
print(f'  → {len(final.columns)} columns, {len(final)} rows saved to data/matches_full.csv')

print('Done.')
PYEOF
