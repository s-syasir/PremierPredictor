#!/usr/bin/env bash
# Full data pipeline: current season → historical seasons → xG → form/merge → player impact
# Usage: ./scripts/fetch_all.sh [--force]
#   --force  deletes all cached data and re-fetches everything

set -euo pipefail

FORCE=0
[[ "${1:-}" == "--force" ]] && FORCE=1

SCRIPTS_DIR="$(dirname "$0")"
cd "$SCRIPTS_DIR/.."

if [[ $FORCE -eq 1 ]]; then
    echo "Force mode: clearing data/ directory..."
    rm -rf data/
    mkdir -p data/
fi

echo "========================================="
echo " PremierPredictor — full data pipeline"
echo "========================================="
echo ""

echo "[0/5] Current season data (auto-archives past seasons if past June)..."
bash "$SCRIPTS_DIR/fetch_current_season.sh"
echo ""

echo "[1/5] Downloading historical season CSVs..."
bash "$SCRIPTS_DIR/fetch_seasons.sh"
echo ""

echo "[2/5] Stacking historical seasons into all_seasons.csv..."
python3 - <<'PYEOF'
import sys, pandas as pd
from pathlib import Path
sys.path.insert(0, 'scripts')
DATA = Path('data')
SEASONS = {'2019-20':'1920','2020-21':'2021','2021-22':'2122',
           '2022-23':'2223','2023-24':'2324','2024-25':'2425'}
frames = []
for label, code in SEASONS.items():
    df = pd.read_csv(DATA/f'E0_{code}.csv', encoding='latin1', on_bad_lines='skip')
    df['Season'] = label
    frames.append(df)
out = pd.concat(frames, ignore_index=True)
out['Date'] = pd.to_datetime(out['Date'], dayfirst=True, errors='coerce')
out = out.dropna(subset=['Date','HomeTeam','AwayTeam','FTHG','FTAG'])
out = out.sort_values('Date').reset_index(drop=True)
out.to_csv(DATA/'all_seasons.csv', index=False)
print(f'  → {len(out)} matches saved')
PYEOF
echo ""

echo "[3/5] Fetching Understat xG + player stats (historical)..."
bash "$SCRIPTS_DIR/fetch_xg.sh"
echo ""

echo "[4/5] Computing form, standings, merging xG, player impact..."
python3 - <<'PYEOF'
import sys, pandas as pd
sys.path.insert(0, 'scripts')
from fetch_data import compute_form_and_standings, merge_xg, player_impact, squad_depth
from pathlib import Path
DATA = Path('data')

fd      = pd.read_csv(DATA/'all_seasons.csv', parse_dates=['Date'])
fd      = fd.dropna(subset=['HomeTeam','AwayTeam','FTHG','FTAG'])
enriched = compute_form_and_standings(fd)
enriched.to_csv(DATA/'fd_enriched.csv', index=False)

xg = pd.read_csv(DATA/'understat_matches_all.csv')
final = merge_xg(enriched, xg)
final.to_csv(DATA/'matches_full.csv', index=False)

players = pd.read_csv(DATA/'understat_players_all.csv')
player_impact(players).to_csv(DATA/'player_impact.csv', index=False)
squad_depth(players).to_csv(DATA/'squad_depth.csv', index=False)

print(f'  matches_full:   {len(final)} rows × {len(final.columns)} cols')
print(f'  player_impact:  {len(player_impact(players))} players')
PYEOF

echo ""
echo "[5/5] Championship data & promoted teams..."
python3 - <<'PYEOF'
import sys
sys.path.insert(0, 'scripts')
from fetch_data import fetch_promoted_teams
fetch_promoted_teams()
PYEOF

echo ""
echo "========================================="
echo " All done. data/ contents:"
echo "========================================="
ls -lh data/*.csv | awk '{print "  "$NF, $5}'
