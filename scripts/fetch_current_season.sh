#!/usr/bin/env bash
# Fetches current-season data (Football-Data.co.uk + Understat xG).
# Archives the previous season automatically if run past June.
# Safe to re-run any time — always pulls the latest gameweek data.
# Usage: ./scripts/fetch_current_season.sh

set -euo pipefail
cd "$(dirname "$0")/.."

python3 -c "
import sys
sys.path.insert(0, 'scripts')
from fetch_data import fetch_current_season
fetch_current_season()
"
