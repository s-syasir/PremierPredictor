#!/usr/bin/env bash
# Fetches xG and player stats from Understat for all configured seasons.
# Caches per-season CSVs in data/; skips seasons already on disk.
# Usage: ./scripts/fetch_xg.sh [--force]

set -euo pipefail

FORCE=0
[[ "${1:-}" == "--force" ]] && FORCE=1

cd "$(dirname "$0")/.."

if [[ $FORCE -eq 1 ]]; then
    echo "Force mode: removing cached Understat files..."
    rm -f data/understat_*.csv
fi

python3 - <<'PYEOF'
import sys
sys.path.insert(0, 'scripts')
from fetch_data import fetch_all_understat
fetch_all_understat()
PYEOF
