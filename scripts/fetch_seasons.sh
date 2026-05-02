#!/usr/bin/env bash
# Downloads Premier League CSV files from Football-Data.co.uk
# Usage: ./scripts/fetch_seasons.sh [season_codes...]
# Example: ./scripts/fetch_seasons.sh 2425 2324       (specific seasons)
#          ./scripts/fetch_seasons.sh                 (all 6 default seasons)

set -euo pipefail

BASE_URL="https://www.football-data.co.uk/mmz4281"
DATA_DIR="$(dirname "$0")/../data"
mkdir -p "$DATA_DIR"

# Default: all 6 seasons
if [[ $# -gt 0 ]]; then
    SEASONS=("$@")
else
    SEASONS=(1920 2021 2122 2223 2324 2425)
fi

for CODE in "${SEASONS[@]}"; do
    DEST="$DATA_DIR/E0_${CODE}.csv"
    if [[ -f "$DEST" ]]; then
        echo "  $CODE already cached ($DEST)"
    else
        echo "  Downloading $CODE ..."
        curl -fsSL --retry 3 "${BASE_URL}/${CODE}/E0.csv" -o "$DEST"
        echo "  Saved → $DEST ($(wc -l < "$DEST") rows)"
        sleep 1
    fi
done

echo ""
echo "Done. Files in $DATA_DIR:"
ls -lh "$DATA_DIR"/E0_*.csv
