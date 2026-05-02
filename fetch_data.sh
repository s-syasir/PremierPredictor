#!/usr/bin/env bash
# PremierPredictor — fetch all data (current season + historical pipeline)
# Usage:
#   ./fetch_data.sh           — fetch/update everything
#   ./fetch_data.sh --force   — clear cached data and re-fetch from scratch

set -euo pipefail
bash "$(dirname "$0")/scripts/fetch_all.sh" "$@"
