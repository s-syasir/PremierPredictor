## PremierPredictor data pipeline
## Usage: make <target>

DATA_DIR := data
PYTHON   := python3
SCRIPTS  := scripts

.PHONY: help setup data seasons xg form player promoted refresh clean

help:
	@echo ""
	@echo "  make setup    — install Python dependencies"
	@echo "  make data     — run full pipeline (seasons → xG → form → player stats)"
	@echo "  make seasons  — download/update Football-Data.co.uk CSVs only"
	@echo "  make xg       — fetch/update Understat xG + player stats only"
	@echo "  make form     — recompute form & standings, rebuild matches_full.csv"
	@echo "  make player   — rebuild player_impact.csv and squad_depth.csv"
	@echo "  make promoted — re-fetch Championship data and detect promoted teams"
	@echo "  make refresh  — delete all cached data and re-fetch everything"
	@echo "  make clean    — remove data/ directory"
	@echo ""

setup:
	$(PYTHON) -m pip install --user -r requirements.txt --break-system-packages

data: $(DATA_DIR)
	bash $(SCRIPTS)/fetch_all.sh

seasons: $(DATA_DIR)
	bash $(SCRIPTS)/fetch_seasons.sh

xg: $(DATA_DIR)
	bash $(SCRIPTS)/fetch_xg.sh

form: $(DATA_DIR)/all_seasons.csv $(DATA_DIR)/understat_matches_all.csv
	bash $(SCRIPTS)/fetch_form.sh

player: $(DATA_DIR)/understat_players_all.csv
	$(PYTHON) -c "\
import sys, pandas as pd; \
sys.path.insert(0, 'scripts'); \
from fetch_data import player_impact, squad_depth; \
from pathlib import Path; \
DATA = Path('$(DATA_DIR)'); \
p = pd.read_csv(DATA/'understat_players_all.csv'); \
player_impact(p).to_csv(DATA/'player_impact.csv', index=False); \
squad_depth(p).to_csv(DATA/'squad_depth.csv', index=False); \
print('Rebuilt player_impact.csv and squad_depth.csv')"

promoted: $(DATA_DIR)
	$(PYTHON) -c "import sys; sys.path.insert(0, 'scripts'); from fetch_data import fetch_promoted_teams; fetch_promoted_teams()"

refresh:
	bash $(SCRIPTS)/fetch_all.sh --force

clean:
	rm -rf $(DATA_DIR)

$(DATA_DIR):
	mkdir -p $(DATA_DIR)
