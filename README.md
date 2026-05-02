# PremierPredictor
### Premier League 2025-26 Final Table Projector · Live Season Data

Three models built from scratch in a single Jupyter notebook — no black-box ML.  
Trained on all **2025-26 played matches** (updated each gameweek), then used to simulate  
the **remaining fixtures** 10,000 times and project each team's final position probabilities.

| Layer | Method | What it estimates |
|-------|--------|------------------|
| Team strength | Elo ratings | Relative team quality, updated after every match |
| Goal rates | Dixon–Coles Poisson regression | Expected goals per fixture, fitted on xG |
| Season outcomes | Monte Carlo simulation × 10,000 | Probability distributions over final positions |

---

## Quickstart

```bash
# 1. Install dependencies
make setup

# 2. Fetch all data (current season + historical)
./fetch_data.sh

# 3. Open the notebook
jupyter lab PremierPredictor.ipynb
```

Run all cells top-to-bottom. Charts save automatically to `notebook_outputs/`.  
To refresh with the latest gameweek: `./fetch_data.sh` then re-run the notebook.

---

## Data pipeline

```bash
./fetch_data.sh           # fetch/update everything
./fetch_data.sh --force   # clear cache and re-fetch from scratch
```

Individual targets via Make:

```bash
make data      # full pipeline
make seasons   # historical Football-Data.co.uk CSVs only
make xg        # Understat xG + player stats only
make form      # recompute form/standings
make refresh   # wipe and re-fetch everything
```

| Source | What it provides |
|--------|-----------------|
| [Football-Data.co.uk](https://www.football-data.co.uk) | Match results, Bet365 odds, 2019–present |
| [Understat](https://understat.com) | Expected goals (xG) per match and player |

Key output files in `data/`:

| File | Contents |
|------|----------|
| `current_season.csv` | Live 2025-26 results — updated each run |
| `understat_current_season.csv` | Live 2025-26 xG data |
| `all_seasons.csv` | Stacked 2019-25 historical results |
| `matches_full.csv` | Historical results + form + xG merged |
| `championship_table.csv` | Current Championship table (2026-27 promotion race) |
| `promoted_teams.csv` | Confirmed/candidate promoted teams for next season |

**Auto-archiving:** Run `./fetch_data.sh` past June and `current_season.csv` is automatically  
archived as `data/E0_{code}.csv`; a fresh file starts for the new season.

---

## How the models work

### 1. Elo ratings

A zero-sum relative rating system. After each match the winner gains points from the loser —
scaled by how *surprising* the result was.

```
E_H = 1 / (1 + 10^((R_A - R_H - 100) / 400))   ← home team gets +100 bonus
ΔR  = K × (actual − expected)                    ← K = 32
```

All teams start at 1500. After 339 matches the ratings reflect a full season of results.

### 2. Dixon–Coles Poisson regression

Each team gets three fitted parameters:
- **α (attack)**: scoring rate on log scale
- **β (defence)**: conceding rate on log scale  
- **θ (home advantage)**: per-team stadium bonus

```
λ_H = exp(α_H − β_A + θ_H)
λ_A = exp(α_A − β_H)
```

Fitted on **xG** (not raw goals) via L-BFGS-B over all played 2025-26 matches.

**Four enhancements:**

| Enhancement | What it does |
|------------|-------------|
| **Time decay ξ** | Weights recent matches more; bounded [0, 0.015] to prevent overfitting |
| **Per-team home advantage θ** | One parameter per team — Anfield ≠ Goodison |
| **Adaptive form window** | Window length (3/5/8 games) selected by log-likelihood grid search |
| **Low-score correction ρ** | Dixon–Coles τ function fixes 0-0/1-1 under-prediction |

### 3. Monte Carlo simulation

With 339 matches played and ~41 remaining, we:

1. Take each team's **actual current points + GD** as the starting point
2. Simulate the remaining fixtures using the fitted Poisson parameters
3. Apply ρ rejection sampling for low-score correction
4. Rank by points + GD → one simulated final table
5. Repeat 10,000 times → probability distribution over finishing positions

This is a genuine live projection: the model knows where teams actually stand and only
simulates the uncertainty in the remaining games.

---

## Outputs

All charts saved to `notebook_outputs/`:

| Chart | What it shows |
|-------|---------------|
| `odds_chart.png` | Title / Top-4 / Relegation probabilities for all 20 teams |
| `points_dist.png` | Simulated final points distributions for the top 6 |
| `feature_importance.png` | Dixon–Coles attack & defence rankings |
| `elo_trajectories.png` | Elo rating curves across the full 2025-26 season so far |
| `elo_ratings.png` | Current Elo standings, coloured by league zone |
| `attack_defence.png` | Attack vs defence scatter for all 20 teams |
| `bookie_scatter.png` | Model win probabilities vs devigged Bet365 odds |

---

## Project structure

```
PremierPredictor/
├── PremierPredictor.ipynb          # main notebook
├── fetch_data.sh             # entry point: fetch all data
├── Makefile                  # make setup / data / refresh / clean
├── requirements.txt
├── scripts/
│   ├── fetch_data.py         # all data-fetching functions
│   ├── fetch_all.sh          # full pipeline
│   ├── fetch_current_season.sh  # current season only (Football-Data + Understat + Championship)
│   ├── fetch_seasons.sh      # historical CSVs
│   ├── fetch_xg.sh           # Understat historical xG
│   └── fetch_form.sh         # form/standings recompute
├── data/                     # fetched CSVs — gitignored, regenerated by ./fetch_data.sh
└── notebook_outputs/         # chart PNGs saved by the notebook
```

---

## Championship & next season

The data pipeline also fetches the **2025-26 Championship table** — tracking which teams are on
course for promotion to the 2026-27 Premier League. The notebook's §9 section shows the current
standings and confirmed/candidate promoted teams.

Once the Championship playoff final is played, re-run `./fetch_data.sh` to auto-detect the
playoff winner and update `data/promoted_teams.csv`.
