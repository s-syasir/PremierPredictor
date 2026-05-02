"""
PremierPredictor data collection pipeline
Fetches: 6 seasons of match data, Understat xG (match + player), form, standings, squad depth
"""

import json
import shutil
import time
from datetime import date
from pathlib import Path

import pandas as pd
import requests

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)",
    "Referer":    "https://understat.com/league/EPL/2024",
    "X-Requested-With": "XMLHttpRequest",
}

# ── 0 · Current season detection & archiving ─────────────────────────────────

META_PATH = DATA_DIR / "current_season_meta.json"

def current_season_info():
    """
    Returns (label, fd_code, understat_year) for the active PL season.
    PL season runs Aug–May. If month < 6 we're still in last year's season.
    Examples (run in Jan 2026): ("2025-26", "2526", 2025)
    Examples (run in Sep 2026): ("2026-27", "2627", 2026)
    """
    today = date.today()
    year_start = today.year if today.month >= 6 else today.year - 1
    year_end   = year_start + 1
    label      = f"{year_start}-{str(year_end)[2:]}"
    code       = f"{str(year_start)[2:]}{str(year_end)[2:]}"
    return label, code, year_start


def _read_meta():
    if META_PATH.exists():
        return json.loads(META_PATH.read_text())
    return {}


def _write_meta(label, code):
    META_PATH.write_text(json.dumps({
        "season_label": label,
        "season_code":  code,
        "last_updated": date.today().isoformat(),
    }, indent=2))


def maybe_archive_season():
    """
    If the season stored in current_season_meta.json differs from the currently
    active season (i.e. we've crossed June into a new season), archive
    current_season.csv → data/E0_{old_code}.csv and clear it.
    """
    current_csv = DATA_DIR / "current_season.csv"
    if not current_csv.exists():
        return

    meta = _read_meta()
    if not meta:
        return

    _, active_code, _ = current_season_info()
    stored_code = meta.get("season_code", "")

    if stored_code and stored_code != active_code:
        archive_path = DATA_DIR / f"E0_{stored_code}.csv"
        if not archive_path.exists():
            shutil.copy(current_csv, archive_path)
            print(f"  Archived {stored_code} season → {archive_path.name}")
        current_csv.unlink()
        META_PATH.unlink(missing_ok=True)
        print(f"  Cleared current_season.csv — new season is {active_code}")


def fetch_current_season():
    """
    Fetch current-season data from Football-Data.co.uk + Understat.
    Always re-fetches (the FD file grows each gameweek).
    Saves to data/current_season.csv + data/understat_current_season.csv.
    Also auto-archives the previous season if we've crossed June.
    """
    maybe_archive_season()

    label, code, us_year = current_season_info()
    print(f"  Current season: {label}  (FD code={code})")

    # ── Football-Data ──
    url = f"https://www.football-data.co.uk/mmz4281/{code}/E0.csv"
    print(f"  Fetching FD {label} ...", end=" ", flush=True)
    r = requests.get(url, timeout=30)
    r.raise_for_status()

    raw_path = DATA_DIR / f"E0_{code}.csv"
    raw_path.write_bytes(r.content)

    df = pd.read_csv(raw_path, encoding="latin1", on_bad_lines="skip").copy()
    df["Season"] = label
    df["Date"]   = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG"])
    df = df.sort_values("Date").reset_index(drop=True)
    df.to_csv(DATA_DIR / "current_season.csv", index=False)
    print(f"done  ({len(df)} matches played so far)")

    # ── Understat ──
    us_path = DATA_DIR / f"understat_matches_{label}.csv"
    print(f"  Fetching Understat {label} ...", end=" ", flush=True)
    r2 = requests.get(
        f"https://understat.com/getLeagueData/EPL/{us_year}",
        headers=HEADERS, timeout=30,
    )
    r2.raise_for_status()
    data = r2.json()

    match_rows = []
    for m in data.get("dates", []):
        if not m.get("isResult"):
            continue
        match_rows.append({
            "understat_id": m["id"],
            "season":       label,
            "date":         m["datetime"][:10],
            "home_team":    m["h"]["title"],
            "away_team":    m["a"]["title"],
            "home_goals":   int(m["goals"]["h"]),
            "away_goals":   int(m["goals"]["a"]),
            "home_xg":      float(m["xG"]["h"]),
            "away_xg":      float(m["xG"]["a"]),
            "home_win_prob":float(m["forecast"]["w"]),
            "draw_prob":    float(m["forecast"]["d"]),
            "away_win_prob":float(m["forecast"]["l"]),
        })

    mdf = pd.DataFrame(match_rows)
    mdf.to_csv(us_path, index=False)
    mdf.to_csv(DATA_DIR / "understat_current_season.csv", index=False)
    print(f"done  ({len(mdf)} matches with xG)")

    # ── Championship (for next season's promoted teams) ──
    next_code = f"{int(code[:2])+1:02d}{int(code[2:])+1:02d}"
    champ_url = f"https://www.football-data.co.uk/mmz4281/{code}/E1.csv"
    print(f"  Fetching Championship {label} ...", end=" ", flush=True)
    try:
        r3 = requests.get(champ_url, timeout=30)
        r3.raise_for_status()
        champ_path = DATA_DIR / f"E1_{code}.csv"
        champ_path.write_bytes(r3.content)
        cdf = pd.read_csv(champ_path, encoding="latin1", on_bad_lines="skip")
        cdf["Date"] = pd.to_datetime(cdf["Date"], errors="coerce")
        cdf = cdf.dropna(subset=["Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG"])
        cdf["FTHG"] = cdf["FTHG"].astype(int)
        cdf["FTAG"]  = cdf["FTAG"].astype(int)
        champ_table = _build_champ_table(cdf.sort_values("Date").reset_index(drop=True))
        champ_table.to_csv(DATA_DIR / "championship_table.csv")

        auto_promoted = champ_table.head(2)["Team"].tolist()
        playoff_spots = champ_table.iloc[2:6]["Team"].tolist()
        playoff_winner = _detect_playoff_winner(auto_promoted, code, next_code)
        rows = [{"team": t, "method": "auto_promoted", "champ_position": i+1}
                for i, t in enumerate(auto_promoted)]
        if playoff_winner:
            pos = int(champ_table[champ_table["Team"] == playoff_winner].index[0])
            rows.append({"team": playoff_winner, "method": "playoff_winner", "champ_position": pos})
        else:
            rows += [{"team": t, "method": "playoff_candidate",
                      "champ_position": int(champ_table[champ_table["Team"] == t].index[0])}
                     for t in playoff_spots]
        pd.DataFrame(rows).to_csv(DATA_DIR / "promoted_teams.csv", index=False)
        print(f"done  ({len(cdf)} Championship matches)")
    except Exception as e:
        print(f"skipped ({e})")

    _write_meta(label, code)
    return df, mdf


# ── 1 · Historical seasons from Football-Data.co.uk ─────────────────────────

SEASONS = {
    "2019-20": "1920",
    "2020-21": "2021",
    "2021-22": "2122",
    "2022-23": "2223",
    "2023-24": "2324",
    "2024-25": "2425",
}

def fetch_fd_seasons():
    frames = []
    for label, code in SEASONS.items():
        path = DATA_DIR / f"E0_{code}.csv"
        if not path.exists():
            url = f"https://www.football-data.co.uk/mmz4281/{code}/E0.csv"
            print(f"  Downloading {label} …", end=" ", flush=True)
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            path.write_bytes(r.content)
            print("done")
            time.sleep(1)
        else:
            print(f"  {label} already cached")
        df = pd.read_csv(path, encoding="latin1", on_bad_lines="skip")
        df["Season"] = label
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    combined["Date"] = pd.to_datetime(combined["Date"], dayfirst=True, errors="coerce")
    combined = combined.dropna(subset=["Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG"])
    combined = combined.sort_values("Date").reset_index(drop=True)
    out = DATA_DIR / "all_seasons.csv"
    combined.to_csv(out, index=False)
    print(f"  → {len(combined)} matches saved to {out}")
    return combined


# ── 2 · Form & standings at time of each match ───────────────────────────────

def compute_form_and_standings(matches: pd.DataFrame, form_window: int = 5) -> pd.DataFrame:
    """
    For each match, compute standings/form from results in that season only.
    Adds: pts_before, gd_before, form (pts from last N games), rank_before.
    """
    matches = matches.sort_values(["Season", "Date"]).reset_index(drop=True)
    rows = []

    for season, grp in matches.groupby("Season", sort=False):
        teams = sorted(set(grp["HomeTeam"]) | set(grp["AwayTeam"]))
        pts   = {t: 0 for t in teams}
        gd    = {t: 0 for t in teams}
        form  = {t: [] for t in teams}

        for _, row in grp.iterrows():
            h, a = row["HomeTeam"], row["AwayTeam"]
            ranked = sorted(teams, key=lambda t: (-pts[t], -gd[t]))
            rank   = {t: i + 1 for i, t in enumerate(ranked)}

            rows.append({
                **row.to_dict(),
                "home_pts_before":   pts[h],
                "away_pts_before":   pts[a],
                "home_gd_before":    gd[h],
                "away_gd_before":    gd[a],
                "home_form5":        sum(form[h][-form_window:]),
                "away_form5":        sum(form[a][-form_window:]),
                "home_rank_before":  rank[h],
                "away_rank_before":  rank[a],
            })

            hg, ag = int(row["FTHG"]), int(row["FTAG"])
            if   hg > ag: hp, ap = 3, 0
            elif hg == ag: hp = ap = 1
            else:          hp, ap = 0, 3

            pts[h] += hp;  pts[a] += ap
            gd[h]  += hg - ag;  gd[a] += ag - hg
            form[h].append(hp);  form[a].append(ap)

    return pd.DataFrame(rows)


# ── 3 · Understat xG ─────────────────────────────────────────────────────────

UNDERSTAT_YEARS = [2019, 2020, 2021, 2022, 2023, 2024]

TEAM_MAP = {
    "Manchester United":      "Man United",
    "Manchester City":        "Man City",
    "Newcastle United":       "Newcastle",
    "Nottingham Forest":      "Nott'm Forest",
    "Wolverhampton Wanderers":"Wolves",
    "Leicester":              "Leicester",
    "West Ham":               "West Ham",
    "Leeds":                  "Leeds",
    "Sheffield United":       "Sheffield United",
    "Luton":                  "Luton",
}

def fetch_understat_season(year: int):
    season_label = f"{year}-{str(year+1)[-2:]}"
    match_path  = DATA_DIR / f"understat_matches_{season_label}.csv"
    player_path = DATA_DIR / f"understat_players_{season_label}.csv"

    if match_path.exists() and player_path.exists():
        print(f"  {season_label} already cached")
        return pd.read_csv(match_path), pd.read_csv(player_path)

    print(f"  Fetching Understat {season_label} …", end=" ", flush=True)
    r = requests.get(
        f"https://understat.com/getLeagueData/EPL/{year}",
        headers=HEADERS, timeout=30,
    )
    r.raise_for_status()
    data = r.json()

    match_rows = []
    for m in data.get("dates", []):
        if not m.get("isResult"):
            continue
        match_rows.append({
            "understat_id": m["id"],
            "season":       season_label,
            "date":         m["datetime"][:10],
            "home_team":    m["h"]["title"],
            "away_team":    m["a"]["title"],
            "home_goals":   int(m["goals"]["h"]),
            "away_goals":   int(m["goals"]["a"]),
            "home_xg":      float(m["xG"]["h"]),
            "away_xg":      float(m["xG"]["a"]),
            "home_win_prob":float(m["forecast"]["w"]),
            "draw_prob":    float(m["forecast"]["d"]),
            "away_win_prob":float(m["forecast"]["l"]),
        })

    player_rows = []
    for p in data.get("players", []):
        player_rows.append({
            "season":        season_label,
            "player_id":     p["id"],
            "player":        p["player_name"],
            "team":          p["team_title"],
            "position":      p["position"],
            "games":         int(p["games"]),
            "minutes":       int(p["time"]),
            "goals":         int(p["goals"]),
            "assists":       int(p["assists"]),
            "shots":         int(p["shots"]),
            "key_passes":    int(p["key_passes"]),
            "yellow_cards":  int(p["yellow_cards"]),
            "red_cards":     int(p["red_cards"]),
            "xg":            float(p["xG"]),
            "xa":            float(p["xA"]),
            "npg":           int(p["npg"]),
            "npxg":          float(p["npxG"]),
            "xg_chain":      float(p["xGChain"]),
            "xg_buildup":    float(p["xGBuildup"]),
        })

    mdf = pd.DataFrame(match_rows)
    pdf = pd.DataFrame(player_rows)
    mdf.to_csv(match_path,  index=False)
    pdf.to_csv(player_path, index=False)
    print(f"{len(mdf)} matches, {len(pdf)} players")
    return mdf, pdf


def fetch_all_understat():
    all_matches, all_players = [], []
    for yr in UNDERSTAT_YEARS:
        mdf, pdf = fetch_understat_season(yr)
        all_matches.append(mdf)
        all_players.append(pdf)
        time.sleep(1)

    matches_all = pd.concat(all_matches, ignore_index=True)
    players_all = pd.concat(all_players, ignore_index=True)
    matches_all.to_csv(DATA_DIR / "understat_matches_all.csv", index=False)
    players_all.to_csv(DATA_DIR / "understat_players_all.csv", index=False)
    print(f"  → {len(matches_all)} xG match records, {len(players_all)} player-season records")
    return matches_all, players_all


# ── 4 · Merge FD + xG ────────────────────────────────────────────────────────

def merge_xg(fd: pd.DataFrame, xg: pd.DataFrame) -> pd.DataFrame:
    xg = xg.copy()
    xg["home_team"] = xg["home_team"].replace(TEAM_MAP)
    xg["away_team"] = xg["away_team"].replace(TEAM_MAP)
    xg["_date"] = pd.to_datetime(xg["date"])

    fd = fd.copy()
    fd["_date"] = pd.to_datetime(fd["Date"])

    merged = fd.merge(
        xg[["_date","home_team","away_team","home_xg","away_xg",
            "home_win_prob","draw_prob","away_win_prob","understat_id"]],
        left_on=["_date","HomeTeam","AwayTeam"],
        right_on=["_date","home_team","away_team"],
        how="left",
    ).drop(columns=["home_team","away_team"])

    n = merged["home_xg"].notna().sum()
    print(f"  xG matched: {n}/{len(merged)} ({n/len(merged)*100:.0f}%)")
    return merged


# ── 5 · Player impact analysis ───────────────────────────────────────────────

def player_impact(players: pd.DataFrame) -> pd.DataFrame:
    df = players.copy()
    for col in ["minutes","goals","assists","shots","key_passes","xg","xa","npxg","xg_chain","xg_buildup"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    career = (
        df.groupby(["player_id","player"])
        .agg(
            teams        =("team",    lambda x: ", ".join(sorted(x.unique()))),
            seasons      =("season",  "nunique"),
            games        =("games",   "sum"),
            minutes      =("minutes", "sum"),
            goals        =("goals",   "sum"),
            assists      =("assists", "sum"),
            shots        =("shots",   "sum"),
            key_passes   =("key_passes","sum"),
            xg           =("xg",      "sum"),
            xa           =("xa",      "sum"),
            npxg         =("npxg",    "sum"),
            xg_chain     =("xg_chain","sum"),
            xg_buildup   =("xg_buildup","sum"),
        )
        .reset_index()
    )

    p90 = career["minutes"] / 90
    career["xg_p90"]           = (career["xg"]      / p90).round(3)
    career["xa_p90"]           = (career["xa"]      / p90).round(3)
    career["xgchain_p90"]      = (career["xg_chain"]/ p90).round(3)
    career["goal_contrib_p90"] = ((career["goals"] + career["assists"]) / p90).round(3)
    career["shots_p90"]        = (career["shots"]   / p90).round(3)

    return career.sort_values("xg", ascending=False).reset_index(drop=True)


def squad_depth(players: pd.DataFrame) -> pd.DataFrame:
    df = players.copy()
    df["minutes"] = pd.to_numeric(df["minutes"], errors="coerce").fillna(0)

    df["role"] = "fringe"
    df.loc[df["minutes"] >= 1500, "role"] = "rotation"
    df.loc[df["minutes"] >= 2250, "role"] = "first_choice"

    depth = (
        df.groupby(["season","team"])
        .agg(
            total_players =("player", "count"),
            first_choice  =("role",   lambda x: (x=="first_choice").sum()),
            rotation      =("role",   lambda x: (x=="rotation").sum()),
            fringe        =("role",   lambda x: (x=="fringe").sum()),
            squad_xg      =("xg",     "sum"),
            top_scorer_xg =("xg",     "max"),
        )
        .reset_index()
    )
    depth["xg_concentration"] = (depth["top_scorer_xg"] / depth["squad_xg"]).round(3)

    return depth.sort_values(["season","xg_concentration"], ascending=[True, False])


# ── 7 · Championship data & promoted teams ───────────────────────────────────

_CURR_CODE = list(SEASONS.values())[-1]
_yr        = int(_CURR_CODE[:2])
_NEXT_CODE = f"{_yr + 1:02d}{_yr + 2:02d}"


def fetch_championship_season(season_code: str = _CURR_CODE) -> pd.DataFrame:
    path = DATA_DIR / f"E1_{season_code}.csv"
    if not path.exists():
        url = f"https://www.football-data.co.uk/mmz4281/{season_code}/E1.csv"
        print(f"  Downloading Championship {season_code} …", end=" ", flush=True)
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        path.write_bytes(r.content)
        print("done")
        time.sleep(1)
    else:
        print(f"  Championship {season_code} already cached")

    df = pd.read_csv(path, encoding="latin1", on_bad_lines="skip")
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG"])
    df["FTHG"] = df["FTHG"].astype(int)
    df["FTAG"]  = df["FTAG"].astype(int)
    return df.sort_values("Date").reset_index(drop=True)


def _build_champ_table(df: pd.DataFrame) -> pd.DataFrame:
    teams = sorted(set(df["HomeTeam"].tolist() + df["AwayTeam"].tolist()))
    pts = {t: 0 for t in teams}
    gd  = {t: 0 for t in teams}
    gf  = {t: 0 for t in teams}
    for _, row in df.iterrows():
        h, a, hg, ag = row.HomeTeam, row.AwayTeam, row.FTHG, row.FTAG
        gf[h] += hg; gf[a] += ag
        gd[h] += hg - ag; gd[a] += ag - hg
        if hg > ag:    pts[h] += 3
        elif hg == ag: pts[h] += 1; pts[a] += 1
        else:          pts[a] += 3
    table = pd.DataFrame({
        "Team": teams,
        "Pts":  [pts[t] for t in teams],
        "GD":   [gd[t]  for t in teams],
        "GF":   [gf[t]  for t in teams],
    }).sort_values(["Pts", "GD", "GF"], ascending=False).reset_index(drop=True)
    table.index += 1
    return table


def _detect_playoff_winner(
    auto_promoted: list,
    curr_pl_code: str,
    next_pl_code: str,
) -> str | None:
    import io as _io
    try:
        next_url = f"https://www.football-data.co.uk/mmz4281/{next_pl_code}/E0.csv"
        r = requests.get(next_url, timeout=15)
        r.raise_for_status()
        next_teams = set(
            pd.read_csv(_io.BytesIO(r.content), encoding="latin1", on_bad_lines="skip")
            ["HomeTeam"].dropna().unique()
        )
        curr_teams = set(
            pd.read_csv(DATA_DIR / f"E0_{curr_pl_code}.csv", encoding="latin1", on_bad_lines="skip")
            ["HomeTeam"].dropna().unique()
        )
        new_teams = next_teams - curr_teams - set(auto_promoted)
        if len(new_teams) == 1:
            winner = new_teams.pop()
            print(f"  Playoff winner auto-detected: {winner}")
            return winner
        print(f"  Unexpected diff size ({len(new_teams)}): {new_teams} — cannot auto-detect")
    except Exception as e:
        print(f"  Next-season data unavailable ({e}); playoff winner unknown")
    return None


def fetch_promoted_teams(
    champ_code: str   = _CURR_CODE,
    curr_pl_code: str = _CURR_CODE,
    next_pl_code: str = _NEXT_CODE,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    champ_df = fetch_championship_season(champ_code)
    table    = _build_champ_table(champ_df)

    auto_promoted = table.head(2)["Team"].tolist()
    playoff_spots = table.iloc[2:6]["Team"].tolist()

    playoff_winner = _detect_playoff_winner(auto_promoted, curr_pl_code, next_pl_code)

    rows = []
    for i, t in enumerate(auto_promoted, start=1):
        rows.append({"team": t, "method": "auto_promoted", "champ_position": i})
    if playoff_winner:
        pos = int(table[table["Team"] == playoff_winner].index[0]) if playoff_winner in table["Team"].values else None
        rows.append({"team": playoff_winner, "method": "playoff_winner", "champ_position": pos})
    else:
        for t in playoff_spots:
            rows.append({"team": t, "method": "playoff_candidate", "champ_position": int(table[table["Team"] == t].index[0])})

    promoted_df = pd.DataFrame(rows)

    table.to_csv(DATA_DIR / "championship_table.csv")
    promoted_df.to_csv(DATA_DIR / "promoted_teams.csv", index=False)
    print(f"  → data/championship_table.csv  ({len(table)} teams)")
    print(f"  → data/promoted_teams.csv  ({len(promoted_df)} rows)")
    return table, promoted_df


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("PremierPredictor data pipeline")
    print("=" * 60)

    print("\n[0] Current season data")
    fetch_current_season()

    print("\n[1] Football-Data.co.uk — historical seasons")
    fd = fetch_fd_seasons()

    print("\n[2] Form & standings at time of each match")
    fd_enriched = compute_form_and_standings(fd)
    fd_enriched.to_csv(DATA_DIR / "fd_enriched.csv", index=False)
    print(f"  → {len(fd_enriched)} rows saved to data/fd_enriched.csv")

    print("\n[3] Understat — match xG + player stats (historical)")
    xg_matches, xg_players = fetch_all_understat()

    print("\n[4] Merging xG into enriched match data")
    final = merge_xg(fd_enriched, xg_matches)
    final.to_csv(DATA_DIR / "matches_full.csv", index=False)
    print(f"  → {len(final.columns)} columns, {len(final)} rows → data/matches_full.csv")

    print("\n[5] Player impact table")
    impact = player_impact(xg_players)
    impact.to_csv(DATA_DIR / "player_impact.csv", index=False)
    print(f"  → {len(impact)} players → data/player_impact.csv")

    print("\n[6] Squad depth analysis")
    depth = squad_depth(xg_players)
    depth.to_csv(DATA_DIR / "squad_depth.csv", index=False)
    print(f"  → data/squad_depth.csv")

    print("\n[7] Championship & promoted teams")
    champ_table, promoted_df = fetch_promoted_teams()
    confirmed = promoted_df[promoted_df["method"].isin(["auto_promoted", "playoff_winner"])]
    print(f"\n  Promoted to PL ({_yr + 1}-{_yr + 2}):")
    for _, row in confirmed.iterrows():
        tag = "Champions" if row.champ_position == 1 else ("Runners-up" if row.champ_position == 2 else "Playoff winners")
        print(f"    {row.team}  ({tag})")
    if "playoff_candidate" in promoted_df["method"].values:
        candidates = promoted_df[promoted_df["method"] == "playoff_candidate"]["team"].tolist()
        print(f"  Playoff winner unknown — candidates: {candidates}")

    print("\n✓ All done. Files in data/:")
    for f in sorted(DATA_DIR.iterdir()):
        mb = f.stat().st_size / 1024
        print(f"  {f.name:<45} {mb:.0f} KB")


if __name__ == "__main__":
    main()
