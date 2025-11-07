#!/usr/bin/env python3
"""
NHL scraper (NEW API ONLY) — 2025-26 season
- Standings + teams:  https://api-web.nhle.com/v1/standings/now
- Schedule by day:    https://api-web.nhle.com/v1/schedule/YYYY-MM-DD
- Gamecenter payloads:
    landing:          https://api-web.nhle.com/v1/gamecenter/{gamePk}/landing
    play-by-play:     https://api-web.nhle.com/v1/gamecenter/{gamePk}/play-by-play
    boxscore:         https://api-web.nhle.com/v1/gamecenter/{gamePk}/boxscore
- Shift charts:       https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={gamePk}

Writes (atomic):
- data/raw/YYYY-MM-DD/standings_now.json
- data/raw/YYYY-MM-DD/games/{gamePk}/landing.json
- data/raw/YYYY-MM-DD/games/{gamePk}/pbp.json
- data/raw/YYYY-MM-DD/games/{gamePk}/boxscore.json
- data/raw/YYYY-MM-DD/games/{gamePk}/shifts.json

CSV quick views:
- data/csv/YYYY-MM-DD/standings.csv
- data/csv/YYYY-MM-DD/teams.csv      (derived from standings)
- data/csv/YYYY-MM-DD/schedule.csv   (from daily sweep)
- data/csv/YYYY-MM-DD/players.csv    (derived from boxscores encountered)
"""

import os
import json
import time
from datetime import date, datetime, timedelta
from typing import Dict, Any, List, Optional

import pandas as pd
import requests

API_WEB = "https://api-web.nhle.com/v1"
API_STATS_REST = "https://api.nhle.com/stats/rest/en"

USER_AGENT = "nhl-newapi-scraper/1.1 (+github actions)"
TIMEOUT = 45
RETRIES = 3
BACKOFF = 1.5

def season_start_for_2025_26() -> date:
    return date(2025, 9, 1)

def today_str() -> str:
    return date.today().strftime("%Y-%m-%d")

# ----------------- IO helpers (atomic) -----------------
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def atomic_write_bytes(path: str, data: bytes):
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, path)

def dump_json_atomic(obj: Any, path: str):
    data = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
    atomic_write_bytes(path, data)

def dump_csv_atomic(df: pd.DataFrame, path: str):
    tmp = path + ".tmp"
    df.to_csv(tmp, index=False)
    os.replace(tmp, path)

# ----------------- HTTP helper -----------------
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})

def jget(url: str, params: Optional[Dict[str, Any]] = None,
         retries: int = RETRIES, sleep: float = BACKOFF) -> Optional[Dict[str, Any]]:
    for i in range(retries):
        try:
            r = SESSION.get(url, params=params, timeout=TIMEOUT)
            if r.status_code >= 500:
                raise requests.HTTPError(f"{r.status_code} on {url}")
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if i == retries - 1:
                print(f"[WARN] Failed {url}: {e}")
                return None
            time.sleep(sleep * (2 ** i))
    return None

# ----------------- Main -----------------
def main():
    run_date = today_str()
    raw_dir = os.path.join("data", "raw", run_date)
    csv_dir = os.path.join("data", "csv", run_date)
    ensure_dir(raw_dir)
    ensure_dir(csv_dir)

    # ---------- 0) Standings (teams metadata) ----------
    print("Fetching standings/now …")
    standings = jget(f"{API_WEB}/standings/now")
    rows: List[Dict[str, Any]] = []
    if standings:
        dump_json_atomic(standings, os.path.join(raw_dir, "standings_now.json"))
        rows = standings.get("standings", [])
        if rows:
            dump_csv_atomic(pd.json_normalize(rows, max_level=1), os.path.join(csv_dir, "standings.csv"))
        # derive teams list
        team_rows: List[Dict[str, Any]] = []
        for row in rows:
            team_rows.append({
                "teamId": row.get("teamId"),
                "name": row.get("teamName"),
                "triCode": row.get("teamAbbrev"),
                "conference": row.get("conferenceName"),
                "division": row.get("divisionName"),
            })
        if team_rows:
            teams_df = pd.DataFrame(team_rows).drop_duplicates("teamId").sort_values("teamId")
            dump_csv_atomic(teams_df, os.path.join(csv_dir, "teams.csv"))
    else:
        print("[WARN] standings/now unavailable; continuing…")

    # ---------- 1) Schedule sweep (date-by-date) ----------
    start_d = season_start_for_2025_26()
    end_d = date.today()
    print(f"Fetching schedule day-by-day: {start_d} → {end_d} …")
    game_rows: List[Dict[str, Any]] = []
    game_pks: List[int] = []
    cur = start_d
    while cur <= end_d:
        ymd = cur.strftime("%Y-%m-%d")
        sched = jget(f"{API_WEB}/schedule/{ymd}")
        if sched and isinstance(sched.get("gameWeek"), list):
            for wk in sched["gameWeek"]:
                for g in wk.get("games", []):
                    gpk = g.get("id") or g.get("gameId") or g.get("gamePk")
                    if not gpk:
                        continue
                    gpk = int(gpk)
                    if gpk not in game_pks:
                        game_pks.append(gpk)
                    game_rows.append({
                        "gamePk": gpk,
                        "gameDateUTC": g.get("startTimeUTC") or g.get("startTime"),
                        "homeId": (g.get("homeTeam", {}) or {}).get("id"),
                        "homeTri": (g.get("homeTeam", {}) or {}).get("abbrev"),
                        "awayId": (g.get("awayTeam", {}) or {}).get("id"),
                        "awayTri": (g.get("awayTeam", {}) or {}).get("abbrev"),
                        "venue": (g.get("venue", {}) or {}).get("default") or g.get("venueName"),
                        "gameState": g.get("gameState"),
                    })
        time.sleep(0.10)
        cur += timedelta(days=1)

    if game_rows:
        sched_df = pd.DataFrame(game_rows).drop_duplicates("gamePk")
        dump_csv_atomic(sched_df, os.path.join(csv_dir, "schedule.csv"))
    print(f"Total unique games discovered: {len(game_pks)}")

    # ---------- 2) Per-game payloads (per-game subfolders, atomic) ----------
    players_seen: Dict[int, Dict[str, Any]] = {}

    for idx, gpk in enumerate(game_pks, start=1):
        if idx % 50 == 0:
            print(f"…{idx} games processed")

        gdir = os.path.join(raw_dir, "games", str(gpk))
        ensure_dir(gdir)

        # landing
        landing = jget(f"{API_WEB}/gamecenter/{gpk}/landing")
        if landing:
            dump_json_atomic(landing, os.path.join(gdir, "landing.json"))

        # play-by-play
        pbp = jget(f"{API_WEB}/gamecenter/{gpk}/play-by-play")
        if pbp:
            dump_json_atomic(pbp, os.path.join(gdir, "pbp.json"))

        # boxscore (also harvest basic player fields)
        box = jget(f"{API_WEB}/gamecenter/{gpk}/boxscore")
        if box:
            dump_json_atomic(box, os.path.join(gdir, "boxscore.json"))
            try:
                def harvest(side: Dict[str, Any], team_id: Optional[int]):
                    players = (side or {}).get("players") or (side or {}).get("skaters") or []
                    if isinstance(players, dict):
                        players = list(players.values())
                    for p in players:
                        pid = p.get("playerId") or p.get("id")
                        if not pid:
                            continue
                        pid = int(pid)
                        rec = players_seen.get(pid, {})
                        rec.update({
                            "personId": pid,
                            "fullName": p.get("name", {}).get("default") or p.get("name"),
                            "teamId": team_id,
                            "sweaterNumber": p.get("sweaterNumber") or p.get("jerseyNumber"),
                            "position": (p.get("position") or {}).get("abbrev") or p.get("position"),
                            "shootsCatches": p.get("shootsCatches") or p.get("shoots") or p.get("catches"),
                            "height": p.get("height"),
                            "weight": p.get("weight"),
                        })
                        players_seen[pid] = rec

                teams_blk = (box or {}).get("teams") or {}
                home_blk = teams_blk.get("home") or {}
                away_blk = teams_blk.get("away") or {}
                home_id = home_blk.get("id") if isinstance(home_blk.get("id"), int) else None
                away_id = away_blk.get("id") if isinstance(away_blk.get("id"), int) else None
                harvest(home_blk, home_id)
                harvest(away_blk, away_id)
            except Exception as e:
                print(f"[WARN] Could not harvest players from boxscore {gpk}: {e}")

        # shift charts
        shifts = jget(f"{API_STATS_REST}/shiftcharts", params={"cayenneExp": f"gameId={gpk}"})
        if shifts:
            dump_json_atomic(shifts, os.path.join(gdir, "shifts.json"))

        time.sleep(0.12)

    # ---------- 3) Players CSV ----------
    if players_seen:
        players_df = pd.DataFrame(list(players_seen.values())).drop_duplicates("personId", keep="last")
        dump_csv_atomic(players_df, os.path.join(csv_dir, "players.csv"))

    print("Done.")

if __name__ == "__main__":
    main()
