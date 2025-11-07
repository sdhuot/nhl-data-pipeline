#!/usr/bin/env python3
"""
NHL scraper (NEW API ONLY) — 2025-26 season

Sources (all confirmed from the "new" stack):
- Standings + team metadata: https://api-web.nhle.com/v1/standings/now
- League schedule by day:    https://api-web.nhle.com/v1/schedule/YYYY-MM-DD
- Per-game data:
    - landing:     https://api-web.nhle.com/v1/gamecenter/{gamePk}/landing
    - play-by-play:https://api-web.nhle.com/v1/gamecenter/{gamePk}/play-by-play
    - boxscore:    https://api-web.nhle.com/v1/gamecenter/{gamePk}/boxscore
- Shift charts:    https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={gamePk}

Writes:
- data/raw/YYYY-MM-DD/... (all JSON)
- data/csv/YYYY-MM-DD/standings.csv
- data/csv/YYYY-MM-DD/teams.csv (derived from standings)
- data/csv/YYYY-MM-DD/schedule.csv (from daily schedule sweep)
- data/csv/YYYY-MM-DD/players.csv (derived from boxscores seen so far)

Notes:
- Season is fixed to 2025-09-01 through today. (Adjust window below if desired.)
- No dependencies on statsapi.web.nhl.com. If only the new API works for you, this will run.
"""

import os
import json
import time
from datetime import date, datetime, timedelta
from typing import Dict, Any, List, Optional

import pandas as pd
import requests

# ----------------- Config -----------------
API_WEB = "https://api-web.nhle.com/v1"
API_STATS_REST = "https://api.nhle.com/stats/rest/en"

USER_AGENT = "nhl-newapi-scraper/1.0 (+github actions)"
TIMEOUT = 45
RETRIES = 3
BACKOFF = 1.5

# Season window (fixed to 2025-26 as requested)
def season_start_for_2025_26() -> date:
    return date(2025, 9, 1)

def today_str() -> str:
    return date.today().strftime("%Y-%m-%d")

# ------------- IO Helpers -----------------
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def dump_json(obj: Any, path: str):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

# ------------- HTTP Helper ----------------
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

# ------------- Main -----------------------
def main():
    run_date = today_str()
    raw_dir = os.path.join("data", "raw", run_date)
    csv_dir = os.path.join("data", "csv", run_date)
    ensure_dir(raw_dir)
    ensure_dir(csv_dir)

    # ---------- 0) Standings (teams metadata) ----------
    print("Fetching standings/now …")
    standings = jget(f"{API_WEB}/standings/now")
    if standings:
        dump_json(standings, os.path.join(raw_dir, "standings_now.json"))
        rows = standings.get("standings", [])
        if rows:
            pd.json_normalize(rows, max_level=1).to_csv(
                os.path.join(csv_dir, "standings.csv"), index=False
            )
        # Build a teams table (id, name, triCode) from standings payload
        team_rows: List[Dict[str, Any]] = []
        for row in rows:
            club = row.get("teamAbbrev", {}) or {}
            # Some shapes: teamId, teamName, teamAbbrev (string)
            team_rows.append({
                "teamId": row.get("teamId"),
                "name": row.get("teamName"),
                "triCode": row.get("teamAbbrev"),
                "conference": row.get("conferenceName"),
                "division": row.get("divisionName"),
            })
        if team_rows:
            teams_df = pd.DataFrame(team_rows).drop_duplicates(subset=["teamId"]).sort_values("teamId")
            teams_df.to_csv(os.path.join(csv_dir, "teams.csv"), index=False)
    else:
        print("[WARN] standings/now unavailable; continuing…")

    # ---------- 1) League schedule sweep (by date) ----------
    # We stick to new API by scanning each date from season start to today.
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
            # gameWeek -> [ { "date":"YYYY-MM-DD", "games":[...] }, ... ] (varies)
            for wk in sched["gameWeek"]:
                for g in wk.get("games", []):
                    gpk = g.get("id") or g.get("gameId") or g.get("gamePk")
                    if not gpk:
                        continue
                    if gpk not in game_pks:
                        game_pks.append(int(gpk))
                    game_rows.append({
                        "gamePk": int(gpk),
                        "gameDate": g.get("startTimeUTC") or g.get("startTime"),  # UTC iso
                        "homeId": (g.get("homeTeam", {}) or {}).get("id"),
                        "homeTri": (g.get("homeTeam", {}) or {}).get("abbrev"),
                        "awayId": (g.get("awayTeam", {}) or {}).get("id"),
                        "awayTri": (g.get("awayTeam", {}) or {}).get("abbrev"),
                        "venue": (g.get("venue", {}) or {}).get("default") or g.get("venueName"),
                        "gameState": g.get("gameState"),
                    })
        # tiny delay so we don’t hammer
        time.sleep(0.10)
        cur += timedelta(days=1)

    # Write schedule csv
    if game_rows:
        pd.DataFrame(game_rows).drop_duplicates(subset=["gamePk"]).to_csv(
            os.path.join(csv_dir, "schedule.csv"), index=False
        )
    print(f"Total unique games discovered: {len(game_pks)}")

    # ---------- 2) Per-game payloads (new API only) ----------
    # For each gamePk: landing, play-by-play, boxscore (api-web)
    # plus shift charts (stats/rest)
    players_seen: Dict[int, Dict[str, Any]] = {}  # aggregate simple player attributes from boxscores
    for idx, gpk in enumerate(game_pks, start=1):
        if idx % 50 == 0:
            print(f"…{idx} games processed")

        # landing
        landing = jget(f"{API_WEB}/gamecenter/{gpk}/landing")
        if landing:
            dump_json(landing, os.path.join(raw_dir, f"game_{gpk}_landing.json"))

        # play-by-play
        pbp = jget(f"{API_WEB}/gamecenter/{gpk}/play-by-play")
        if pbp:
            dump_json(pbp, os.path.join(raw_dir, f"game_{gpk}_pbp.json"))

        # boxscore
        box = jget(f"{API_WEB}/gamecenter/{gpk}/boxscore")
        if box:
            dump_json(box, os.path.join(raw_dir, f"game_{gpk}_boxscore.json"))
            # Try to lift basic player fields so you still get a players.csv without StatsAPI
            # Shapes can vary; commonly box["playerByGameStats"] or team blocks have players with sweaterNumber/position/shootsCatches
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
                harvest(home_blk, (home_blk.get("id") if isinstance(home_blk.get("id"), int) else None))
                harvest(away_blk, (away_blk.get("id") if isinstance(away_blk.get("id"), int) else None))
            except Exception as e:
                print(f"[WARN] Could not harvest players from boxscore {gpk}: {e}")

        # shift charts (Stats REST)
        shifts = jget(f"{API_STATS_REST}/shiftcharts", params={"cayenneExp": f"gameId={gpk}"})
        if shifts:
            dump_json(shifts, os.path.join(raw_dir, f"game_{gpk}_shifts.json"))

        time.sleep(0.12)  # be nice

    # ---------- 3) Players CSV (derived from boxscores seen so far) ----------
    if players_seen:
        players_df = pd.DataFrame(list(players_seen.values()))
        # drop duplicate records keeping the most recent info encountered
        players_df = players_df.drop_duplicates(subset=["personId"], keep="last")
        players_df.to_csv(os.path.join(csv_dir, "players.csv"), index=False)

    print("Done.")

if __name__ == "__main__":
    main()
