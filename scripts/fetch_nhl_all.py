#!/usr/bin/env python3
"""
Fetch broad NHL data from api-web.nhle.com and save raw JSON files.

Folder layout:
  data/raw/{seasonId}/{YYYY-MM}/{YYYY-MM-DD}/
    standings_now.json
    teams.json
    schedule.json
    games/
      {gameId}/
        boxscore.json
        pbp.json

Usage:
  python scripts/fetch_nhl_all.py --from 2025-09-01 --to 2025-11-07
  python scripts/fetch_nhl_all.py --season auto   # infers season and scans 9/1..today
"""

import argparse
import json
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import time
import sys
import typing as T

import requests

API = "https://api-web.nhle.com/v1"
STANDINGS_NOW = f"{API}/standings/now"
TEAMS = f"{API}/teams"
SCHEDULE_DAY = f"{API}/schedule"              # /YYYY-MM-DD
GAMECENTER = f"{API}/gamecenter"              # /{gameId}/boxscore, /{gameId}/play-by-play

# --------- utils ---------
def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

def season_id_for(today: date) -> str:
    start_year = today.year if today.month >= 7 else today.year - 1
    return f"{start_year}{start_year+1}"

def season_start_for(today: date) -> date:
    start_year = today.year if today.month >= 7 else today.year - 1
    # NHL seasons effectively start Sept 1 for our harvesting window
    return date(start_year, 9, 1)

def daterange(d0: date, d1: date):
    cur = d0
    while cur <= d1:
        yield cur
        cur += timedelta(days=1)

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def save_json(obj: T.Any, path: Path) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def get_json(url: str, *, retries: int = 3, timeout: int = 30) -> T.Any:
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == retries:
                raise
            sleep = 1.5 * attempt
            log(f"Warn: {url} failed ({e}); retrying in {sleep:.1f}s …")
            time.sleep(sleep)
    return None  # never reached

# --------- core fetchers ---------
def fetch_standings() -> T.Any:
    return get_json(STANDINGS_NOW)

def fetch_teams() -> T.Any:
    return get_json(TEAMS)

def fetch_schedule_for_day(d: date) -> T.Any:
    url = f"{SCHEDULE_DAY}/{d.strftime('%Y-%m-%d')}"
    return get_json(url)

def fetch_game_boxscore(game_id: int) -> T.Any:
    url = f"{GAMECENTER}/{game_id}/boxscore"
    return get_json(url)

def fetch_game_pbp(game_id: int) -> T.Any:
    url = f"{GAMECENTER}/{game_id}/play-by-play"
    return get_json(url)

# --------- pipeline ---------
def run_pipeline(from_date: date, to_date: date, out_root: Path) -> None:
    today = date.today()
    season = season_id_for(today)

    # 1) standings + teams are stored once per run under the last day folder
    last_day_folder = out_root / season / to_date.strftime("%Y-%m") / to_date.strftime("%Y-%m-%d")
    ensure_dir(last_day_folder)

    log("Fetching standings …")
    standings = fetch_standings()
    save_json(standings, last_day_folder / "standings_now.json")

    log("Fetching teams …")
    teams = fetch_teams()
    save_json(teams, last_day_folder / "teams.json")

    # 2) day-by-day schedule + per-game artifacts
    for d in daterange(from_date, to_date):
        month_dir = out_root / season / d.strftime("%Y-%m")
        day_dir = month_dir / d.strftime("%Y-%m-%d")
        ensure_dir(day_dir)

        log(f"Fetching schedule for {d.isoformat()} …")
        try:
            schedule = fetch_schedule_for_day(d)
        except Exception as e:
            log(f"Error schedule {d}: {e}")
            continue

        save_json(schedule, day_dir / "schedule.json")

        games = schedule.get("gameWeek", [])
        # gameWeek is usually a list with one item containing "games"; be defensive:
        all_games = []
        for bucket in games:
            if isinstance(bucket, dict):
                all_games.extend(bucket.get("games", []))

        for g in all_games:
            game_id = g.get("id") or g.get("gameId") or g.get("gamePk")
            if not game_id:
                continue
            try:
                gid = int(game_id)
            except Exception:
                continue

            game_dir = day_dir / "games" / str(gid)
            ensure_dir(game_dir)

            # Boxscore
            try:
                log(f"  Game {gid}: boxscore …")
                box = fetch_game_boxscore(gid)
                save_json(box, game_dir / "boxscore.json")
            except Exception as e:
                log(f"  Warn {gid} boxscore: {e}")

            # Play-by-play
            try:
                log(f"  Game {gid}: play-by-play …")
                pbp = fetch_game_pbp(gid)
                save_json(pbp, game_dir / "pbp.json")
            except Exception as e:
                log(f"  Warn {gid} pbp: {e}")

    log("Done.")

# --------- CLI ---------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NHL all-in collector")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--season", choices=["auto"], help="Use 'auto' to infer season and scan Sept 1..today")
    group.add_argument("--from", dest="date_from", help="Start date YYYY-MM-DD")
    parser.add_argument("--to", dest="date_to", help="End date YYYY-MM-DD")
    parser.add_argument("--out", default="data/raw", help="Output root folder (default: data/raw)")
    return parser.parse_args()

def main() -> None:
    args = parse_args()
    tz_anchorage = timezone(timedelta(hours=-9))  # project default
    today_local = datetime.now(tz=tz_anchorage).date()

    if args.season == "auto":
        start = season_start_for(today_local)
        end = today_local
    else:
        if not args.date_from or not args.date_to:
            print("Error: --from and --to are required when --season is not used", file=sys.stderr)
            sys.exit(2)
        start = date.fromisoformat(args.date_from)
        end = date.fromisoformat(args.date_to)
        if end < start:
            print("Error: --to earlier than --from", file=sys.stderr)
            sys.exit(2)

    out_root = Path(args.out)
    run_pipeline(start, end, out_root)

if __name__ == "__main__":
    main()
