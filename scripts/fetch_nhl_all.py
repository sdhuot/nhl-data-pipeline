#!/usr/bin/env python3
"""
NHL all-in scraper (raw + a few ready CSVs)

What it grabs (current season window):
- Standings (current)
- Teams list
- Team rosters and player details
- Season schedule (Sep 1 -> today)
- For every gamePk in that schedule:
  - statsapi feed/live
  - api-web gamecenter: landing, play-by-play, boxscore
  - shift charts (api.nhle.com)

Writes to:
- data/raw/YYYY-MM-DD/...
- data/csv/YYYY-MM-DD/...

You can safely run daily; it only writes into today's dated folder.
"""

import os, json, time
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import requests
import pandas as pd

# ------------ HTTP helpers ------------
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "nhl-scraper/1.0 (+github actions)"})
TIMEOUT = 40

def jget(url, params=None, retries=3, sleep=1.5):
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

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def dump_json(obj, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

# ------------ Season window helpers ------------
def season_start_date(today: date) -> str:
    start_year = today.year if today.month >= 7 else today.year - 1
    return f"{start_year}-09-01"

def today_yyyymmdd():
    return date.today().strftime("%Y-%m-%d")

# ------------ Endpoints ------------
STATSAPI = "https://api.nhle.com/stats/api/v1"
API_WEB  = "https://api-web.nhle.com/v1"
API_NHLE = "https://api.nhle.com/stats/rest/en"

def main():
    run_date = today_yyyymmdd()
    raw_dir = os.path.join("data", "raw", run_date)
    csv_dir = os.path.join("data", "csv", run_date)
    ensure_dir(raw_dir)
    ensure_dir(csv_dir)

    today_d = date.today()
    start = season_start_date(today_d)
    end   = today_d.strftime("%Y-%m-%d")

    # ---------- 0) Standings (current) ----------
    print("Fetching standings...")
    standings_now = jget(f"{API_WEB}/standings/now")
    if standings_now:
        dump_json(standings_now, os.path.join(raw_dir, "standings_now.json"))
        rows = standings_now.get("standings", [])
        if rows:
            pd.json_normalize(rows, max_level=1).to_csv(
                os.path.join(csv_dir, "standings.csv"), index=False
            )

    # ---------- 1) Teams ----------
    print("Fetching teams...")
    teams = jget(f"{STATSAPI}/teams")
    teams_df = pd.DataFrame()
    if teams:
        dump_json(teams, os.path.join(raw_dir, "teams.json"))
        teams_df = pd.json_normalize(teams.get("teams", []), max_level=1)
        teams_df.to_csv(os.path.join(csv_dir, "teams.csv"), index=False)

    # ---------- 2) Rosters + player details ----------
    players_records = []
    if not teams_df.empty:
        for _, t in teams_df.iterrows():
            tid = t.get("id")
            tname = t.get("name")
            if pd.isna(tid):
                continue
            print(f"Roster: {tname} ({tid})")
            roster = jget(f"{STATSAPI}/teams/{int(tid)}", params={"expand": "team.roster"})
            if roster:
                dump_json(roster, os.path.join(raw_dir, f"team_{tid}_roster.json"))
                roster_list = (
                    roster.get("teams", [{}])[0]
                    .get("roster", {})
                    .get("roster", [])
                )
                for r in roster_list:
                    p = r.get("person", {})
                    pid = p.get("id")
                    rec = {
                        "teamId": tid,
                        "teamName": tname,
                        "personId": pid,
                        "personFullName": p.get("fullName"),
                        "positionCode": (r.get("position") or {}).get("code"),
                        "positionName": (r.get("position") or {}).get("name"),
                        "jerseyNumber": r.get("jerseyNumber"),
                    }
                    # Player details
                    if pid:
                        person = jget(f"{STATSAPI}/people/{int(pid)}")
                        if person:
                            dump_json(
                                person,
                                os.path.join(raw_dir, f"player_{pid}.json")
                            )
                            pdata = (person.get("people") or [{}])[0]
                            # attach some common fields
                            rec.update({
                                "birthDate": pdata.get("birthDate"),
                                "birthCity": pdata.get("birthCity"),
                                "birthCountry": pdata.get("birthCountry"),
                                "height": pdata.get("height"),
                                "weight": pdata.get("weight"),
                                "shootsCatches": pdata.get("shootsCatches"),
                                "currentAge": pdata.get("currentAge"),
                                "primaryNumber": pdata.get("primaryNumber"),
                                "primaryPosition": (pdata.get("primaryPosition") or {}).get("abbreviation"),
                            })
                    players_records.append(rec)

    if players_records:
        pd.DataFrame(players_records).to_csv(os.path.join(csv_dir, "players.csv"), index=False)

    # ---------- 3) Schedule ----------
    print(f"Fetching schedule {start} -> {end} ...")
    sched = jget(f"{STATSAPI}/schedule", params={"startDate": start, "endDate": end})
    game_rows = []
    game_pks = []
    if sched:
        dump_json(sched, os.path.join(raw_dir, "schedule.json"))
        for d in sched.get("dates", []):
            gms = d.get("games", [])
            for g in gms:
                game_pks.append(g.get("gamePk"))
                game_rows.append({
                    "gamePk": g.get("gamePk"),
                    "gameType": g.get("gameType"),
                    "season": g.get("season"),
                    "gameDate": g.get("gameDate"),
                    "status": (g.get("status") or {}).get("detailedState"),
                    "homeId": (g.get("teams", {}).get("home", {}).get("team") or {}).get("id"),
                    "homeName": (g.get("teams", {}).get("home", {}).get("team") or {}).get("name"),
                    "awayId": (g.get("teams", {}).get("away", {}).get("team") or {}).get("id"),
                    "awayName": (g.get("teams", {}).get("away", {}).get("team") or {}).get("name"),
                    "venue": (g.get("venue") or {}).get("name"),
                })
    if game_rows:
        pd.DataFrame(game_rows).to_csv(os.path.join(csv_dir, "schedule.csv"), index=False)

    # ---------- 4) Per-game payloads ----------
    # Keep it polite with a tiny delay to avoid hammering
    for idx, gpk in enumerate([gp for gp in game_pks if gp], start=1):
        if idx % 50 == 0:
            print(f"…{idx} games processed")

        # statsapi feed/live
        feed = jget(f"{STATSAPI}/game/{gpk}/feed/live")
        if feed:
            dump_json(feed, os.path.join(raw_dir, f"game_{gpk}_feed_live.json"))

        # api-web gamecenter (landing, play-by-play, boxscore)
        landing = jget(f"{API_WEB}/gamecenter/{gpk}/landing")
        if landing:
            dump_json(landing, os.path.join(raw_dir, f"game_{gpk}_landing.json"))

        pbp = jget(f"{API_WEB}/gamecenter/{gpk}/play-by-play")
        if pbp:
            dump_json(pbp, os.path.join(raw_dir, f"game_{gpk}_pbp.json"))

        box = jget(f"{API_WEB}/gamecenter/{gpk}/boxscore")
        if box:
            dump_json(box, os.path.join(raw_dir, f"game_{gpk}_boxscore.json"))

        # shift charts (uses gameId)
        shifts = jget(f"{API_NHLE}/shiftcharts", params={"cayenneExp": f"gameId={gpk}"})
        if shifts:
            dump_json(shifts, os.path.join(raw_dir, f"game_{gpk}_shifts.json"))

        time.sleep(0.15)  # be nice

    print("Done.")

if __name__ == "__main__":
    main()
