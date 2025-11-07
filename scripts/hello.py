#!/usr/bin/env python3
import os, json, datetime, pathlib
from zoneinfo import ZoneInfo

def main():
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    now_ak  = now_utc.astimezone(ZoneInfo("America/Anchorage"))

    run_dir = pathlib.Path("data") / "_runs" / now_ak.strftime("%Y-%m-%d")
    run_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "utc_iso": now_utc.isoformat(),
        "alaska_iso": now_ak.isoformat(),
        "note": "If you see this file, the nightly job ran and had write access."
    }

    out_path = run_dir / f"run-{now_ak.strftime('%H%M%S')}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"Wrote {out_path}")

if __name__ == "__main__":
    main()
