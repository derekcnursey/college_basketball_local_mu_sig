#!/usr/bin/env python3
import csv
import json
import os
import re
import sys
from datetime import datetime, timezone

FIELD_MAP = {
    "away_team": ["away_team_name", "away_team"],
    "home_team": ["home_team_name", "home_team"],
    "neutral_site": ["neutral_site"],
    "market_spread_home": ["home_spread_num", "market_spread_home"],
    "model_mu_home": ["model_home_spread", "pred_margin", "model_mu_home"],
    "pred_sigma": ["pred_sigma"],
    "edge_home_points": ["edge_points", "edge_home_points"],
    "pred_home_win_prob": ["pred_home_win_prob"],
    "pick_side": ["pick_side"],
    "pick_cover_prob": ["pick_cover_prob"],
    "pick_prob_edge": ["pick_prob_edge"],
    "pick_ev_per_1": ["pick_ev_per_1"],
    "pick_spread_odds": ["pick_spread_odds"],
    "pick_fair_odds": ["pick_fair_odds"],
}


def normalize_date(value: str) -> str | None:
    if not value:
        return None
    match = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", value)
    if match:
        year, month, day = match.groups()
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
    match = re.search(r"(\d{4})/(\d{1,2})/(\d{1,2})", value)
    if match:
        year, month, day = match.groups()
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
    return None


def extract_date(value: str) -> str | None:
    if not value:
        return None
    return normalize_date(value)


def extract_date_from_filename(path_value: str) -> str | None:
    match = re.search(r"(\d{4})[_-](\d{1,2})[_-](\d{1,2})", path_value)
    if not match:
        return None
    year, month, day = match.groups()
    return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"


def slugify(text: str) -> str:
    lowered = (text or "").lower()
    slug = re.sub(r"[^a-z0-9]+", "_", lowered)
    return slug.strip("_")


def coerce(value: str | None):
    if value is None:
        return None
    cleaned = value.strip()
    if cleaned == "":
        return None
    lowered = cleaned.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    try:
        if re.fullmatch(r"-?\d+", cleaned):
            return int(cleaned)
        return float(cleaned)
    except ValueError:
        return cleaned


def pick_value(row: dict[str, str], keys: list[str]) -> str | None:
    for key in keys:
        if key in row and row[key] != "":
            return row[key]
    return None


def build_game(row: dict[str, str], date: str) -> dict:
    game: dict[str, object] = {}
    for target, sources in FIELD_MAP.items():
        raw = pick_value(row, sources)
        game[target] = coerce(raw)

    away = str(game.get("away_team") or "")
    home = str(game.get("home_team") or "")
    game_id = slugify(f"{date}_{away}_{home}")
    game["game_id"] = game_id
    return game


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: csv_to_json.py /path/to/preds.csv [YYYY-MM-DD]", file=sys.stderr)
        return 1

    csv_path = sys.argv[1]
    override_date = sys.argv[2] if len(sys.argv) > 2 else None

    if not os.path.isfile(csv_path):
        print(f"CSV not found: {csv_path}", file=sys.stderr)
        return 1

    with open(csv_path, "r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    if not rows:
        print("CSV has no rows.", file=sys.stderr)
        return 1

    date_value = override_date
    if not date_value:
        date_value = extract_date(rows[0].get("date", ""))

    if not date_value:
        date_value = extract_date_from_filename(os.path.basename(csv_path))

    if not date_value:
        print("Could not infer date. Provide YYYY-MM-DD as second arg.", file=sys.stderr)
        return 1

    games = [build_game(row, date_value) for row in rows]

    payload = {
        "date": date_value,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "games": games,
    }

    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    output_dirs = [
        os.path.join(repo_root, "site", "public", "data"),
        os.path.join(repo_root, "predictions", "json"),
    ]

    for data_dir in output_dirs:
        os.makedirs(data_dir, exist_ok=True)
        output_path = os.path.join(data_dir, f"predictions_{date_value}.json")
        with open(output_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
        print(f"Wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
