import json
import os
import logging
from typing import Any, Dict, List, Tuple

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "transform.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_FILE, mode='a')]
)

def load_json(file_path: str) -> Any:
    with open(file_path) as f:
        return json.load(f)

def format_score(score):
    if score:
        runs = score.get("inngs1", {}).get("runs")
        wickets = score.get("inngs1", {}).get("wickets")
        overs = score.get("inngs1", {}).get("overs")
        return f"{runs}/{wickets} ({overs} ov)" if runs is not None else None
    return None

def extract_normalized_tables(data: Dict[str, Any]) -> Tuple[List, List, List, List]:
    series_table = {}
    # venues_table removed
    teams_table = {}
    matches_table = []

    team_name_to_id = {}
    team_id_counter = 1

    matches = data.get("typeMatches", [])
    for type_block in matches:
        for series_match in type_block.get("seriesMatches", []):
            series_info = series_match.get("seriesAdWrapper", {})
            series_id = series_info.get("seriesId")
            series_name = series_info.get("seriesName")
            series_table[series_id] = {"series_id": series_id, "series_name": series_name}

            for match in series_info.get("matches", []):
                match_info = match.get("matchInfo", {})
                match_id = match_info.get("matchId")
                team1 = match_info.get("team1", {}).get("teamName")
                team2 = match_info.get("team2", {}).get("teamName")
                status = match_info.get("status")
                desc = match_info.get("matchDesc")

                # Score
                score1 = match.get("matchScore", {}).get("team1Score", {})
                score2 = match.get("matchScore", {}).get("team2Score", {})
                team1_score = format_score(score1)
                team2_score = format_score(score2)

                # Winner & Win By
                winner = None
                win_by = None
                if team1 and team2 and status:
                    if team1 in status:
                        winner = team1
                    elif team2 in status:
                        winner = team2
                    parts = status.split(" by ")
                    if len(parts) == 2:
                        win_by = parts[1]

                # Teams
                for team in [team1, team2, winner]:
                    if team and team not in team_name_to_id:
                        team_name_to_id[team] = team_id_counter
                        teams_table[team_id_counter] = {"team_id": team_id_counter, "team_name": team}
                        team_id_counter += 1

                team1_id = team_name_to_id.get(team1)
                team2_id = team_name_to_id.get(team2)
                winner_id = team_name_to_id.get(winner)

                # Match (venue_id removed)
                matches_table.append({
                    "match_id": match_id,
                    "series_id": series_id,
                    "team1_id": team1_id,
                    "team2_id": team2_id,
                    "team1_score": team1_score,
                    "team2_score": team2_score,
                    "status": status,
                    "desc": desc,
                    "winner_id": winner_id,
                    "win_by": win_by
                })

    return (
        list(series_table.values()),
        list(teams_table.values()),
        matches_table
    )

def save_json(data: Any, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def build_cleaned_matches(matches: List[Dict]) -> List[Dict]:
    cleaned = []
    for m in matches:
        cleaned.append({
            "match_id": m.get("match_id"),
            "series_id": m.get("series_id"),
            "team1_id": m.get("team1_id"),
            "team2_id": m.get("team2_id"),
            "team1_score": m.get("team1_score"),
            "team2_score": m.get("team2_score"),
            "status": m.get("status"),
            "desc": m.get("desc"),
            "winner_id": m.get("winner_id"),
            "win_by": m.get("win_by")
        })
    return cleaned


def extract_icc_standings(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    standings = []
    try:
        # Handle new format with 'headers' and 'values'
        if "headers" in data and "values" in data:
            headers = data["headers"]
            for row in data["values"]:
                value = row.get("value", [])
                record = {headers[i].lower(): value[i] if i < len(value) else None for i in range(len(headers))}
                standings.append(record)
        else:
            # Fallback to previous logic
            teams = []
            if "groupStandings" in data and data["groupStandings"]:
                for group in data["groupStandings"]:
                    if "standings" in group:
                        teams.extend(group["standings"])
            elif "standings" in data:
                teams = data["standings"]
            else:
                logging.warning("No standings found in ICC standings data.")
            for team in teams:
                standings.append({
                    "team_id": team.get("teamId"),
                    "team_name": team.get("teamName"),
                    "rank": team.get("rank"),
                    "points": team.get("points"),
                    "matches": team.get("matches"),
                    "won": team.get("won"),
                    "lost": team.get("lost"),
                    "tied": team.get("tied"),
                    "nrr": team.get("nrr"),
                    "series_won": team.get("seriesWon"),
                    "series_lost": team.get("seriesLost"),
                    "series_drawn": team.get("seriesDrawn")
                })
        logging.info(f"Extracted {len(standings)} ICC standings records.")
    except Exception as e:
        logging.error(f"Failed to extract ICC standings: {e}")
    return standings

def main():
    # Process match data
    raw_data = load_json("data/raw/recent_matches.json")
    series, teams, matches = extract_normalized_tables(raw_data)
    save_json(series, "data/processed/series.json")
    save_json(teams, "data/processed/teams.json")
    save_json(matches, "data/processed/matches.json")
    # Legacy output for DVC compatibility
    cleaned_matches = build_cleaned_matches(matches)
    save_json(cleaned_matches, "data/processed/cleaned_matches.json")
    # Process ICC standings
    icc_data = load_json("data/raw/icc_standings.json")
    icc_standings = extract_icc_standings(icc_data)
    save_json(icc_standings, "data/processed/icc_standings.json")
    logging.info("Normalized data extracted and saved, including legacy cleaned_matches.json and ICC standings.")

if __name__ == "__main__":
    main()
