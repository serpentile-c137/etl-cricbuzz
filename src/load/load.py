import json
import os
import logging
import pymysql
import configparser
from typing import Any, List, Dict

# ---------- CONFIG ----------
config = configparser.ConfigParser()
config.read("config.ini")

DB_CONFIG = {
    "host": config["MYSQL"]["host"],
    "user": config["MYSQL"]["user"],
    "password": config["MYSQL"]["password"],
    "database": config["MYSQL"]["database"]
}

DATA_DIR = "data/processed"
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "load.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_FILE, mode='a')]
)

# ---------- UTILS ----------
def load_json(file_path: str) -> Any:
    try:
        with open(file_path) as f:
            data = json.load(f)
        logging.info(f"Loaded {file_path}")
        return data
    except Exception as e:
        logging.error(f"Error loading {file_path}: {e}")
        raise

def insert_data(cursor, table: str, data: List[Dict[str, Any]]):
    if not data:
        logging.warning(f"No data to insert into {table}")
        return

    # Filter out records with null primary keys for series
    if table == "series":
        data = [row for row in data if row.get("series_id") is not None]
        if not data:
            logging.warning("No valid series records to insert (all missing series_id).")
            return

    keys = list(data[0].keys())

    # MySQL reserved words that should be quoted
    MYSQL_RESERVED_WORDS = {"desc", "rank", "order", "group", "select", "from", "to"}

    columns = []
    for k in keys:
        if k.lower() in MYSQL_RESERVED_WORDS:
            columns.append(f"`{k}`")
        else:
            columns.append(k)

    columns_str = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(keys))
    query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"

    values = [tuple(row.values()) for row in data]
    try:
        cursor.executemany(query, values)
        logging.info(f"Inserted {len(values)} rows into {table}")
    except Exception as e:
        logging.error(f"Error inserting into {table}: {e}")
        raise

# ---------- MAIN ----------
def main():
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Create all required tables if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS series (
                series_id INT PRIMARY KEY,
                series_name VARCHAR(255)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS teams (
                team_id INT PRIMARY KEY,
                team_name VARCHAR(255) UNIQUE
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS matches (
                match_id INT PRIMARY KEY,
                series_id INT,
                team1_id INT,
                team2_id INT,
                team1_score VARCHAR(50),
                team2_score VARCHAR(50),
                status TEXT,
                `desc` TEXT,
                winner_id INT,
                win_by VARCHAR(50),
                FOREIGN KEY (series_id) REFERENCES series(series_id),
                FOREIGN KEY (team1_id) REFERENCES teams(team_id),
                FOREIGN KEY (team2_id) REFERENCES teams(team_id),
                FOREIGN KEY (winner_id) REFERENCES teams(team_id)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS icc_standings (
                `rank` INT,
                flag VARCHAR(255),
                team VARCHAR(255),
                pct FLOAT,
                PRIMARY KEY (`rank`, team)
            )
        """)

        # Load JSON files
        series_data = load_json(os.path.join(DATA_DIR, "series.json"))
        teams_data = load_json(os.path.join(DATA_DIR, "teams.json"))
        matches_data = load_json(os.path.join(DATA_DIR, "matches.json"))
        icc_standings_data = load_json(os.path.join(DATA_DIR, "icc_standings.json"))

        # Insert in dependency-safe order
        insert_data(cursor, "series", series_data)
        insert_data(cursor, "teams", teams_data)
        insert_data(cursor, "matches", matches_data)

        # Insert ICC standings
        icc_rows = []
        for row in icc_standings_data:
            icc_rows.append({
                "rank": int(row.get("rank")) if row.get("rank") else None,
                "flag": str(row.get("flag")) if row.get("flag") else None,
                "team": str(row.get("team")) if row.get("team") else None,
                "pct": float(row.get("pct")) if row.get("pct") else None
            })
        insert_data(cursor, "icc_standings", icc_rows)

        conn.commit()
        logging.info("Data loaded into MySQL successfully.")
    except Exception as e:
        logging.critical(f"ETL load step failed: {e}")
        if 'conn' in locals() and conn:
            conn.rollback()
    finally:
        if 'conn' in locals() and conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    main()
