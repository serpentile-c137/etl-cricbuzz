import http.client
import configparser
import json
import os
import logging
from typing import Any, Dict

# Configure logging to both console and file
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "extract.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, mode='a')
    ]
)

def load_config(config_path: str = "config.ini") -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    if not config.read(config_path):
        logging.error(f"Failed to read config file: {config_path}")
        raise FileNotFoundError(f"Config file not found: {config_path}")
    logging.info(f"Loaded config from {config_path}")
    return config


def fetch_recent_matches(config: configparser.ConfigParser) -> Dict[str, Any]:
    try:
        host = config["API"]["x-rapidapi-host"]
        key = config["API"]["x-rapidapi-key"]
        conn = http.client.HTTPSConnection(host)
        headers = {
            'x-rapidapi-key': key,
            'x-rapidapi-host': host
        }
        conn.request("GET", "/matches/v1/recent", headers=headers)
        res = conn.getresponse()
        data = res.read().decode("utf-8")
        json_data = json.loads(data)
        logging.info("Fetched recent matches from API.")
        return json_data
    except Exception as e:
        logging.error(f"Failed to fetch recent matches: {e}")
        raise

def fetch_icc_standings(config: configparser.ConfigParser) -> Dict[str, Any]:
    try:
        host = config["API"]["x-rapidapi-host"]
        key = config["API"]["x-rapidapi-key"]
        conn = http.client.HTTPSConnection(host)
        headers = {
            'x-rapidapi-key': key,
            'x-rapidapi-host': host
        }
        conn.request("GET", "/stats/v1/iccstanding/team/matchtype/1", headers=headers)
        res = conn.getresponse()
        data = res.read().decode("utf-8")
        json_data = json.loads(data)
        logging.info("Fetched ICC standings from API.")
        return json_data
    except Exception as e:
        logging.error(f"Failed to fetch ICC standings: {e}")
        raise

def save_json(data: Dict[str, Any], file_path: str) -> None:
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)
        logging.info(f"Saved data to {file_path}")
    except Exception as e:
        logging.error(f"Failed to save data to {file_path}: {e}")
        raise


def main() -> None:
    try:
        config = load_config("config.ini")
        # Fetch and save recent matches
        json_data = fetch_recent_matches(config)
        save_json(json_data, "data/raw/recent_matches.json")
        # Fetch and save ICC standings
        icc_data = fetch_icc_standings(config)
        save_json(icc_data, "data/raw/icc_standings.json")
        logging.info("ETL extract step completed successfully.")
    except Exception as e:
        logging.critical(f"ETL extract step failed: {e}")
        raise

if __name__ == "__main__":
    main()
