etl-cricbuzz
==============================

ETL pipeline for crickbuzz data using API.
This project is a real-world **ETL pipeline** that extracts cricket match and ICC standings data using the [Cricbuzz API](https://rapidapi.com/cricketapilive/api/cricbuzz-cricket/), transforms it into normalized tables, and loads it into a **MySQL database**. The pipeline is built with Python and managed using **DVC** for reproducibility and **Git** for version control.

> 🧱 **Project structure is based on [`cookiecutter-data-science`](https://github.com/drivendataorg/cookiecutter-data-science) template.**

---

Project Organization
------------

```
├── LICENSE
├── Makefile           <- Makefile with commands like `make data` or `make train`
├── README.md          <- The top-level README for developers using this project.
├── data
│   ├── external       <- Data from third party sources.
│   ├── interim        <- Intermediate data that has been transformed.
│   ├── processed      <- The final, canonical data sets for modeling and loading to DB.
│   └── raw            <- The original, immutable data dump (from API).
│
├── docs               <- Sphinx documentation project.
│
├── logs               <- Log files for each ETL step.
│
├── requirements.txt   <- The requirements file for reproducing the analysis environment.
│
├── setup.py           <- Makes project pip installable (pip install -e .) so src can be imported.
├── src                <- Source code for the ETL pipeline.
│   ├── __init__.py    <- Makes src a Python module
│   ├── extract        <- Scripts to extract/download data from APIs
│   │   └── extract.py
│   ├── transform      <- Scripts to transform raw data into normalized tables
│   │   └── transform.py
│   ├── load           <- Scripts to load processed data into MySQL
│   │   └── load.py
│   └── ...            <- Additional modules for features, modeling, etc.
└── tox.ini            <- tox file with settings for running tox; see tox.readthedocs.io
```

---

## 🔌 Requirements

- Python 3.8+
- MySQL server running locally
- `config.ini` with valid API and DB credentials
- RapidAPI key for [Cricbuzz API](https://rapidapi.com/cricketapilive/api/cricbuzz-cricket/)

---

## ⚙️ Setup Instructions

### 1. Clone the repo and install dependencies

```bash
git clone https://github.com/yourname/etl-cricbuzz.git
cd etl-cricbuzz
pip install -r requirements.txt
```

### 2. Configure API and Database

Create a `config.ini` file in the project root:

```ini
[API]
x-rapidapi-key = your_rapidapi_key
x-rapidapi-host = cricbuzz-cricket.p.rapidapi.com

[MYSQL]
host = localhost
user = root
password = yourpassword
database = cricbuzz_etl
```

### 3. Initialize DVC and Set Up Pipeline

Initialize DVC in your project:

```bash
dvc init
```

Add the three pipeline stages:

**Extract stage:**
```bash
dvc stage add -n extract \
  -d src/extract.py \
  -o data/raw/recent_matches.json \
  -o data/raw/icc_standings.json \
  python src/extract.py
```

**Transform stage:**
```bash
dvc stage add -n transform \
  -d src/transform.py \
  -d data/raw/recent_matches.json \
  -d data/raw/icc_standings.json \
  -o data/processed/series.json \
  -o data/processed/teams.json \
  -o data/processed/matches.json \
  -o data/processed/icc_standings.json \
  -o data/processed/cleaned_matches.json \
  python src/transform.py
```

**Load stage:**
```bash
dvc stage add -n load \
  -d src/load.py \
  -d data/processed/series.json \
  -d data/processed/teams.json \
  -d data/processed/matches.json \
  -d data/processed/icc_standings.json \
  python src/load.py
```

You should now have a `dvc.yaml` file tracking all ETL stages.

### 4. Run Full Pipeline

Once you've added all stages:

```bash
dvc repro
```

This will:
- Run `extract.py` to fetch data from API
- Run `transform.py` to normalize and clean it
- Run `load.py` to push it into MySQL