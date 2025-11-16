# data_server/duck_router.py
from __future__ import annotations

from pathlib import Path
import duckdb

# Base directory = this file's directory
BASE_DIR = Path(__file__).resolve().parent
DB_DIR = BASE_DIR / "db"
DB_DIR.mkdir(exist_ok=True)

# --- Config knobs ---
DUCKDB_CONFIG = {
    "threads": 4,                # tweak as you like
    "memory_limit": "4GB",       # hard-ish cap per process
    "preserve_insertion_order": "false",
    "temp_directory": str(DB_DIR / "tmp"),
    "max_temp_directory_size": "2GB",
}


def _open(db_path: Path) -> duckdb.DuckDBPyConnection:
    """
    Centralized 'open connection' with config applied at connect time.
    """
    return duckdb.connect(str(db_path), config=DUCKDB_CONFIG)


# -------- Path helpers --------

def state_db_path() -> Path:
    """
    Single DB file for all state-level rows.
    """
    return DB_DIR / "acs_states.duckdb"


def county_db_path(state_fips: str) -> Path:
    """
    One DB file per state's counties.
    Example: acs_counties_06.duckdb for CA.
    """
    sf = str(state_fips).zfill(2)
    return DB_DIR / f"acs_counties_{sf}.duckdb"


# -------- Connection helpers --------

def state_conn():
    """
    Connection to the states DB.
    """
    return _open(state_db_path())


def county_conn(state_fips: str):
    """
    Connection to the DB holding counties for `state_fips`.
    """
    return _open(county_db_path(state_fips))
