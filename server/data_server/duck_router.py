# data_server/duck_router.py
from __future__ import annotations

from pathlib import Path
import duckdb

BASE_DIR = Path(__file__).resolve().parent
DB_DIR = BASE_DIR / "db"
DB_DIR.mkdir(exist_ok=True)

DUCKDB_CONFIG = {
    "threads": 4,
    "memory_limit": "4GB",
    "preserve_insertion_order": "false",
    "temp_directory": str(DB_DIR / "tmp"),
    "max_temp_directory_size": "2GB",
}


def _open(db_path: Path) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(db_path), config=DUCKDB_CONFIG)


def state_db_path() -> Path:
    return DB_DIR / "acs_states.duckdb"


def county_db_path(state_fips: str) -> Path:
    sf = str(state_fips).zfill(2)
    return DB_DIR / f"acs_counties_{sf}.duckdb"


def state_conn() -> duckdb.DuckDBPyConnection:
    return _open(state_db_path())


def county_conn(state_fips: str) -> duckdb.DuckDBPyConnection:
    return _open(county_db_path(state_fips))