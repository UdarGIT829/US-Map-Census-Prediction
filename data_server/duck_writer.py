# data_server/duck_writer.py
from __future__ import annotations

import duckdb
from typing import Iterable

from data_server.duck_router import state_conn, county_conn

# You can keep these if other code imports them, but they're unused now:
DB_PATH = "./acs_cache.duckdb"

STATE_TABLE_NAME = "acs5_state_profile"
COUNTY_TABLE_NAME = "acs5_county_profile"

def _open_conn(db_path: str = DB_PATH):
    con = duckdb.connect(db_path)
    
    con.execute("PRAGMA threads=4")
    con.execute("SET memory_limit='4GB'")
    return con

def _ensure_table(con: duckdb.DuckDBPyConnection, table: str, columns: Iterable[str]) -> None:
    """
    Ensure that `table` exists with at least the basic columns.
    We will add missing DPxx_* columns dynamically.
    """
    # Basic schema: id fields and year; DP columns will be added with ALTER TABLE.
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            geo_level TEXT,
            year      INTEGER,
            state     TEXT,
            county    TEXT,
            NAME      TEXT
        );
    """)

    # DuckDB's PRAGMA table_info returns rows like:
    # (column_index, column_name, column_type, ...),
    # so we need row[1] for the *name*.
    info = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    existing_cols = {row[1] for row in info}

    for col in columns:
        # Skip known ID/label columns; they're already in the CREATE TABLE
        if col in ("geo_level", "year", "state", "county", "NAME"):
            continue
        if col in existing_cols:
            continue

        try:
            con.execute(f'ALTER TABLE {table} ADD COLUMN "{col}" TEXT;')
            existing_cols.add(col)
        except Exception as e:
            # In case of a race (another process added it just now),
            # ignore "already exists" and continue.
            if "already exists" in str(e):
                existing_cols.add(col)
                continue
            raise


def write_row_and_get_query(
    row: dict,
    year: int,
    geo_level: str,
    state_fips: str | None = None,
    county_fips: str | None = None,
) -> str:
    """
    Insert `row` into the appropriate DuckDB file/table and return
    a SELECT SQL that would fetch exactly that row.

    - States:   db_router.state_conn(),   table acs5_state_profile
    - Counties: db_router.county_conn(),  table acs5_county_profile (per-state DB)
    """
    # normalize
    geo_level = geo_level.lower()

    # Fill id fields in the row (same as before)
    row = dict(row)  # make a copy
    row["geo_level"] = geo_level
    row["year"] = int(year)
    if state_fips is not None:
        row["state"] = str(state_fips).zfill(2)
    if county_fips is not None:
        row["county"] = str(county_fips).zfill(3)

    # Decide which DB/table
    if geo_level == "state":
        con = state_conn()
        table = STATE_TABLE_NAME
    elif geo_level == "county":
        if not state_fips:
            raise ValueError("state_fips is required when geo_level='county'")
        con = county_conn(state_fips)
        table = COUNTY_TABLE_NAME
    else:
        raise ValueError(f"Unsupported geo_level={geo_level!r}")

    try:
        # 1) Ensure table and columns exist
        _ensure_table(con, table, row.keys())

        # 2) Insert row
        cols = list(row.keys())
        placeholders = ", ".join(["?"] * len(cols))
        collist = ", ".join(cols)

        values = [row[c] for c in cols]
        con.execute(
            f"INSERT INTO {table} ({collist}) VALUES ({placeholders})",
            values,
        )

        # 3) Build a SELECT that would fetch this exact row
        state_val = row.get("state")
        county_val = row.get("county")

        where_parts = [
            f"geo_level = '{geo_level}'",
            f"year = {int(year)}",
        ]
        if state_val is not None:
            where_parts.append(f"state = '{state_val}'")
        if county_val is not None:
            where_parts.append(f"county = '{county_val}'")

        where_clause = " AND ".join(where_parts)
        sql = f"SELECT * FROM {table} WHERE {where_clause};"
        return sql

    finally:
        con.close()


# ---- Minimal KV cache ----
def _ensure_kv(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS cache_kv (
          key TEXT PRIMARY KEY,
          value TEXT,
          updated_at TIMESTAMP DEFAULT current_timestamp
        )
    """)

def kv_get(key: str, db_path: str = DB_PATH) -> str | None:
    con = _open_conn(db_path)
    try:
        _ensure_kv(con)
        row = con.execute("SELECT value FROM cache_kv WHERE key = ?", [key]).fetchone()
        return row[0] if row else None
    finally:
        con.close()

def kv_set(key: str, value_json: str, db_path: str = DB_PATH) -> None:
    con = _open_conn(db_path)
    try:
        _ensure_kv(con)
        # Upsert emulation: delete then insert (atomic via transaction)
        con.execute("BEGIN")
        con.execute("DELETE FROM cache_kv WHERE key = ?", [key])
        con.execute("INSERT INTO cache_kv(key, value) VALUES (?, ?)", [key, value_json])
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()
