# duck_writer.py
# Minimal DuckDB writer for ACS profile data.
# - Creates/opens a single DB file (default ./acs_cache.duckdb)
# - Ensures table acs5_profile exists and has ALL columns present in the payload
# - Inserts/UPSERTs a single row
# - Returns an exact SELECT SQL to retrieve that row later

import os
import duckdb

DB_PATH = "./acs_cache.duckdb"
TABLE = "acs5_profile"

# Core identifier columns we always keep
CORE_COLS = {
    "geo_level": "TEXT",   # 'state' or 'county'
    "year": "INTEGER",
    "state": "TEXT",
    "county": "TEXT",      # NULL for state-level rows
    "NAME": "TEXT",
}

def _open_conn(db_path: str = DB_PATH):
    con = duckdb.connect(db_path)
    
    con.execute("PRAGMA threads=4")
    con.execute("SET memory_limit='4GB'")
    return con

def _table_exists(con, table: str) -> bool:
    return bool(con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table]
    ).fetchone()[0])

def _current_columns(con, table: str):
    if not _table_exists(con, table):
        return set()
    rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    # rows: [ (cid, name, type, notnull, dflt_value, pk), ... ]
    return {r[1] for r in rows}

def _ensure_table(con, table: str):
    if not _table_exists(con, table):
      con.execute(f"""
        CREATE TABLE {table} (
          geo_level TEXT,
          year INTEGER,
          state TEXT,
          county TEXT,
          NAME TEXT
        )
      """)
      # Helpful clustered sort for common filters (optional, cheap metadata op)
      con.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_key ON {table}(geo_level, year, state, county)")
    else:
      # Make sure core columns exist (in case table pre-existed differently)
      existing = _current_columns(con, table)
      for col, typ in CORE_COLS.items():
          if col not in existing:
              con.execute(f'ALTER TABLE {table} ADD COLUMN "{col}" {typ}')

def _add_missing_columns(con, table: str, payload_keys):
    existing = _current_columns(con, table)
    # Add any missing payload columns as TEXT (DuckDB can CAST later as needed)
    new_cols = [k for k in payload_keys if k not in existing]
    for col in new_cols:
        # Variable names like DP02_0001E are safe identifiers; quote anyway
        con.execute(f'ALTER TABLE {table} ADD COLUMN "{col}" TEXT')

def write_row_and_get_query(
    row: dict,
    *,
    year: int,
    geo_level: str,         # 'state' or 'county'
    state_fips: str,
    county_fips: str | None = None,
    db_path: str = DB_PATH,
    table: str = TABLE
) -> str:
    """
    Ensures DB and table, evolves schema to include ALL row keys, inserts row.
    Returns a SELECT SQL string you can use to retrieve this exact slice later.
    """
    if geo_level not in ("state", "county"):
        raise ValueError("geo_level must be 'state' or 'county'")

    # Normalize core identifiers into the row so they are also materialized columns
    row = dict(row)  # shallow copy
    row["geo_level"] = geo_level
    row["year"] = int(year)
    row["state"] = state_fips
    row["county"] = county_fips if county_fips is not None else None

    con = _open_conn(db_path)
    try:
        _ensure_table(con, table)
        _add_missing_columns(con, table, row.keys())

        # Prepare INSERT with only the columns present in 'row'
        cols = list(row.keys())
        placeholders = ", ".join(["?"] * len(cols))
        collist = ", ".join([f'"{c}"' for c in cols])

        # Convert row values to strings for TEXT cols; leave year as int is OK
        values = [row[c] for c in cols]

        con.execute(f"INSERT INTO {table} ({collist}) VALUES ({placeholders})", values)
        con.commit()

        # Build exact retrieval query
        if geo_level == "state":
            sql = f"""SELECT * FROM {table}
WHERE geo_level='state' AND year={year} AND state='{state_fips}'"""
        else:
            sql = f"""SELECT * FROM {table}
WHERE geo_level='county' AND year={year} AND state='{state_fips}' AND county='{county_fips}'"""

        return sql.strip()
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
