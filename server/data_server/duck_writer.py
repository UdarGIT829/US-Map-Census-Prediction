# data_server/duck_writer.py
#
# Root cause of the TransactionContext / silent year corruption bug:
#   _ensure_table (DDL) and _upsert_row (DML) were sharing the same
#   DuckDB connection. DuckDB auto-commits DDL statements, which leaves
#   the connection in an inconsistent transaction state. Subsequent
#   BEGIN/COMMIT in _upsert_row then either fails visibly
#   ("cannot rollback - no transaction is active") or silently commits
#   with wrong field values.
#
# Fix: DDL and DML use separate connections.
#   1. Open a connection, run _ensure_table (all DDL), close it.
#   2. Open a fresh connection, run _upsert_row (pure DML), close it.
#   Each connection sees a clean transaction state with no DDL residue.

from __future__ import annotations

import duckdb
from typing import Iterable

from data_server.duck_router import state_conn, county_conn

DB_PATH = "./acs_cache.duckdb"

STATE_TABLE_NAME  = "acs5_state_profile"
COUNTY_TABLE_NAME = "acs5_county_profile"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _open_conn(db_path: str = DB_PATH) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(db_path)
    con.execute("PRAGMA threads=4")
    con.execute("SET memory_limit='4GB'")
    return con


def _ensure_table(
    con: duckdb.DuckDBPyConnection,
    table: str,
    columns: Iterable[str],
) -> None:
    """
    Create `table` if it doesn't exist, then ALTER TABLE to add any new
    DP columns that aren't present yet.

    Must be called on a dedicated DDL connection that is closed before
    any DML transaction opens on the same file.
    """
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            geo_level TEXT,
            year      INTEGER,
            state     TEXT,
            county    TEXT,
            NAME      TEXT
        );
    """)

    info = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    existing_cols = {row[1] for row in info}

    for col in columns:
        if col in ("geo_level", "year", "state", "county", "NAME"):
            continue
        if col in existing_cols:
            continue
        try:
            con.execute(f'ALTER TABLE {table} ADD COLUMN "{col}" TEXT;')
            existing_cols.add(col)
        except Exception as e:
            if "already exists" in str(e):
                existing_cols.add(col)
            else:
                raise


def _upsert_row(
    con: duckdb.DuckDBPyConnection,
    table: str,
    row: dict,
    geo_level: str,
    year: int,
    state_fips: str | None,
    county_fips: str | None,
) -> None:
    """
    Delete any existing row for (geo_level, year, state, county),
    then insert the new one.

    Must be called on a fresh connection that has not run any DDL.
    DuckDB's transaction state is clean on a new connection, so
    BEGIN/COMMIT works correctly here.
    """
    where_parts = [f"geo_level = '{geo_level}'", f"year = {year}"]
    if state_fips is not None:
        where_parts.append(f"state = '{str(state_fips).zfill(2)}'")
    if county_fips is not None:
        where_parts.append(f"county = '{str(county_fips).zfill(3)}'")
    where_clause = " AND ".join(where_parts)

    cols         = list(row.keys())
    values       = [row[c] for c in cols]
    placeholders = ", ".join(["?"] * len(cols))
    collist      = ", ".join(f'"{c}"' for c in cols)

    in_transaction = False
    try:
        con.execute("BEGIN")
        in_transaction = True
        con.execute(f"DELETE FROM {table} WHERE {where_clause}")
        con.execute(
            f"INSERT INTO {table} ({collist}) VALUES ({placeholders})",
            values,
        )
        con.execute("COMMIT")
        in_transaction = False
    except Exception:
        if in_transaction:
            try:
                con.execute("ROLLBACK")
            except Exception:
                pass
        raise


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def write_row_and_get_query(
    row: dict,
    year: int,
    geo_level: str,
    state_fips: str | None = None,
    county_fips: str | None = None,
) -> str:
    """
    Upsert `row` into the appropriate DuckDB table and return a SELECT
    that fetches exactly that row.

    DDL (_ensure_table) and DML (_upsert_row) run on separate connections
    so that DDL auto-commits never corrupt the DML transaction state.
    """
    geo_level = geo_level.lower()

    row = dict(row)
    row["geo_level"] = geo_level
    row["year"]      = int(year)
    if state_fips  is not None:
        row["state"]  = str(state_fips).zfill(2)
    if county_fips is not None:
        row["county"] = str(county_fips).zfill(3)

    if geo_level == "state":
        table = STATE_TABLE_NAME
        conn_factory = state_conn
    elif geo_level == "county":
        if not state_fips:
            raise ValueError("state_fips is required when geo_level='county'")
        table = COUNTY_TABLE_NAME
        conn_factory = lambda: county_conn(state_fips)
    else:
        raise ValueError(f"Unsupported geo_level={geo_level!r}")

    # Step 1: DDL on its own connection — create table + add any new columns
    con_ddl = conn_factory()
    try:
        _ensure_table(con_ddl, table, row.keys())
    finally:
        con_ddl.close()

    # Step 2: DML on a fresh connection — clean transaction state guaranteed
    con_dml = conn_factory()
    try:
        _upsert_row(con_dml, table, row, geo_level, int(year), state_fips, county_fips)
    finally:
        con_dml.close()

    # Return SELECT for this exact row (used by api.py)
    state_val  = row.get("state")
    county_val = row.get("county")
    where_parts = [f"geo_level = '{geo_level}'", f"year = {int(year)}"]
    if state_val:
        where_parts.append(f"state = '{state_val}'")
    if county_val:
        where_parts.append(f"county = '{county_val}'")
    return f"SELECT * FROM {table} WHERE {' AND '.join(where_parts)};"


# ---------------------------------------------------------------------------
# KV cache  (lives in DB_PATH = ./acs_cache.duckdb)
# ---------------------------------------------------------------------------

def _ensure_kv(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS cache_kv (
            key        TEXT PRIMARY KEY,
            value      TEXT,
            updated_at TIMESTAMP DEFAULT current_timestamp
        )
    """)


def kv_get(key: str, db_path: str = DB_PATH) -> str | None:
    con = _open_conn(db_path)
    try:
        _ensure_kv(con)
        row = con.execute(
            "SELECT value FROM cache_kv WHERE key = ?", [key]
        ).fetchone()
        return row[0] if row else None
    finally:
        con.close()


def kv_set(key: str, value_json: str, db_path: str = DB_PATH) -> None:
    con = _open_conn(db_path)
    try:
        _ensure_kv(con)
        con.execute("BEGIN")
        con.execute("DELETE FROM cache_kv WHERE key = ?", [key])
        con.execute(
            "INSERT INTO cache_kv (key, value) VALUES (?, ?)", [key, value_json]
        )
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()