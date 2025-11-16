# api.py
# Minimal FastAPI endpoints over ./acs_cache.duckdb
# Adds:
#   - GET /data/state/{state_fips}
#   - GET /data/county/{state_fips}/{county_fips}
# Each endpoint:
#   1) uses fetch_or_cache(...) to ensure data exists / cache hit
#   2) calls write_row_and_get_query(...) to get exact SQL
#   3) executes SQL and returns the full wide row (dict)
#
# Run:  uvicorn api:app --reload

from typing import Optional

from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
import duckdb

from data_server.duck_router import state_conn, county_conn
from data_server.duck_writer import STATE_TABLE_NAME, COUNTY_TABLE_NAME
# Reuse the fetch + discovery from your helper script
from data_server.acs_loader import (
    fetch_or_cache, GROUPS, YEAR, list_counties_for_state,
    list_acs_years_for_state, list_acs_years_for_county,
    build_delta_sql
)

DB_PATH = "./acs_cache.duckdb"
TABLE   = "acs5_profile"

VALID_STATE_FIPS = {
    "01","02","04","05","06","08","09","10","11","12","13","15","16","17","18","19","20",
    "21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37",
    "38","39","40","41","42","44","45","46","47","48","49","50","51","53","54","55","56",
    "72"  # Puerto Rico
}
_COUNTY_LIST_CACHE: dict[tuple[int, str], dict[str, str]] = {}
_YEARS_CACHE_STATE: dict[tuple[str, int, int], list[int]] = {}
_YEARS_CACHE_COUNTY: dict[tuple[str, str, int, int], list[int]] = {}


def _validate_state_code(code: str):
    if code not in VALID_STATE_FIPS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid state FIPS '{code}'."
        )

def _validate_county_code(state_fips: str, county_fips: str, year: int):
    _validate_state_code(state_fips)
    key = (year, state_fips)
    mapping = _COUNTY_LIST_CACHE.get(key)
    if mapping is None:
        # fetch once and memoize
        mapping = list_counties_for_state(str(year), state_fips)
        _COUNTY_LIST_CACHE[key] = mapping or {}
    if county_fips not in mapping:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid county FIPS '{county_fips}' for state '{state_fips}'. "
        )
        
def _validate_year_for_state(state_fips: str, year: int):
    # probe cached list (2009..year)
    key = (state_fips, 2009, int(YEAR))
    years = _YEARS_CACHE_STATE.get(key)
    if years is None:
        years = list_acs_years_for_state(state_fips, 2009, int(YEAR))
        _YEARS_CACHE_STATE[key] = years
    if year not in years:
        raise HTTPException(
            status_code=400,
            detail=f"Year {year} not available for state {state_fips}. Try one of: {years[-10:] if years else '[]'}"
        )

def _validate_year_for_county(state_fips: str, county_fips: str, year: int):
    key = (state_fips, county_fips, 2009, int(YEAR))
    years = _YEARS_CACHE_COUNTY.get(key)
    if years is None:
        years = list_acs_years_for_county(state_fips, county_fips, 2009, int(YEAR))
        _YEARS_CACHE_COUNTY[key] = years
    if year not in years:
        raise HTTPException(
            status_code=400,
            detail=f"Year {year} not available for county {state_fips}-{county_fips}. Try one of: {years[-10:] if years else '[]'}"
        )

app = FastAPI(title="ACS Cache API", version="0.2")

def _conn():
    con = duckdb.connect(DB_PATH)
    con.execute("PRAGMA threads=4")
    con.execute("SET memory_limit='4GB'")
    return con

def _table_exists(con) -> bool:
    (exists,) = con.execute(
        "SELECT COUNT(*)>0 FROM information_schema.tables WHERE table_name = ?",
        [TABLE]
    ).fetchone()
    return bool(exists)

@app.get("/regions")
def list_regions():
    con = _conn()
    try:
        if not _table_exists(con):
            return JSONResponse(content=[], status_code=200)
        rows = con.execute(f"""
            SELECT geo_level, year, state, county, NAME
            FROM {TABLE}
            ORDER BY year DESC, geo_level, state, county NULLS FIRST
        """).fetchall()
        return [
            {"geo_level": r[0], "year": r[1], "state": r[2], "county": r[3], "NAME": r[4]}
            for r in rows
        ]
    finally:
        con.close()

@app.get("/columns")
def list_columns():
    con = _conn()
    try:
        if not _table_exists(con):
            return JSONResponse(content=[], status_code=200)
        info = con.execute(f"PRAGMA table_info('{TABLE}')").fetchall()
        cols = [row[1] for row in sorted(info, key=lambda r: r[0])]
        return cols
    finally:
        con.close()

@app.get("/states")
def list_states():
    """
    Return all valid state FIPS codes as a sorted list of objects.
    Source is VALID_STATE_FIPS in this file.
    """
    items = [{"state": s} for s in sorted(VALID_STATE_FIPS, key=lambda x: int(x))]
    return items

@app.get("/counties/{state_fips}")
def list_counties(
    state_fips: str,
    year: int | None = Query(None, description="ACS 5-year vintage (defaults to loader's YEAR)")
):
    """
    Return all counties for the given state_fips as [{county, NAME}], sorted by county code.
    Uses loader's list_counties_for_state and memoizes per (year,state).
    """
    yr = int(year if year is not None else YEAR)
    _validate_state_code(state_fips)

    key = (yr, state_fips)
    mapping = _COUNTY_LIST_CACHE.get(key)
    if mapping is None:
        mapping = list_counties_for_state(str(yr), state_fips) or {}
        _COUNTY_LIST_CACHE[key] = mapping

    # Convert dict[str,str] -> list of objects, sorted by numeric county code
    items = [{"county": c, "NAME": mapping[c]} for c in sorted(mapping.keys(), key=lambda x: int(x))]
    return items

@app.get("/years/state/{state_fips}")
def get_years_state(
    state_fips: str,
    start: Optional[int] = Query(None, description="First year to probe (default 2009)"),
    end: Optional[int]   = Query(None, description="Last year to probe (default loader YEAR)")
):
    """
    List ACS acs5/profile years available for the given state (ascending).
    """
    _validate_state_code(state_fips)
    s = int(start) if start is not None else 2009
    e = int(end)   if end   is not None else int(YEAR)
    key = (state_fips, s, e)
    if key not in _YEARS_CACHE_STATE:
        _YEARS_CACHE_STATE[key] = list_acs_years_for_state(state_fips, s, e)
    return _YEARS_CACHE_STATE[key]

@app.get("/years/county/{state_fips}/{county_fips}")
def get_years_county(
    state_fips: str,
    county_fips: str,
    start: Optional[int] = Query(None, description="First year to probe (default 2009)"),
    end: Optional[int]   = Query(None, description="Last year to probe (default loader YEAR)")
):
    """
    List ACS acs5/profile years available for the given county (ascending).
    """
    yr_default = int(YEAR)
    _validate_county_code(state_fips, county_fips, yr_default)  # validates state + county format
    s = int(start) if start is not None else 2009
    e = int(end)   if end   is not None else yr_default
    key = (state_fips, county_fips, s, e)
    if key not in _YEARS_CACHE_COUNTY:
        _YEARS_CACHE_COUNTY[key] = list_acs_years_for_county(state_fips, county_fips, s, e)
    return _YEARS_CACHE_COUNTY[key]


@app.get("/data/state/{state_fips}")
def get_state(
    state_fips: str,
    year: int | None = Query(None, description="ACS 5-year vintage (defaults to loader's YEAR)"),
    query_only: bool = Query(False, description="Return only the SQL, do not execute")
):
    _validate_state_code(state_fips)
    yr = int(year if year is not None else YEAR)
    _validate_year_for_state(state_fips, yr)

    row, from_cache = fetch_or_cache(...)
    sql = write_row_and_get_query(
        row,
        year=yr,
        geo_level="state",
        state_fips=state_fips,
        county_fips=None,
    )

    con = state_conn()
    try:
        rel = con.sql(sql)
        cols = rel.columns
        data = rel.fetchall()
    finally:
        con.close()


@app.get("/data/county/{state_fips}/{county_fips}")
def get_county(
    state_fips: str,
    county_fips: str,
    year: int | None = Query(None, description="ACS 5-year vintage (defaults to loader's YEAR)"),
    query_only: bool = Query(False, description="Return only the SQL, do not execute")
):
    yr = int(year if year is not None else YEAR)
    _validate_county_code(state_fips, county_fips, yr)
    _validate_year_for_county(state_fips, county_fips, yr)

    row, from_cache = fetch_or_cache(...)
    sql = write_row_and_get_query(
        row,
        year=yr,
        geo_level="county",
        state_fips=state_fips,
        county_fips=county_fips,
    )

    con = county_conn(state_fips)
    try:
        rel = con.sql(sql)
        cols = rel.columns
        data = rel.fetchall()
    finally:
        con.close()

        
# ---- Delta endpoints ----
@app.get("/delta/state/{state_fips}")
def delta_state(
    state_fips: str,
    year_a: int = Query(..., description="Baseline ACS 5-year vintage"),
    year_b: int = Query(..., description="Comparison ACS 5-year vintage"),
    query_only: bool = Query(False, description="Return only the SQL, do not execute")
):
    # basic validation
    _validate_state_code(state_fips)
    if year_a == year_b:
        raise HTTPException(status_code=400, detail="year_a and year_b must be different")
    _validate_year_for_state(state_fips, int(year_a))
    _validate_year_for_state(state_fips, int(year_b))

    # Ensure both years exist / cached
    row_a, _ = fetch_or_cache(str(year_a), "state", GROUPS, state_fips=state_fips)
    row_b, _ = fetch_or_cache(str(year_b), "state", GROUPS, state_fips=state_fips)

    # Write both rows (returns point-select SQL; we just ensure the table contains them)
    _ = write_row_and_get_query(row_a, year=int(year_a), geo_level="state", state_fips=state_fips, county_fips=None)
    _ = write_row_and_get_query(row_b, year=int(year_b), geo_level="state", state_fips=state_fips, county_fips=None)

    # Build delta SQL across all DP columns present
    all_cols = sorted(set(row_a.keys()) | set(row_b.keys()))
    sql = build_delta_sql(
        year_a=int(year_a),
        year_b=int(year_b),
        geo_level="state",
        state_fips=state_fips,
        county_fips=None,
        all_columns=all_cols,
    )

    if query_only:
        return {"sql": sql}

    # Execute
    con = _conn()
    try:
        rel = con.sql(sql)
        cols = rel.columns
        data = rel.fetchall()
        if not data:
            return JSONResponse(content={"error": "No rows found for delta"}, status_code=404)
        return dict(zip(cols, data[0]))
    finally:
        con.close()

@app.get("/delta/county/{state_fips}/{county_fips}")
def delta_county(
    state_fips: str,
    county_fips: str,
    year_a: int = Query(..., description="Baseline ACS 5-year vintage"),
    year_b: int = Query(..., description="Comparison ACS 5-year vintage"),
    query_only: bool = Query(False, description="Return only the SQL, do not execute")
):
    if year_a == year_b:
        raise HTTPException(status_code=400, detail="year_a and year_b must be different")

    # Validate geo + years
    _validate_county_code(state_fips, county_fips, int(YEAR))
    _validate_year_for_county(state_fips, county_fips, int(year_a))
    _validate_year_for_county(state_fips, county_fips, int(year_b))

    # Ensure both years exist / cached
    row_a, _ = fetch_or_cache(str(year_a), "county", GROUPS, state_fips=state_fips, county_fips=county_fips)
    row_b, _ = fetch_or_cache(str(year_b), "county", GROUPS, state_fips=state_fips, county_fips=county_fips)

    # Write both rows so DuckDB has them
    _ = write_row_and_get_query(row_a, year=int(year_a), geo_level="county", state_fips=state_fips, county_fips=county_fips)
    _ = write_row_and_get_query(row_b, year=int(year_b), geo_level="county", state_fips=state_fips, county_fips=county_fips)

    # Delta SQL
    all_cols = sorted(set(row_a.keys()) | set(row_b.keys()))
    sql = build_delta_sql(
        year_a=int(year_a),
        year_b=int(year_b),
        geo_level="county",
        state_fips=state_fips,
        county_fips=county_fips,
        all_columns=all_cols,
    )

    if query_only:
        return {"sql": sql}

    # Execute
    con = _conn()
    try:
        rel = con.sql(sql)
        cols = rel.columns
        data = rel.fetchall()
        if not data:
            return JSONResponse(content={"error": "No rows found for delta"}, status_code=404)
        return dict(zip(cols, data[0]))
    finally:
        con.close()


if __name__ == "__main__":
    import os, uvicorn
    port = int(os.getenv("PORT", "32101"))  # change default if you like
    uvicorn.run("api:app", host="0.0.0.0", port=port, reload=True)