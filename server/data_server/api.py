# api.py
# FastAPI data server over the ACS DuckDB cache.
#
# Endpoints:
#   GET /states
#   GET /counties/{state_fips}
#   GET /years/state/{state_fips}
#   GET /years/county/{state_fips}/{county_fips}
#   GET /data/state/{state_fips}?year=
#   GET /data/county/{state_fips}/{county_fips}?year=
#
# Run:  uvicorn api:app --reload --port 32101

from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from data_server.acs_loader import (
    GROUPS,
    YEAR,
    fetch_or_cache,
    get_all_metadata,
    list_counties_for_state,
    list_acs_years_for_state,
    list_acs_years_for_county,
)
from data_server.duck_writer import write_row_and_get_query

# ---------------------------------------------------------------------------
# Valid state FIPS codes (50 states + DC + Puerto Rico)
# ---------------------------------------------------------------------------

VALID_STATE_FIPS = {
    "01","02","04","05","06","08","09","10","11","12","13","15","16","17","18","19","20",
    "21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37",
    "38","39","40","41","42","44","45","46","47","48","49","50","51","53","54","55","56",
    "72",  # Puerto Rico
}

# ---------------------------------------------------------------------------
# In-process caches for geo/year validation (avoids redundant KV lookups)
# ---------------------------------------------------------------------------

_county_list_cache:  dict[tuple[int, str], dict[str, str]] = {}
_years_cache_state:  dict[tuple[str, int, int], list[int]] = {}
_years_cache_county: dict[tuple[str, str, int, int], list[int]] = {}

# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def _validate_state(state_fips: str) -> None:
    if state_fips not in VALID_STATE_FIPS:
        raise HTTPException(status_code=400, detail=f"Invalid state FIPS '{state_fips}'.")


def _validate_county(state_fips: str, county_fips: str, year: int) -> None:
    _validate_state(state_fips)
    key = (year, state_fips)
    if key not in _county_list_cache:
        _county_list_cache[key] = list_counties_for_state(str(year), state_fips) or {}
    if county_fips not in _county_list_cache[key]:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid county FIPS '{county_fips}' for state '{state_fips}'.",
        )


def _validate_year_state(state_fips: str, year: int) -> None:
    key = (state_fips, 2009, int(YEAR))
    if key not in _years_cache_state:
        _years_cache_state[key] = list_acs_years_for_state(state_fips, 2009, int(YEAR))
    if year not in _years_cache_state[key]:
        available = _years_cache_state[key]
        raise HTTPException(
            status_code=400,
            detail=f"Year {year} not available for state {state_fips}. "
                   f"Available: {available[-10:] if available else []}",
        )


def _validate_year_county(state_fips: str, county_fips: str, year: int) -> None:
    key = (state_fips, county_fips, 2009, int(YEAR))
    if key not in _years_cache_county:
        _years_cache_county[key] = list_acs_years_for_county(
            state_fips, county_fips, 2009, int(YEAR)
        )
    if year not in _years_cache_county[key]:
        available = _years_cache_county[key]
        raise HTTPException(
            status_code=400,
            detail=f"Year {year} not available for county {state_fips}-{county_fips}. "
                   f"Available: {available[-10:] if available else []}",
        )


# ---------------------------------------------------------------------------
# Response builder
# ---------------------------------------------------------------------------

def _build_response(
    row: dict[str, str],
    metadata: dict[str, dict],
    geo_level: str,
    year: int,
    from_cache: bool,
) -> dict:
    """
    Combine raw ACS values with per-year variable labels into the response shape:

    {
      "meta": { geo_level, year, state, county, NAME, from_cache },
      "data": [ {"code": ..., "label": ..., "concept": ..., "value": ...}, ... ]
    }

    Variables with no entry in metadata (NAME, state, county, etc.) are placed
    in meta only, not in data.
    """
    meta = {
        "geo_level":  geo_level,
        "year":       year,
        "state":      row.get("state"),
        "county":     row.get("county"),
        "NAME":       row.get("NAME"),
        "from_cache": from_cache,
    }

    data = []
    for code in sorted(row.keys()):
        if code not in metadata:
            continue   # skip geo identifier columns
        info = metadata[code]
        data.append({
            "code":    code,
            "label":   info.get("label", ""),
            "concept": info.get("concept", ""),
            "group":   info.get("group", ""),
            "value":   row[code],
        })

    return {"meta": meta, "data": data}


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="ACS Cache API", version="1.0")


# --- Metadata endpoints ---

@app.get("/states")
def list_states():
    """All valid state FIPS codes, sorted numerically."""
    return [{"state": s} for s in sorted(VALID_STATE_FIPS, key=lambda x: int(x))]


@app.get("/counties/{state_fips}")
def list_counties(
    state_fips: str,
    year: Optional[int] = Query(None, description="ACS vintage (defaults to loader YEAR)"),
):
    """All counties for a state as [{county, NAME}]."""
    _validate_state(state_fips)
    yr = int(year if year is not None else YEAR)

    key = (yr, state_fips)
    if key not in _county_list_cache:
        _county_list_cache[key] = list_counties_for_state(str(yr), state_fips) or {}

    return [
        {"county": c, "NAME": _county_list_cache[key][c]}
        for c in sorted(_county_list_cache[key], key=lambda x: int(x))
    ]


@app.get("/years/state/{state_fips}")
def years_for_state(
    state_fips: str,
    start: Optional[int] = Query(None, description="First year to probe (default 2009)"),
    end:   Optional[int] = Query(None, description="Last year to probe (default loader YEAR)"),
):
    """Available ACS 5-year vintages for a state."""
    _validate_state(state_fips)
    s = int(start if start is not None else 2009)
    e = int(end   if end   is not None else YEAR)
    key = (state_fips, s, e)
    if key not in _years_cache_state:
        _years_cache_state[key] = list_acs_years_for_state(state_fips, s, e)
    return _years_cache_state[key]


@app.get("/years/county/{state_fips}/{county_fips}")
def years_for_county(
    state_fips:  str,
    county_fips: str,
    start: Optional[int] = Query(None, description="First year to probe (default 2009)"),
    end:   Optional[int] = Query(None, description="Last year to probe (default loader YEAR)"),
):
    """Available ACS 5-year vintages for a county."""
    yr_default = int(YEAR)
    _validate_county(state_fips, county_fips, yr_default)
    s = int(start if start is not None else 2009)
    e = int(end   if end   is not None else yr_default)
    key = (state_fips, county_fips, s, e)
    if key not in _years_cache_county:
        _years_cache_county[key] = list_acs_years_for_county(state_fips, county_fips, s, e)
    return _years_cache_county[key]


# --- Data endpoints ---

@app.get("/data/state/{state_fips}")
def get_state(
    state_fips: str,
    year: Optional[int] = Query(None, description="ACS 5-year vintage (defaults to loader YEAR)"),
):
    """
    Return ACS 5-year profile data for a state.

    Response shape:
      {
        "meta": { geo_level, year, state, NAME, from_cache },
        "data": [ { code, label, concept, group, value }, ... ]
      }

    Labels are sourced from the Census metadata for the requested year,
    so they are always accurate for the vintage being returned.
    """
    _validate_state(state_fips)
    yr = int(year if year is not None else YEAR)
    _validate_year_state(state_fips, yr)

    row, from_cache = fetch_or_cache(
        year=str(yr),
        geo_kind="state",
        groups=GROUPS,
        state_fips=state_fips,
    )

    # Persist to DuckDB (upsert — safe to call every time)
    write_row_and_get_query(
        row, year=yr, geo_level="state", state_fips=state_fips
    )

    metadata = get_all_metadata(str(yr), GROUPS)
    return _build_response(row, metadata, "state", yr, from_cache)


@app.get("/data/county/{state_fips}/{county_fips}")
def get_county(
    state_fips:  str,
    county_fips: str,
    year: Optional[int] = Query(None, description="ACS 5-year vintage (defaults to loader YEAR)"),
):
    """
    Return ACS 5-year profile data for a county.

    Response shape:
      {
        "meta": { geo_level, year, state, county, NAME, from_cache },
        "data": [ { code, label, concept, group, value }, ... ]
      }

    Labels are sourced from the Census metadata for the requested year.
    """
    yr = int(year if year is not None else YEAR)
    _validate_county(state_fips, county_fips, yr)
    _validate_year_county(state_fips, county_fips, yr)

    row, from_cache = fetch_or_cache(
        year=str(yr),
        geo_kind="county",
        groups=GROUPS,
        state_fips=state_fips,
        county_fips=county_fips,
    )

    write_row_and_get_query(
        row, year=yr, geo_level="county",
        state_fips=state_fips, county_fips=county_fips,
    )

    metadata = get_all_metadata(str(yr), GROUPS)
    return _build_response(row, metadata, "county", yr, from_cache)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import os
    import uvicorn
    port = int(os.getenv("PORT", "32101"))
    uvicorn.run("api:app", host="0.0.0.0", port=port, reload=True)