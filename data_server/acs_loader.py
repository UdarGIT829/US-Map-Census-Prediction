import os, json, time, pathlib, urllib.parse, urllib.request
from duck_writer import write_row_and_get_query
from duck_writer import kv_get, kv_set
import urllib.error, http.client, random

RETRY_MAX = 5
RETRY_BASE = 0.5  # seconds

# ---- user knobs ----
YEAR = "2023"                  # ACS 5-year vintage
STATE_FIPS = "06"              # California
COUNTY_FIPS = "059"            # Orange County
GROUPS = ["DP02","DP03","DP04","DP05"]
BATCH_SIZE = 45
CACHE_DIR = pathlib.Path("./cache")
API_KEY = "2d9aff51fb3edc58c7db726bdb39cf02dd96aedb"   # optional; speeds up and raises limits


# ---- KV Loaders ----
def _kv_load_json(key: str):
    s = kv_get(key)
    return None if s is None else json.loads(s)

def _kv_save_json(key: str, obj):
    kv_set(key, json.dumps(obj, ensure_ascii=False))


# ---- HTTP helpers ----
def _read_json(url):
    last_err = None
    for attempt in range(1, RETRY_MAX + 1):
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                return json.loads(r.read().decode("utf-8"))
        except (urllib.error.URLError, http.client.RemoteDisconnected, urllib.error.HTTPError) as e:
            # Only retry on 5xx/429 or low-level disconnects
            if isinstance(e, urllib.error.HTTPError) and e.code not in (429, 500, 502, 503, 504):
                raise
            last_err = e
            sleep = RETRY_BASE * (2 ** (attempt - 1)) + random.random() * 0.2
            time.sleep(sleep)
    # after retries, bubble up
    raise last_err

def _census(base, params):
    qs = urllib.parse.urlencode(params)
    return _read_json(f"{base}?{qs}")

# ---- variable discovery ----
def get_group_variables(year, group):
    cache_key = f"groupvars:{year}:{group}"
    cached = _kv_load_json(cache_key)
    if cached is not None:
        return cached

    meta_url = f"https://api.census.gov/data/{year}/acs/acs5/profile/groups/{group}.json"
    meta = _read_json(meta_url)
    vars_all = [vn for vn in meta.get("variables", {}) if vn.startswith(f"{group}_")]
    vars_all.sort()

    _kv_save_json(cache_key, vars_all)
    return vars_all

def discover_all_vars(year, groups):
    all_vars = []
    for g in groups:
        print(f"[meta] discovering {g} …")
        gvars = get_group_variables(year, g)
        print(f"       {g}: {len(gvars)} vars")
        all_vars.extend(gvars)
    return all_vars

def list_counties_for_state(year: str, state_fips: str) -> dict[str, str]:
    """
    Return {county_fips -> NAME} for all counties in the given state, from ACS profile.
    Example call made:
      GET /data/{year}/acs/acs5/profile?get=NAME&for=county:*&in=state:{state_fips}
    """
    cache_key = f"counties:{year}:{state_fips}"
    cached = _kv_load_json(cache_key)
    if cached is not None:
        return cached  # already dict[str,str]

    base = f"https://api.census.gov/data/{year}/acs/acs5/profile"
    params = {"get": "NAME", "for": "county:*", "in": f"state:{state_fips}"}
    if API_KEY:
        params["key"] = API_KEY

    data = _census(base, params)
    if not data or len(data) < 2:
        return {}

    headers = data[0]
    name_idx = headers.index("NAME")
    county_idx = headers.index("county")

    result: dict[str, str] = {}
    for row in data[1:]:
        county = row[county_idx]
        name = row[name_idx]
        result[county] = name

    _kv_save_json(cache_key, result)
    return result

# --- Get possible years from ACS ---

START_YEAR_DEFAULT = 2009  # ACS 5-year profile earliest practical vintage

def _probe_year(year: int, geo_kind: str, state_fips: str, county_fips: str | None) -> bool:
    """
    Return True if ACS acs5/profile responds with a valid JSON row for the given year+geo.
    We keep the query tiny: get=NAME for the specific geo.
    """
    base = f"https://api.census.gov/data/{year}/acs/acs5/profile"
    if geo_kind == "state":
        params = {"get": "NAME", "for": f"state:{state_fips}"}
    elif geo_kind == "county":
        params = {"get": "NAME", "for": f"county:{county_fips}", "in": f"state:{state_fips}"}
    else:
        return False
    if API_KEY:
        params["key"] = API_KEY
    try:
        data = _census(base, params)
        return bool(data) and len(data) >= 2
    except Exception:
        return False

def list_acs_years_for_state(state_fips: str, start_year: int | None = None, end_year: int | None = None) -> list[int]:
    """
    Probe acs/acs5/profile for the given state and return all available years (ascending).
    """
    s = int(start_year if start_year is not None else START_YEAR_DEFAULT)
    e = int(end_year if end_year is not None else int(YEAR))
    cache_key = f"years:state:{state_fips}:{s}:{e}"
    cached = _kv_load_json(cache_key)
    if cached is not None:
        return cached

    years = [y for y in range(s, e + 1) if _probe_year(y, "state", state_fips, None)]
    _kv_save_json(cache_key, years)
    return years

def list_acs_years_for_county(state_fips: str, county_fips: str, start_year: int | None = None, end_year: int | None = None) -> list[int]:
    """
    Probe acs/acs5/profile for the given county and return all available years (ascending).
    """
    s = int(start_year if start_year is not None else START_YEAR_DEFAULT)
    e = int(end_year if end_year is not None else int(YEAR))
    cache_key = f"years:county:{state_fips}:{county_fips}:{s}:{e}"
    cached = _kv_load_json(cache_key)
    if cached is not None:
        return cached

    years = [y for y in range(s, e + 1) if _probe_year(y, "county", state_fips, county_fips)]
    _kv_save_json(cache_key, years)
    return years


# ---- generic fetcher (state/county) ----
def fetch_vars(year, varnames, geo_kind, state_fips=None, county_fips=None, batch_size=BATCH_SIZE):
    base = f"https://api.census.gov/data/{year}/acs/acs5/profile"
    out = {}
    for i in range(0, len(varnames), batch_size):
        chunk = varnames[i:i+batch_size]
        get_list = ["NAME"] + chunk
        params = {"get": ",".join(get_list)}
        if geo_kind == "state":
            params["for"] = f"state:{state_fips}"
        elif geo_kind == "county":
            params["for"] = f"county:{county_fips}"
            params["in"] = f"state:{state_fips}"
        else:
            raise ValueError("geo_kind must be 'state' or 'county'")
        if API_KEY:
            params["key"] = API_KEY
        data = _census(base, params)
        headers, values = data[0], data[1]
        out.update(dict(zip(headers, values)))
    return out

# ---- Delta fetcher ----
def build_delta_sql(
    *,
    year_a: int,
    year_b: int,
    geo_level: str,          # 'state' | 'county'
    state_fips: str,
    county_fips: str | None,
    all_columns: list[str],
    table: str = "acs5_profile"
) -> str:
    """
    Build a single SELECT that returns deltas (year_b - year_a) for every DPxx_* column.
    Includes identifiers + both years for reference.
    `all_columns` should include all keys from a fetched row; we filter to DP* here.
    """
    # keep only ACS DP variables
    dp_cols = [c for c in all_columns if c.startswith("DP")]
    dp_cols.sort()

    # delta expressions
    delta_exprs = []
    for c in dp_cols:
        # Use quoted identifiers and try_cast to avoid type issues
        delta_exprs.append(
            f"try_cast(b.\"{c}\" AS DOUBLE) - try_cast(a.\"{c}\" AS DOUBLE) AS \"{c}__delta\""
        )

    # join condition & where by geo
    join_on = "a.geo_level=b.geo_level AND a.state=b.state AND coalesce(a.county,'')=coalesce(b.county,'')"
    where_geo = ["a.geo_level='{gl}'".format(gl=geo_level)]
    if geo_level == "state":
        where_geo.append(f"a.state='{state_fips}'")
    else:
        where_geo.append(f"a.state='{state_fips}'")
        where_geo.append(f"a.county='{county_fips}'")
    where_geo.append(f"a.year={int(year_a)}")
    where_geo.append(f"b.year={int(year_b)}")

    select_id = [
        "a.geo_level AS geo_level",
        "a.state AS state",
        "a.county AS county",
        "a.year AS year_a",
        "b.year AS year_b",
        "a.NAME AS NAME_a",
        "b.NAME AS NAME_b",
    ]

    sql = f"""
SELECT
  {", ".join(select_id)}
  , {", ".join(delta_exprs)}
FROM {table} a
JOIN {table} b
  ON {join_on}
WHERE {" AND ".join(where_geo)}
"""
    return sql.strip()

# ---- cache helpers ----
def cache_path(year, geo_kind, groups, state_fips, county_fips=None):
    gtag = "-".join(groups)
    if geo_kind == "state":
        name = f"acs5_profile_{year}_state{state_fips}_{gtag}.json"
    else:
        name = f"acs5_profile_{year}_state{state_fips}_county{county_fips}_{gtag}.json"
    return CACHE_DIR / name

def _row_cache_key(year, geo_kind, groups, state_fips, county_fips=None) -> str:
    gtag = "-".join(groups)
    if geo_kind == "state":
        return f"row:{year}:state:{state_fips}:{gtag}"
    else:
        return f"row:{year}:county:{state_fips}:{county_fips}:{gtag}"

def fetch_or_cache(year, geo_kind, groups, state_fips, county_fips=None):
    """
    Return the wide ACS row from DuckDB-backed KV cache; if missing, fetch,
    then persist the JSON blob to KV and return it. No filesystem writes.
    """
    key = _row_cache_key(year, geo_kind, groups, state_fips, county_fips)
    cached = _kv_load_json(key)
    if cached is not None:
        print(f"[cache] hit(db): {key}")
        return cached, True

    t0 = time.time()
    all_vars = discover_all_vars(year, groups)
    if geo_kind == "state":
        row = fetch_vars(year, all_vars, "state", state_fips=state_fips)
    else:
        row = fetch_vars(year, all_vars, "county", state_fips=state_fips, county_fips=county_fips)

    ordered = {k: row[k] for k in sorted(row.keys())}
    _kv_save_json(key, ordered)
    print(f"[cache] saved(db) {key}  (columns={len(ordered)}, {time.time()-t0:.2f}s)")
    return ordered, False

if __name__ == "__main__":
    # Print ready-to-run curl examples for all endpoints.
    # Assumes you start the API with:  uvicorn api:app --reload
    BASE = "http://127.0.0.1:32101"

    # Canonical examples (California + Orange County)
    EX_STATE = "06"   # California
    EX_COUNTY = "059" # Orange County

    try:
        # Import YEAR from loader (string like "2023"); compute a prior year for delta demos
        from minimal_acs_loader_both import YEAR as _LOADER_YEAR
        curr_year = int(_LOADER_YEAR)
    except Exception:
        curr_year = 2023  # sensible fallback

    # pick an earlier year safely (no earlier than 2009)
    prev_year = max(2009, curr_year - 5)

    examples = [
        "# --- Metadata ---",
        f"curl '{BASE}/states'",
        f"curl '{BASE}/regions'",
        f"curl '{BASE}/columns'",
        f"curl '{BASE}/counties/{EX_STATE}'",
        f"curl '{BASE}/counties/{EX_STATE}?year={curr_year}'",

        "# --- Year discovery ---",
        f"curl '{BASE}/years/state/{EX_STATE}'",
        f"curl '{BASE}/years/state/{EX_STATE}?start=2009&end={curr_year}'",
        f"curl '{BASE}/years/county/{EX_STATE}/{EX_COUNTY}'",
        f"curl '{BASE}/years/county/{EX_STATE}/{EX_COUNTY}?start=2009&end={curr_year}'",

        "# --- Data (point-in-time) ---",
        f"curl '{BASE}/data/state/{EX_STATE}?year={curr_year}'",
        f"curl '{BASE}/data/state/{EX_STATE}?year={curr_year}&query_only=true'",
        f"curl '{BASE}/data/county/{EX_STATE}/{EX_COUNTY}?year={curr_year}'",
        f"curl '{BASE}/data/county/{EX_STATE}/{EX_COUNTY}?year={curr_year}&query_only=true'",

        "# --- Deltas (year_b - year_a) ---",
        f"curl '{BASE}/delta/state/{EX_STATE}?year_a={prev_year}&year_b={curr_year}'",
        f"curl '{BASE}/delta/state/{EX_STATE}?year_a={prev_year}&year_b={curr_year}&query_only=true'",
        f"curl '{BASE}/delta/county/{EX_STATE}/{EX_COUNTY}?year_a={prev_year}&year_b={curr_year}'",
        f"curl '{BASE}/delta/county/{EX_STATE}/{EX_COUNTY}?year_a={prev_year}&year_b={curr_year}&query_only=true'",
    ]

    print("\n".join(examples))