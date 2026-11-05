import json
import os
import time
import pathlib
import urllib.parse
import urllib.request
import urllib.error
import http.client
import random

from data_server.duck_writer import kv_get, kv_set

# ---------------------------------------------------------------------------
# Retry config
# ---------------------------------------------------------------------------
RETRY_MAX = 5
RETRY_BASE = 0.5  # seconds

# ---------------------------------------------------------------------------
# User knobs
# ---------------------------------------------------------------------------
YEAR = "2023"                               # default ACS 5-year vintage
GROUPS = ["DP02", "DP03", "DP04", "DP05"]
BATCH_SIZE = 45
# Free key: https://api.census.gov/data/key_signup.html
# Read from the environment so it never lands in source control. Without a
# key the Census API still answers, but with a much lower request limit.
API_KEY = os.environ.get("CENSUS_API_KEY", "").strip()

if not API_KEY:
    print(
        "WARNING: CENSUS_API_KEY is not set. Census API requests will be "
        "unauthenticated and may be rate-limited on a cold cache."
    )

START_YEAR_DEFAULT = 2009   # earliest practical ACS 5-year profile vintage

# ---------------------------------------------------------------------------
# KV helpers
# ---------------------------------------------------------------------------

def _kv_load_json(key: str):
    s = kv_get(key)
    return None if s is None else json.loads(s)


def _kv_save_json(key: str, obj):
    kv_set(key, json.dumps(obj, ensure_ascii=False))


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _read_json(url: str):
    """Fetch a URL and return parsed JSON, with exponential-backoff retries."""
    last_err = None
    for attempt in range(1, RETRY_MAX + 1):
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                return json.loads(r.read().decode("utf-8"))
        except (urllib.error.URLError, http.client.RemoteDisconnected, urllib.error.HTTPError) as e:
            if isinstance(e, urllib.error.HTTPError) and e.code not in (429, 500, 502, 503, 504):
                raise
            last_err = e
            sleep = RETRY_BASE * (2 ** (attempt - 1)) + random.random() * 0.2
            time.sleep(sleep)
    raise last_err


def _census(base: str, params: dict):
    qs = urllib.parse.urlencode(params)
    return _read_json(f"{base}?{qs}")


# ---------------------------------------------------------------------------
# Variable discovery  (names only — used internally by fetch_or_cache)
# ---------------------------------------------------------------------------

def get_group_variables(year: str, group: str) -> list[str]:
    """
    Return sorted list of variable names for a DP group in a given year.
    Result is KV-cached under  groupvars:{year}:{group}
    """
    cache_key = f"groupvars:{year}:{group}"
    cached = _kv_load_json(cache_key)
    if cached is not None:
        return cached

    meta_url = f"https://api.census.gov/data/{year}/acs/acs5/profile/groups/{group}.json"
    meta = _read_json(meta_url)
    var_names = sorted(
        vn for vn in meta.get("variables", {})
        if vn.startswith(f"{group}_")
    )

    _kv_save_json(cache_key, var_names)
    return var_names


def _discover_all_var_names(year: str, groups: list[str]) -> list[str]:
    """Flat list of all variable names across every group."""
    all_vars = []
    for g in groups:
        all_vars.extend(get_group_variables(year, g))
    return all_vars


# ---------------------------------------------------------------------------
# Metadata (labels + concepts)  — NEW
# ---------------------------------------------------------------------------

def get_group_metadata(year: str, group: str) -> dict[str, dict]:
    """
    Return {varname: {"label": ..., "concept": ...}} for one DP group/year.
    KV-cached under  groupmeta:{year}:{group}

    This is separate from get_group_variables so that callers who only need
    variable names don't pay for the extra JSON parsing.
    """
    cache_key = f"groupmeta:{year}:{group}"
    cached = _kv_load_json(cache_key)
    if cached is not None:
        return cached

    meta_url = f"https://api.census.gov/data/{year}/acs/acs5/profile/groups/{group}.json"
    meta = _read_json(meta_url)

    result = {}
    for vname, vmeta in meta.get("variables", {}).items():
        if not vname.startswith(f"{group}_"):
            continue
        result[vname] = {
            "label":   vmeta.get("label", "").strip(),
            "concept": vmeta.get("concept", "").strip(),
        }

    _kv_save_json(cache_key, result)
    return result


def get_all_metadata(year: str, groups: list[str] | None = None) -> dict[str, dict]:
    """
    Return {varname: {"label": ..., "concept": ..., "group": ...}}
    for every variable across all requested groups in the given year.
    KV-cached as a single blob under  allmeta:{year}:{group_tag}
    """
    if groups is None:
        groups = GROUPS

    gtag = "-".join(groups)
    cache_key = f"allmeta:{year}:{gtag}"
    cached = _kv_load_json(cache_key)
    if cached is not None:
        return cached

    combined = {}
    for g in groups:
        group_meta = get_group_metadata(year, g)
        for vname, info in group_meta.items():
            combined[vname] = {
                "label":   info["label"],
                "concept": info["concept"],
                "group":   g,
            }

    _kv_save_json(cache_key, combined)
    return combined


# ---------------------------------------------------------------------------
# Geography helpers
# ---------------------------------------------------------------------------

def list_counties_for_state(year: str, state_fips: str) -> dict[str, str]:
    """
    Return {county_fips: NAME} for every county in the given state.
    KV-cached under  counties:{year}:{state_fips}
    """
    cache_key = f"counties:{year}:{state_fips}"
    cached = _kv_load_json(cache_key)
    if cached is not None:
        return cached

    base = f"https://api.census.gov/data/{year}/acs/acs5/profile"
    params = {"get": "NAME", "for": "county:*", "in": f"state:{state_fips}"}
    if API_KEY:
        params["key"] = API_KEY

    data = _census(base, params)
    if not data or len(data) < 2:
        return {}

    headers = data[0]
    name_idx   = headers.index("NAME")
    county_idx = headers.index("county")

    result = {row[county_idx]: row[name_idx] for row in data[1:]}
    _kv_save_json(cache_key, result)
    return result


def _probe_year(year: int, geo_kind: str, state_fips: str, county_fips: str | None) -> bool:
    """Return True if ACS acs5/profile has data for year+geo."""
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


def list_acs_years_for_state(
    state_fips: str,
    start_year: int | None = None,
    end_year: int | None = None,
) -> list[int]:
    """
    Return sorted list of ACS 5-year vintages available for a state.
    KV-cached under  years:state:{state_fips}:{s}:{e}
    """
    s = int(start_year if start_year is not None else START_YEAR_DEFAULT)
    e = int(end_year   if end_year   is not None else int(YEAR))
    cache_key = f"years:state:{state_fips}:{s}:{e}"
    cached = _kv_load_json(cache_key)
    if cached is not None:
        return cached

    years = [y for y in range(s, e + 1) if _probe_year(y, "state", state_fips, None)]
    _kv_save_json(cache_key, years)
    return years


def list_acs_years_for_county(
    state_fips: str,
    county_fips: str,
    start_year: int | None = None,
    end_year: int | None = None,
) -> list[int]:
    """
    Return sorted list of ACS 5-year vintages available for a county.
    KV-cached under  years:county:{state_fips}:{county_fips}:{s}:{e}
    """
    s = int(start_year if start_year is not None else START_YEAR_DEFAULT)
    e = int(end_year   if end_year   is not None else int(YEAR))
    cache_key = f"years:county:{state_fips}:{county_fips}:{s}:{e}"
    cached = _kv_load_json(cache_key)
    if cached is not None:
        return cached

    years = [y for y in range(s, e + 1) if _probe_year(y, "county", state_fips, county_fips)]
    _kv_save_json(cache_key, years)
    return years


# ---------------------------------------------------------------------------
# Data fetcher
# ---------------------------------------------------------------------------

def _fetch_vars(
    year: str,
    varnames: list[str],
    geo_kind: str,
    state_fips: str | None = None,
    county_fips: str | None = None,
    batch_size: int = BATCH_SIZE,
) -> dict[str, str]:
    """
    Fetch raw ACS values for `varnames` from the Census API.
    Returns {varname: raw_value} (values are strings as the API returns them).
    """
    base = f"https://api.census.gov/data/{year}/acs/acs5/profile"
    out = {}

    for i in range(0, len(varnames), batch_size):
        chunk = varnames[i : i + batch_size]
        params = {"get": ",".join(["NAME"] + chunk)}

        if geo_kind == "state":
            params["for"] = f"state:{state_fips}"
        elif geo_kind == "county":
            params["for"] = f"county:{county_fips}"
            params["in"]  = f"state:{state_fips}"
        else:
            raise ValueError(f"geo_kind must be 'state' or 'county', got {geo_kind!r}")

        if API_KEY:
            params["key"] = API_KEY

        data = _census(base, params)
        headers, values = data[0], data[1]
        out.update(dict(zip(headers, values)))

    return out


def _row_cache_key(
    year: str,
    geo_kind: str,
    groups: list[str],
    state_fips: str,
    county_fips: str | None = None,
) -> str:
    gtag = "-".join(groups)
    if geo_kind == "state":
        return f"row:{year}:state:{state_fips}:{gtag}"
    return f"row:{year}:county:{state_fips}:{county_fips}:{gtag}"


def fetch_or_cache(
    year: str,
    geo_kind: str,
    groups: list[str] | None = None,
    state_fips: str | None = None,
    county_fips: str | None = None,
) -> tuple[dict[str, str], bool]:
    """
    Return (row_dict, from_cache).

    row_dict  — {varname: raw_value} for every DP variable in `groups`
                plus NAME, state, county geo identifiers.
    from_cache — True if the result came from KV, False if fetched live.

    The result is stored in KV so subsequent calls are instant.
    """
    if groups is None:
        groups = GROUPS

    key = _row_cache_key(year, geo_kind, groups, state_fips, county_fips)
    cached = _kv_load_json(key)
    if cached is not None:
        return cached, True

    t0 = time.time()
    all_var_names = _discover_all_var_names(year, groups)

    row = _fetch_vars(
        year, all_var_names, geo_kind,
        state_fips=state_fips,
        county_fips=county_fips,
    )

    ordered = {k: row[k] for k in sorted(row.keys())}
    _kv_save_json(key, ordered)
    print(f"[acs_loader] fetched {key}  columns={len(ordered)}  {time.time()-t0:.2f}s")
    return ordered, False