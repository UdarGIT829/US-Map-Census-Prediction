# check_all.py (offline version: no HTTP, uses loader + duck_writer directly)

from typing import List, Dict, Any
from time import time
from pathlib import Path
from threading import Lock

# ---- Import shared logic from your existing code ----
from data_server.api import VALID_STATE_FIPS  # same canonical list your API uses
from data_server.acs_loader import list_counties_for_state

from data_server.acs_loader import (
    GROUPS,
    list_acs_years_for_state,
    list_acs_years_for_county,
    list_counties_for_state,
    fetch_or_cache,
)

from data_server.duck_writer import write_row_and_get_query


# ---- User knobs ----
CHECK_YEAR = 2023  # ACS 5-year vintage you want to populate/check

# ---- Success tracking ----
SUCCESS_FILE = Path("check_success.log")
_success_cache = None
_success_lock = Lock()


def _load_success_cache() -> set[str]:
    global _success_cache
    if _success_cache is None:
        if SUCCESS_FILE.exists():
            with SUCCESS_FILE.open("r", encoding="utf-8") as f:
                _success_cache = {line.strip() for line in f if line.strip()}
        else:
            _success_cache = set()
    return _success_cache


def was_success(*parts: object) -> bool:
    """
    Return True if we've previously recorded success for this key.
    parts are joined into a single string key, so always include
    things like year/state/county/etc.
    """
    key = "|".join(str(p) for p in parts)
    cache = _load_success_cache()
    return key in cache


def record_success(*parts: object) -> None:
    """
    Record that the given key has successfully completed.
    No-op if already present.
    """
    key = "|".join(str(p) for p in parts)
    cache = _load_success_cache()
    with _success_lock:
        if key in cache:
            return
        cache.add(key)
        with SUCCESS_FILE.open("a", encoding="utf-8") as f:
            f.write(key + "\n")


# =====================================================================
# Offline equivalents of the old HTTP helper functions
# =====================================================================

def get_states() -> List[str]:
    """
    Offline version of GET /states.
    Uses the same VALID_STATE_FIPS set that the API exposes.
    """
    return [s for s in sorted(VALID_STATE_FIPS, key=lambda x: int(x))]

def get_counties(state_fips: str, year: int) -> List[Dict[str, str]]:
    """
    Offline version of GET /counties/{state_fips}?year=YEAR.

    Uses acs_loader.list_counties_for_state(year, state_fips),
    which returns {county_fips -> NAME}, and converts to the
    list-of-dicts shape used by the old HTTP version:
    [{"county": "001", "NAME": "Foo County"}, ...]
    """
    mapping = list_counties_for_state(str(year), state_fips)  # dict[str, str]

    items: List[Dict[str, str]] = [
        {"county": c, "NAME": name}
        for c, name in sorted(mapping.items(), key=lambda kv: int(kv[0]))
    ]
    return items


def get_years_for_state(state_fips: str) -> List[int]:
    """
    Offline version of GET /years/state/{state_fips}.
    Uses acs_loader.list_acs_years_for_state, which probes the Census API
    and caches results in the KV table.
    """
    # Match API behavior: probe from 2009 up through CHECK_YEAR
    return list_acs_years_for_state(state_fips, 2009, int(CHECK_YEAR))


def get_state_data(state_fips: str, year: int) -> Dict[str, Any]:
    year_str = str(year)
    row, from_cache = fetch_or_cache(year_str, "state", GROUPS, state_fips=state_fips)

    # make sure the row we return has the IDs
    row = dict(row)
    row["geo_level"] = "state"
    row["year"] = int(year)
    row["state"] = str(state_fips).zfill(2)

    _ = write_row_and_get_query(
        row,
        year=year,
        geo_level="state",
        state_fips=state_fips,
        county_fips=None,
    )
    return row


def get_county_data(state_fips: str, county_fips: str, year: int) -> Dict[str, Any]:
    year_str = str(year)
    row, from_cache = fetch_or_cache(
        year_str, "county", GROUPS,
        state_fips=state_fips, county_fips=county_fips,
    )

    row = dict(row)
    row["geo_level"] = "county"
    row["year"] = int(year)
    row["state"] = str(state_fips).zfill(2)
    row["county"] = str(county_fips).zfill(3)

    _ = write_row_and_get_query(
        row,
        year=year,
        geo_level="county",
        state_fips=state_fips,
        county_fips=county_fips,
    )
    return row



def get_years_for_county(state_fips: str, county_fips: str) -> List[int]:
    """
    Offline version of GET /years/county/{state_fips}/{county_fips}.
    """
    return list_acs_years_for_county(state_fips, county_fips, 2009, int(CHECK_YEAR))


def get_county_data(state_fips: str, county_fips: str, year: int) -> Dict[str, Any]:
    """
    Offline version of GET /data/county/{state_fips}/{county_fips}?year=YEAR.

    1) fetch_or_cache(...) to get the wide row
    2) write_row_and_get_query(...) to persist into acs5_profile
    3) Return the row dict directly
    """
    year_str = str(year)
    row, from_cache = fetch_or_cache(
        year_str,
        "county",
        GROUPS,
        state_fips=state_fips,
        county_fips=county_fips,
    )
    _ = write_row_and_get_query(
        row,
        year=year,
        geo_level="county",
        state_fips=state_fips,
        county_fips=county_fips,
    )
    return row


# =====================================================================
# Main phases (same structure as your original script)
# =====================================================================

def main():
    print(f"[info] Offline check using loader + duck_writer")
    print(f"[info] Checking availability of year: {CHECK_YEAR}\n")

    # -------- Phase 1 --------
    print("Starting Phase 1 Timer...")
    _startTime_P1 = time()

    try:
        states = get_states()
    except Exception as e:
        print(f"[error] Failed to get states offline: {e}")
        return
    if not states:
        print("[warn] get_states() returned an empty list.")
        return

    ok, missing = [], []
    for s in sorted(states, key=lambda x: int(x)):
        try:
            years = get_years_for_state(s)
        except Exception as e:
            print(f"[warn] years for state {s} error: {e}")
            missing.append((s, []))
            continue

        if CHECK_YEAR in years:
            ok.append(s)
            tail = years[-5:] if len(years) > 5 else years
            print(f"✓ state {s}: year {CHECK_YEAR} is available (sample tail={tail})")
        else:
            missing.append((s, years))
            tail = years[-5:] if isinstance(years, list) and len(years) > 5 else years
            print(f"✗ state {s}: year {CHECK_YEAR} NOT available (sample tail={tail})")

    print("Ending Phase 1 Timer...")
    _endTime_P1 = time()

    print("\n===== SUMMARY (Pass 1) =====")
    print(f"Elapsed Time: {_endTime_P1-_startTime_P1:.2f} seconds")
    print(f"Total states: {len(states)}")
    print(f"Available for {CHECK_YEAR}: {len(ok)}")
    print(f"Missing for {CHECK_YEAR}: {len(missing)}")
    if missing:
        print("\nStates missing the year (with sample of probed years):")
        for s, yrs in missing:
            tail = yrs[-5:] if isinstance(yrs, list) and len(yrs) > 5 else yrs
            print(f"  - {s}: {tail}")

    # -------- Phase 2 --------
    print(f"\n[info] Fetching ACS *state* data rows for {CHECK_YEAR} across {len(ok)} states...\n")
    print("Starting Phase 2 Timer...")
    _startTime_P2 = time()

    fetch_ok: List[str] = []
    fetch_fail: List[tuple[str, str]] = []

    for s in sorted(ok, key=lambda x: int(x)):
        # --- skip if already done ---
        if was_success("state_data", CHECK_YEAR, s):
            print(f"[skip] data state {s}: already recorded as success for {CHECK_YEAR}")
            continue

        try:
            row = get_state_data(s, CHECK_YEAR)
        except Exception as e:
            print(f"[fail] state {s}: error {e}")
            fetch_fail.append((s, str(e)))
            continue

        year_ok = str(row.get("year")) == str(CHECK_YEAR)
        state_ok = str(row.get("state")) == str(s)
        geo_ok = row.get("geo_level") == "state"
        dp_count = len([k for k in row.keys() if k.startswith("DP")])
        name_val = row.get("NAME")

        if year_ok and state_ok and geo_ok:
            fetch_ok.append(s)
            print(f"✓ data state {s}: got row (NAME={name_val!r}, DP_vars≈{dp_count})")
            record_success("state_data", CHECK_YEAR, s)
        else:
            reason = (
                f"unexpected identifiers geo={row.get('geo_level')} "
                f"state={row.get('state')} year={row.get('year')}"
            )
            print(f"[warn] data state {s}: {reason}")
            fetch_fail.append((s, reason))

    print("Ending Phase 2 Timer...")
    _endTime_P2 = time()

    print("\n===== SUMMARY (Pass 2) =====")
    print(f"Elapsed Time: {_endTime_P2-_startTime_P2:.2f} seconds")
    print(f"Fetched OK: {len(fetch_ok)} / {len(ok)}")
    if fetch_fail:
        print("Failures:")
        for s, why in fetch_fail:
            print(f"  - {s}: {why}")

    # -------- Phase 3 --------
    print(f"\n[info] Fetching *county* rows for {CHECK_YEAR} (all states in OK)...\n")
    print("Starting Phase 3 Timer...")
    _startTime_P3 = time()

    county_total = 0
    county_ok = 0
    county_fail = 0

    for s in sorted(ok, key=lambda x: int(x)):
        try:
            counties = get_counties(s, CHECK_YEAR)  # [{'county': '001', 'NAME': '...'}, ...]
        except Exception as e:
            print(f"[warn] counties for {s} (year {CHECK_YEAR}) error: {e}")
            continue

        for c in counties:
            county_fips = c.get("county")
            if not county_fips:
                continue

            county_total += 1

            # skip if this county/year already succeeded
            if was_success("county_data", CHECK_YEAR, s, county_fips):
                print(f"[skip] county {s}-{county_fips}: already recorded as success for {CHECK_YEAR}")
                continue

            # Check availability for county before fetching/ingesting
            try:
                cyears = get_years_for_county(s, county_fips)
            except Exception as e:
                print(f"[fail] years for {s}-{county_fips}: {e}")
                county_fail += 1
                continue

            if CHECK_YEAR not in cyears:
                # skip cleanly; not an error
                continue

            try:
                crow = get_county_data(s, county_fips, CHECK_YEAR)
            except Exception as e:
                print(f"[fail] county {s}-{county_fips}: error {e}")
                county_fail += 1
                continue

            # sanity checks (same as before)
            ok_geo = crow.get("geo_level") == "county"
            ok_state = str(crow.get("state")) == str(s)
            ok_year = str(crow.get("year")) == str(CHECK_YEAR)
            ok_county = str(crow.get("county")) == str(county_fips)
            if ok_geo and ok_state and ok_year and ok_county:
                county_ok += 1
                print(f"✓ county {s}-{county_fips}: NAME={crow.get('NAME')!r}")
                record_success("county_data", CHECK_YEAR, s, county_fips)
            else:
                county_fail += 1
                print(
                    f"[warn] county {s}-{county_fips}: bad ids "
                    f"geo={crow.get('geo_level')} state={crow.get('state')} "
                    f"county={crow.get('county')} year={crow.get('year')}"
                )

    print("Ending Phase 3 Timer...")
    _endTime_P3 = time()

    print("\n===== SUMMARY (Pass 3) =====")
    print(f"Elapsed Time: {_endTime_P3-_startTime_P3:.2f} seconds")
    print(f"Counties attempted (incl. skipped for year): {county_total}")
    print(f"County fetch OK: {county_ok}")
    print(f"County fetch FAIL: {county_fail}")


if __name__ == "__main__":
    main()
