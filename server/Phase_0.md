PHASE 0 — ACS Data Server
=========================
Last updated: 2026-04-11


OVERVIEW
--------
Phase 0 is the data foundation for the entire project. It is responsible for
fetching, caching, and serving American Community Survey (ACS) 5-year profile
data from the U.S. Census Bureau API. All downstream phases (correlation
screening, recipe sampling, ML) depend on Phase 0 having clean, correctly
labelled data available.

Phase 0 does not do any analysis. It is purely infrastructure.


WHAT THE ACS DATA IS
--------------------
The ACS (American Community Survey) is an ongoing annual survey run by the
U.S. Census Bureau. It is NOT the Decennial Census (which runs every 10 years).

We use the ACS 5-year estimates product (acs5/profile), which pools five years
of survey responses into a single estimate to improve statistical reliability
for smaller geographies. For example, "2023" data represents survey respondents
from 2019 through 2023.

We pull four Data Profile groups:
  DP02 — Social characteristics (education, language, households)
  DP03 — Economic characteristics (income, employment, poverty)
  DP04 — Housing characteristics (occupancy, costs, structure type)
  DP05 — Demographic characteristics (age, sex, race, ethnicity)

Each group contains several hundred variables. Across all four groups, a single
geography/year combination returns approximately 4224 variables total.


VARIABLE SUFFIX TYPES
---------------------
Every ACS concept is published in multiple forms. For a concept like
"total population" the API returns all of the following:

  DP05_0001E   — Estimate (raw count)              <- KEEP for analysis
  DP05_0001PE  — Percent Estimate                  <- KEEP for analysis
  DP05_0001M   — Margin of Error on E              <- DROP (precision metadata)
  DP05_0001PM  — Margin of Error on PE             <- DROP (precision metadata)
  DP05_0001EA  — Annotation on E                   <- DROP (suppression codes)
  DP05_0001MA  — Annotation on M                   <- DROP (suppression codes)
  DP05_0001PEA — Annotation on PE                  <- DROP (suppression codes)
  DP05_0001PMA — Annotation on PM                  <- DROP (suppression codes)

This means roughly 1/4 of the ~4224 columns are real analysis values (E and PE)
and the other 3/4 are metadata. Downstream phases must filter to E and PE
columns only before building any feature matrix.

Annotation values are Census Bureau codes like:
  (X)  — not applicable for this geography or concept
  **   — value suppressed (sample too small to report reliably)
  -    — value rounds to zero or is not available
  N    — not available for this geography type

None values in the data (Python None, null in JSON) indicate that the Census
API returned no value for that variable for that geography. This is expected
and meaningful — it is NOT the same as zero. Do not impute None as 0.0.


FILE STRUCTURE
--------------
data_server/
  acs_loader.py     — Census API client, KV cache logic, all fetch functions
  duck_writer.py    — DuckDB table management, upsert logic, KV get/set
  duck_router.py    — DuckDB file path routing (state DB vs per-state county DB)
  db/               — DuckDB files written at runtime (not in source control)
    acs_states.duckdb          — all state-level ACS rows
    acs_counties_{fips}.duckdb — county rows, one file per state

api.py              — FastAPI HTTP server over the cache
test_phase0.py      — Phase 0 test harness (see TESTING section below)

The KV cache (acs_cache.duckdb) lives at the project root. It stores:
  - Raw fetched ACS rows (JSON blobs), keyed by year/geo/groups
  - Variable metadata (labels and concepts), keyed by year/group
  - Geography discovery results (county lists, available years)

The KV cache is the primary cache. DuckDB tables are a secondary structured
store that downstream phases can query with SQL if needed.


REQUEST FLOW
------------
When a data endpoint is called (e.g. GET /data/state/06?year=2023):

  1. api.py validates the state FIPS code and year
  2. api.py calls acs_loader.fetch_or_cache()
       a. fetch_or_cache checks KV cache for the row
       b. Cache hit  → return immediately (fast, no network)
       c. Cache miss → fetch from Census API in batches of 45 variables,
                       store result in KV, then return
  3. api.py calls acs_loader.get_all_metadata() for the same year
       a. Same KV cache pattern — hits Census API only on first request
          for that year/group combination
  4. api.py calls duck_writer.write_row_and_get_query()
       a. Upserts the row into the appropriate DuckDB table
          (DELETE existing row for that geo/year key, then INSERT)
       b. This is idempotent — safe to call on every request
  5. api.py calls _build_response() to zip data with labels
  6. Response returned as JSON

Variable labels are fetched for the SAME YEAR as the data. This is critical
because ACS variable definitions and labels change between vintages, especially
across the 2019→2020 boundary (major race/ethnicity question redesign) and
the 2022→2023 boundary (new DP05 rows added, tail of DP05 renumbered — several
variable codes changed meaning entirely, not just their labels).


API ENDPOINTS
-------------
GET /states
    Returns all valid state FIPS codes as [{state}].

GET /counties/{state_fips}?year=
    Returns all counties for a state as [{county, NAME}].
    Year defaults to the loader's configured YEAR (currently 2023).

GET /years/state/{state_fips}?start=&end=
    Returns list of ACS 5-year vintages available for a state.
    Probes the Census API year by year — slow on first call, KV-cached after.

GET /years/county/{state_fips}/{county_fips}?start=&end=
    Same for a specific county.

GET /data/state/{state_fips}?year=
    Returns full ACS profile for a state in a given year.
    Response shape:
      {
        "meta": {
          "geo_level": "state",
          "year": 2023,
          "state": "06",
          "county": null,
          "NAME": "California",
          "from_cache": true
        },
        "data": [
          {
            "code":    "DP05_0001E",
            "label":   "Estimate!!SEX AND AGE!!Total population",
            "concept": "ACS DEMOGRAPHIC AND HOUSING ESTIMATES",
            "group":   "DP05",
            "value":   "39040616"
          },
          ...
        ]
      }
    from_cache indicates whether the raw data came from KV (true) or
    was fetched live from the Census API (false).

GET /data/county/{state_fips}/{county_fips}?year=
    Same structure for a county. county field in meta will be populated.


WHAT "FULLY WORKING" LOOKS LIKE
--------------------------------
Phase 0 is considered fully working when ALL of the following are true:

1. KV CACHE CORRECTNESS
   - kv_get on a non-existent key returns None
   - kv_set followed by kv_get returns the stored value
   - kv_set on an existing key overwrites it (no duplicate rows)

2. METADATA INTEGRITY
   - get_group_metadata returns label and concept for every variable
   - get_all_metadata returns all four groups combined with group field populated
   - Spot-check: DP05_0001E has label "Estimate!!SEX AND AGE!!Total population"
   - Total variable count across all groups is approximately 4224

3. VARIABLE SUFFIX COVERAGE
   - All 4224 variables are classified into known suffix types
     (E, PE, M, PM, EA, MA, PEA, PMA)
   - No "other" / unrecognised suffixes
   - E + PE count falls in a reasonable range (currently ~2112)
   - Classified counts sum exactly to total variable count

4. DATA FETCH CORRECTNESS
   - fetch_or_cache returns a row with 4000+ columns for a known state
   - Second call to fetch_or_cache returns from_cache=True and identical data
   - Same for county

5. DUCKDB UPSERT CORRECTNESS
   - After first write_row_and_get_query, exactly 1 row exists for that geo/year
   - After second write_row_and_get_query (same row), still exactly 1 row
   - Holds for both state and county tables

6. RESPONSE SHAPE CORRECTNESS
   - meta contains: geo_level, year, state, county, NAME, from_cache
   - data is a non-empty list of {code, label, concept, group, value} dicts
   - No geo identifier columns (NAME, state, county, etc.) appear in data
   - All values are either str or None — never float, int, or other types
   - None values are preserved as None (not coerced to 0 or empty string)

The test harness (test_phase0.py) verifies all of the above, plus the API
endpoint layer via FastAPI's TestClient (no running server required), and a
vintage comparison between 2019 and 2023 that confirms variable codes and
labels actually differ across the 2020 redesign boundary. A clean run should
show 32/32 (or equivalent) passing.


KNOWN FACTS ABOUT THE DATA
---------------------------
- California state 2023: ~4226 columns returned from Census API
- Orange County CA 2023:  ~4227 columns returned (one extra geo identifier)
- ~1910 of 4224 metadata variables have None values for California state
  This is expected — many variables are suppressed or not applicable at
  the state level. County-level data has fewer Nones (~1874 for Orange County).
- Between 2019 and 2023, the DP profile tables gained 48 new variable codes:
    - 40 new DP05 codes: total housing units (DP05_0091), and citizen voting
      age population by sex (DP05_0092-0094) — each published in all 8 suffix
      variants (E, PE, M, PM, EA, MA, PEA, PMA). These were added in the
      2022→2023 vintage boundary, not at the 2019→2020 boundary.
    - 8 new DP02 codes: broadband internet subscription (DP02_0154) — a
      newly prominent social concept added to the profile tables
- IMPORTANT — DP05 tail renumbering at the 2022→2023 boundary: when the new
  rows above were inserted into DP05, the variable codes at the end of the
  table were reassigned. DP05_0090E changed meaning between vintages:
    - 2019–2022: DP05_0090E = "CITIZEN, VOTING AGE POPULATION: Citizen, 18
      and over population: Female"
    - 2023:       DP05_0090E = "HISPANIC OR LATINO AND RACE: ... Two races
      excluding Some Other Race, and three or more races"
  This is a label change AND a concept reassignment on the same variable code.
  It is not safe to use DP05_0090E as a boundary marker or stable identifier
  across vintages — use the label from vintage-matched metadata instead.
  DP05_0090E is absent from the Census API metadata for 2019 and 2021 despite
  714 DP05 variables existing in both years (the slot existed but returned
  no label entry). It is present and labelled in 2022 and 2023.
- 1424 variable codes exist in both vintages but with changed labels.
  Example: "Married-couple family" became "Married-couple household" across
  all suffix variants of DP02_0002. This confirms that vintage-matched
  metadata fetching is essential — a single cached label set would silently
  serve wrong labels for the other vintage.
- Delta analysis (comparing values across years) has been explicitly removed
  from this project due to variable instability across vintages.
- DP05 race categories are present in the existing 4176 shared variables.
  The race section covers: White, Black or African American, American Indian
  and Alaska Native, Asian, Native Hawaiian and Other Pacific Islander, Some
  Other Race, Two or More Races, and Hispanic or Latino origin, with
  alone/in-combination breakdowns and cross-tabulation by Hispanic origin.
  MENA (Middle Eastern and North African) is classified as White in the
  current scheme. More granular race subgroups (e.g. Chinese alone, Filipino
  alone) are available in the ACS Detailed Tables (B02 series) at a different
  API endpoint — see ADDING NEW DATA SOURCES below.


ADDING NEW DATA SOURCES
-----------------------
The Phase 0 infrastructure is deliberately source-agnostic at the storage
layer. DuckDB tables are keyed by (geo_level, year, state, county) and accept
arbitrarily wide rows — new columns are added automatically via ALTER TABLE
when first encountered. The KV cache stores JSON blobs and does not care about
their origin. The API response shape {code, label, concept, group, value} works
for any variable from any Census table.

The one component that is currently source-specific is acs_loader.py, which
hardcodes the ACS 5-year profile endpoint:
  base = f"https://api.census.gov/data/{year}/acs/acs5/profile"

ACS Detailed Tables (B/C series, e.g. B02001 for detailed race) use a
different base URL:
  base = f"https://api.census.gov/data/{year}/acs/acs5"

And a different metadata URL:
  /data/{year}/acs/acs5/groups/{group}.json  (no "profile" segment)

To add detailed tables, acs_loader.py needs a small routing addition — a
parameter or config flag that selects the correct endpoint. Everything
downstream (duck_writer, duck_router, api.py, and all analysis phases)
requires no changes. The contract those layers depend on is the row shape,
not the data source.

Candidate tables to add in a future phase:
  B02001 / C02003 — Race (detailed subgroups: Chinese, Filipino, etc.)
  B03001           — Hispanic or Latino origin by specific country
  B04006           — Ancestry
  B05001           — Nativity and citizenship status


WHAT PHASE 0 DOES NOT DO
-------------------------
- It does not filter to E/PE columns only — that is the responsibility of
  downstream phases when building the feature matrix
- It does not validate individual variable values for plausibility
- It does not handle geographic levels other than state and county
- It does not currently support ACS Detailed Tables (B/C series) — only
  the DP profile tables; see ADDING NEW DATA SOURCES above
- It does not log requests (logging is deferred to a later phase)
- It does not authenticate API consumers (the FastAPI server is internal)