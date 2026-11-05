# Dataset_Provider.py
#
# Rewired for Phase 1:
#   - Accepts `metadata` (Phase 0 get_all_metadata() output) and `year` as
#     explicit parameters.
#   - Stores both in the returned dataset dict so downstream phases
#     (phase1_screening, generate_feature_families) can use them without
#     re-fetching.
#   - Adds year-aware WHERE clause to DuckDB queries so multi-year databases
#     are handled correctly.  If year=None, no filter is applied (backward
#     compat for single-year DBs already on disk).
#   - Does NOT yet call acs_loader directly (deferred — the DuckDB read is
#     still valid and the acs_loader rewire is a Phase 1 follow-up task).
#   - All other logic (E-column filtering, target merge, location_meta
#     extraction, filter_dataset) is unchanged.

from __future__ import annotations

from phase_1.dp_labels import split_dp_var, is_dp_estimate

import duckdb
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any


def dataset_provider(
    *,
    level: str = "county",
    selected_statecounty: Optional[int] = 3,
    db_root: str = "data_server/db",
    feature_table: Optional[str] = None,
    year: Optional[int] = None,           # NEW — filters DuckDB query by year
    metadata: Optional[Dict[str, Any]] = None,  # NEW — Phase 0 metadata dict
    target_df: Optional[pd.DataFrame] = None,
    target_key: str = "GEO_ID",
    target_col: str = "outcome",
    exclude_cols: Optional[list[str]] = None,
) -> Dict[str, Any]:
    """
    High-level flow:

    1) Resolve which DB file to open (county vs state).
    2) Connect to DuckDB.
    3) Introspect available tables / schema.
    4) Pull a wide ACS feature DataFrame from the profile table,
       optionally filtered to a specific year.
    5) Optionally merge user-supplied target/outcome DataFrame.
    6) Clean up columns (drop ID/name/meta cols, keep E-suffix features).
    7) Return everything needed right before model training, including
       the metadata dict and year so downstream phases don't need to
       re-fetch them.

    Parameters
    ----------
    level : "county" or "state"
    selected_statecounty : state FIPS int, used only when level="county"
    db_root : path to directory containing DuckDB files
    feature_table : override the default table name (rarely needed)
    year : ACS vintage year (e.g. 2023).  If provided, adds a WHERE clause
        to the DuckDB query.  If None, all rows are returned (safe for
        single-year DBs; may return mixed vintages on multi-year DBs).
    metadata : dict from Phase 0 get_all_metadata(year, groups).
        Shape: {varcode: {"label": ..., "concept": ..., "group": ...}}
        Stored in dataset["meta"]["acs_metadata"] for downstream use.
        If None, metadata-dependent features (human labels, family tree
        generation) will not be available until metadata is provided later.
    target_df : DataFrame with at least [target_key, target_col] columns.
    target_key : join key in both features_raw and target_df.
    target_col : name of the outcome column in target_df.
    exclude_cols : additional columns to exclude from X (on top of defaults).

    Returns
    -------
    dict with keys: level, db_path, feature_table, year, raw_features,
                    merged, X, y, meta
    meta contains: location, acs_metadata, year, n_rows_raw, n_rows_merged,
                   n_features, columns_used, tables_df, describe_df
    """

    # ---- 1) Resolve DB path & default table name ----
    if level == "county":
        if selected_statecounty is None:
            raise ValueError("selected_statecounty is required when level='county'.")

        county_fips = f"{int(selected_statecounty):02d}"
        if county_fips == "00":
            raise ValueError(
                "county_fips '00' is reserved for state-level; use level='state'."
            )

        db_path = Path(db_root) / f"acs_counties_{county_fips}.duckdb"
        default_table = "acs5_county_profile"

    elif level == "state":
        db_path = Path(db_root) / "acs_states.duckdb"
        default_table = "acs5_state_profile"

    else:
        raise ValueError(f"Unsupported level={level!r}. Use 'county' or 'state'.")

    if feature_table is None:
        feature_table = default_table

    # ---- 2) Connect to DuckDB ----
    con = duckdb.connect(str(db_path), read_only=True)

    # ---- 3) Introspection ----
    tables_df = con.execute("SHOW TABLES").df()
    describe_df = con.execute(f"DESCRIBE SELECT * FROM {feature_table};").df()

    # ---- 4) Load feature DataFrame ----
    # Apply year filter if requested and if a 'year' column exists in the table.
    table_columns = set(describe_df["column_name"].tolist())
    if year is not None and "year" in table_columns:
        # Diagnostic: show what year values actually exist before filtering.
        # This makes it immediately visible when Phase 0 stored a different
        # year value than expected (e.g. None, 0, or a different vintage).
        actual_years = con.execute(
            f"SELECT DISTINCT year FROM {feature_table} ORDER BY year;"
        ).df()["year"].tolist()
        if year not in actual_years:
            print(
                f"[WARN] year={year} not found in {feature_table}. "
                f"Actual year values in table: {actual_years}. "
                f"Returning all rows — pass year=None to suppress this warning."
            )
            query = f"SELECT * FROM {feature_table};"
        else:
            query = f"SELECT * FROM {feature_table} WHERE year = {int(year)};"
    else:
        if year is not None and "year" not in table_columns:
            print(
                f"[WARN] year={year} requested but 'year' column not found in "
                f"{feature_table}. Returning all rows."
            )
        query = f"SELECT * FROM {feature_table};"

    features_raw: pd.DataFrame = con.execute(query).df()

    # ---- 5) Optionally merge in target/outcome data ----
    if target_df is not None:
        for col, source in [(target_key, "feature table"), (target_key, "target_df"),
                            (target_col, "target_df")]:
            df = features_raw if source == "feature table" else target_df
            if col not in df.columns:
                raise KeyError(f"{col!r} not found in {source} columns.")

        merged = features_raw.merge(
            target_df[[target_key, target_col]],
            on=target_key,
            how="inner",
        )
    else:
        merged = features_raw.copy()

    # ---- 6) Feature cleaning ----
    if target_col in merged.columns:
        y = merged[target_col]
        X = merged.drop(columns=[target_col])
    else:
        y = None
        X = merged.copy()

    # Keep state/county FIPS as separate location metadata
    geo_cols = [c for c in merged.columns if c.lower() in ("state", "county")]
    location_meta = merged[geo_cols].copy()

    # Build exclusion list
    if exclude_cols is None:
        exclude_cols = ["GEO_ID", "GEOID", "NAME", "year"]

    exclude_cols = list(set(exclude_cols))
    exclude_cols += [c for c in X.columns if c.lower() in ("state", "county")]

    # Drop ID / metadata columns
    candidate_cols = [c for c in X.columns if c not in exclude_cols]

    # Restrict to E-suffix DP estimate variables
    feature_cols = [c for c in candidate_cols if is_dp_estimate(c)]
    X = X[feature_cols].copy()

    # Coerce E-suffix columns to numeric (Census API returns strings)
    for col in X.columns:
        parts = split_dp_var(col)
        if parts is not None:
            _, _, suffix = parts
            if suffix == "E":
                X[col] = pd.to_numeric(X[col], errors="coerce")

    # Belt-and-suspenders: coerce anything still object-typed
    for col in X.select_dtypes(include="object").columns:
        try:
            X[col] = pd.to_numeric(X[col])
        except (ValueError, TypeError):
            pass

    # Drop all-NaN columns (suppressed / not applicable at this geo level)
    X = X.dropna(axis=1, how="all")

    # ---- 7) Package outputs ----
    dataset = {
        "level": level,
        "db_path": str(db_path),
        "feature_table": feature_table,
        "year": year,                       # NEW — propagated through pipeline
        "raw_features": features_raw,
        "merged": merged,
        "X": X,
        "y": y,
        "meta": {
            "location": location_meta,
            "acs_metadata": metadata,       # NEW — Phase 0 metadata dict (may be None)
            "year": year,                   # NEW — duplicated here for convenience
            "n_rows_raw": len(features_raw),
            "n_rows_merged": len(merged),
            "n_features": X.shape[1],
            "columns_used": list(X.columns),
            "tables_df": tables_df,
            "describe_df": describe_df,
        },
    }

    return dataset


# ---------------------------------------------------------------------------
# Private helpers for filter_dataset
# ---------------------------------------------------------------------------

def _normalize_to_list(value):
    if value is None:
        return None
    if isinstance(value, (list, tuple, set, pd.Series)):
        return list(value)
    return [value]


def _normalize_fips(values, width: int) -> list[str]:
    """Convert user-supplied FIPS values to zero-padded strings of `width` digits."""
    vals = _normalize_to_list(values)
    if vals is None:
        return []
    return [str(v).zfill(width) for v in vals]


# ---------------------------------------------------------------------------
# filter_dataset — unchanged logic, kept here for import compatibility
# ---------------------------------------------------------------------------

def filter_dataset(
    dataset: dict,
    *,
    state_fips=None,
    county_fips=None,
    invert: bool = False,
) -> dict:
    """
    Return a new dataset dict filtered to the requested FIPS codes.

    state_fips  — int, str, or list thereof  (e.g. 6, "06", [6, 12])
    county_fips — int, str, or list thereof  (e.g. 37, "037", [1, 37, 59])
    invert      — if True, EXCLUDE the matched rows instead of keeping them

    Uses dataset["meta"]["location"]["state"] and ["county"] columns.
    """
    loc = dataset["meta"]["location"].copy()

    state_col = None
    county_col = None
    for c in loc.columns:
        lc = c.lower()
        if lc == "state":
            state_col = c
        elif lc == "county":
            county_col = c

    mask = pd.Series(True, index=loc.index)

    if state_fips is not None and state_col is not None:
        targets = _normalize_fips(state_fips, width=2)
        mask &= loc[state_col].astype(str).isin(targets)

    if county_fips is not None and county_col is not None:
        targets = _normalize_fips(county_fips, width=3)
        mask &= loc[county_col].astype(str).isin(targets)

    if invert:
        mask = ~mask

    keep_idx = loc.index[mask]
    print(f"[filter_dataset] keeping {len(keep_idx)} of {len(loc)} rows")

    def _subset(df):
        if df is None:
            return None
        return df.loc[keep_idx]

    new_dataset = dict(dataset)
    new_meta = dict(dataset["meta"])

    new_dataset["X"] = _subset(dataset["X"])
    new_dataset["y"] = (
        _subset(dataset["y"]) if dataset["y"] is not None else None
    )
    new_dataset["raw_features"] = _subset(dataset["raw_features"])
    new_dataset["merged"] = _subset(dataset["merged"])
    new_meta["location"] = _subset(dataset["meta"]["location"])

    new_meta["n_rows_raw"] = len(new_dataset["raw_features"])
    new_meta["n_rows_merged"] = len(new_dataset["merged"])
    new_dataset["meta"] = new_meta

    return new_dataset