from __future__ import annotations

from dp_labels import split_dp_var, is_dp_estimate

import duckdb
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any


def dataset_provider(
    *,
    level: str = "county",             # "county" or "state"
    selected_statecounty: Optional[int] = 3,  # used only if level == "county"
    db_root: str = "data_server/db",
    feature_table: Optional[str] = None,  # override table name if needed
    target_df: Optional[pd.DataFrame] = None,  # user outcome data
    target_key: str = "GEO_ID",        # join key between ACS + target_df
    target_col: str = "outcome",       # name of the target column
    exclude_cols: Optional[list[str]] = None,  # non-feature cols to drop
) -> Dict[str, Any]:
    """
    High-level flow:

    1) Resolve which DB file to open (county vs state).
    2) Connect to DuckDB.
    3) Introspect available tables / schema (optional).
    4) Pull a wide ACS feature DataFrame from the profile table.
    5) Optionally merge user-supplied target/outcome DataFrame.
    6) Clean up columns (drop ID/name/meta cols, keep numeric features).
    7) Return everything needed right *before* model training.

    Nothing here actually fits or evaluates a model.
    """

    # ---- 1) Resolve DB path & default table name ----
    if level == "county":
        if selected_statecounty is None:
            raise ValueError("selected_statecounty is required when level='county'.")

        county_fips = f"{int(selected_statecounty):02d}"  # "03" etc.
        if county_fips == "00":
            raise ValueError("county_fips '00' is reserved for state-level; use level='state'.")

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

    # ---- 3) (Optional) Introspection / debugging ----
    # You can comment these out later once it's stable.
    tables_df = con.execute("SHOW TABLES").df()
    # print("Available tables:\n", tables_df)

    describe_df = con.execute(f"DESCRIBE SELECT * FROM {feature_table};").df()
    # print(f"\nSchema for {feature_table}:\n", describe_df)

    # ---- 4) Load feature DataFrame from profile table ----
    # Later you can add WHERE filters for year, etc.
    features_raw: pd.DataFrame = con.execute(
        f"SELECT * FROM {feature_table};"
    ).df()

    # ---- 5) Optionally merge in target/outcome data ----
    # Assumption: both have a common key like GEO_ID or county FIPS.
    if target_df is not None:
        if target_key not in features_raw.columns:
            raise KeyError(f"{target_key!r} not found in feature table columns.")

        if target_key not in target_df.columns:
            raise KeyError(f"{target_key!r} not found in target_df columns.")

        if target_col not in target_df.columns:
            raise KeyError(f"{target_col!r} not found in target_df columns.")

        merged = features_raw.merge(
            target_df[[target_key, target_col]],
            on=target_key,
            how="inner",  # you can make this configurable later
        )
    else:
        merged = features_raw.copy()

    # ---- 6) Basic feature cleaning (no scaling, no model yet) ----
    # Separate y if present
    if target_col in merged.columns:
        y = merged[target_col]
        X = merged.drop(columns=[target_col])
    else:
        y = None
        X = merged.copy()

    # --- Keep only state/county as separate location metadata ---
    geo_cols = [c for c in merged.columns if c.lower() in ("state", "county")]
    location_meta = merged[geo_cols].copy()
    print("[DEBUG] location_meta columns:", list(location_meta.columns))

    # Decide what to drop as non-features (ID/name/etc)
    if exclude_cols is None:
        exclude_cols = ["GEO_ID", "GEOID", "NAME", "year"]

    # We already copied out state/county to location_meta, so drop them from X
    exclude_cols = list(set(exclude_cols))  # dedupe
    exclude_cols += [c for c in X.columns if c.lower() in ("state", "county")]

    # 1) Drop ID / metadata columns
    candidate_cols = [c for c in X.columns if c not in exclude_cols]

    # 2) Restrict to DP estimate variables only
    feature_cols = [c for c in candidate_cols if is_dp_estimate(c)]

    X = X[feature_cols].copy()

    # 3) Coerce DP estimate vars to numeric
    for col in X.columns:
        parts = split_dp_var(col)
        if parts is not None:
            _, _, suffix = parts
            if suffix == "E":  # only estimates
                X[col] = pd.to_numeric(X[col], errors="coerce")

    # Optional: global numeric coercion for anything else numeric-ish
    X = X.apply(pd.to_numeric, errors="ignore")

    # 4) Drop all-NaN columns
    X = X.dropna(axis=1, how="all")

    # (Optional) If you want to enforce numeric-only:
    # X = X.select_dtypes(include="number")


    # --- DEBUG: what do we have before select_dtypes? ---
    # print("\n[DEBUG] X.dtypes before select_dtypes:")
    # print(X.dtypes)
    # print("\n[DEBUG] dtype counts:")
    # print(X.dtypes.value_counts())
    # print("\n[DEBUG] X.shape before select_dtypes:", X.shape)

    # Keep only numeric columns as features
    # X = X.select_dtypes(include="number")

    # print("\n[DEBUG] X.shape after select_dtypes:", X.shape)

    # ---- 7) Package outputs right before training step ----
    # You can pass this dict directly into a run_pipeline() or train_model().
    dataset = {
        "level": level,
        "db_path": str(db_path),
        "feature_table": feature_table,
        "raw_features": features_raw,  # untouched ACS profile pull
        "merged": merged,              # ACS + target (if provided)
        "X": X,
        "y": y,
        "meta": {
            "location": location_meta,
            "n_rows_raw": len(features_raw),
            "n_rows_merged": len(merged),
            "n_features": X.shape[1],
            "columns_used": list(X.columns),
            "tables_df": tables_df,    # introspection
            "describe_df": describe_df,
        },
    }

    # NOTE: This is the exact point just before you'd do:
    #   X_train, X_test, y_train, y_test = train_test_split(...)
    #   model = SomeModel(...)
    #   model.fit(X_train, y_train)
    #
    # That "model" part should live in a separate training function.

    return dataset

def _normalize_to_list(value):
    if value is None:
        return None
    if isinstance(value, (list, tuple, set, pd.Series)):
        return list(value)
    return [value]


def _fips_width(series: pd.Series) -> int | None:
    """Infer typical FIPS width (e.g. 2 for state, 3 for county)."""
    s = series.dropna().astype(str)
    if s.empty:
        return None
    lengths = s.str.len()
    return int(lengths.mode().iat(0))  # most common length


def _normalize_fips(values, width: int | None):
    """Convert user-supplied values to zero-padded FIPS strings."""
    vals = _normalize_to_list(values)
    if vals is None or width is None:
        return None
    return [str(v).zfill(width) for v in vals]


def filter_dataset(
    dataset: dict,
    *,
    state_fips=None,    # e.g. 6 or "06" or [6, 12]
    county_fips=None,   # e.g. 37 or "037" or [1, 37, 59]
    invert: bool = False
) -> dict:
    """
    Return a new dataset filtered by FIPS codes.

    - Uses dataset["meta"]["location"]["STATE"] and ["county"].
    - Values are treated as FIPS codes (zero-padded strings).
    """

    loc = dataset["meta"]["location"].copy()

    # Identify state / county columns
    state_col = None
    county_col = None
    for c in loc.columns:
        lc = c.lower()
        if lc == "state":
            state_col = c
        elif lc == "county":
            county_col = c

    # Start with "keep everything"
    mask = pd.Series(True, index=loc.index)

    # --- State filtering ---
    if state_fips is not None and state_col is not None:
        width = 2
        targets = _normalize_fips(state_fips, width)
        mask &= loc[state_col].astype(str).isin(targets)

    # --- County filtering ---
    if county_fips is not None and county_col is not None:
        width = 3
        targets = _normalize_fips(county_fips, width)
        mask &= loc[county_col].astype(str).isin(targets)

    if invert:
        mask = ~mask

    keep_idx = loc.index[mask]
    print(f"[DEBUG] keeping {len(keep_idx)} of {len(loc)} rows")

    def _subset(df):
        if df is None:
            return None
        return df.loc[keep_idx]

    # Build filtered dataset (shallow copy, with filtered pieces)
    new_dataset = dict(dataset)
    new_meta = dict(dataset["meta"])

    new_dataset["X"] = _subset(dataset["X"])
    new_dataset["y"] = _subset(dataset["y"]) if dataset["y"] is not None else None
    new_dataset["raw_features"] = _subset(dataset["raw_features"])
    new_dataset["merged"] = _subset(dataset["merged"])
    new_meta["location"] = _subset(dataset["meta"]["location"])

    new_meta["n_rows_raw"] = len(new_dataset["raw_features"])
    new_meta["n_rows_merged"] = len(new_dataset["merged"])
    new_dataset["meta"] = new_meta

    return new_dataset
