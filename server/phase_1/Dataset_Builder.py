# Dataset_Builder.py
#
# Rewired for Phase 1:
#   - metadata is required — metadata=None raises ValueError immediately.
#     ACS labels change between vintages; no static fallback is acceptable.
#   - PhaseB_1 and PhaseB_2 accept metadata from Phase 0 get_all_metadata().
#   - make_stub_user_target_half_dp() is TEST-ONLY.
#   - load_dp_mapping / dp_mapping.json are not used anywhere in this file.

from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd

from phase_1.Dataset_Provider import dataset_provider, filter_dataset
from phase_1.dp_labels import (
    metadata_to_dp_mapping,
    human_label,
    split_dp_var,
)

VALID_STATE_FIPS = {
    "01","02","04","05","06","08","09","10","11","12","13","15","16","17","18","19","20",
    "21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37",
    "38","39","40","41","42","44","45","46","47","48","49","50","51","53","54","55","56",
    "72",  # Puerto Rico
}


# ---------------------------------------------------------------------------
# Helper: resolve dp_map from whatever the caller provides
# ---------------------------------------------------------------------------

def _resolve_dp_map(metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Return a dp_map dict for label lookups.

    metadata must be the output of Phase 0's get_all_metadata(year, groups).
    It is vintage-matched to the data being analysed — this is not optional.
    ACS DP variable labels change between vintages (1424 confirmed changes
    between 2019 and 2023), so a static fallback file is never acceptable.

    Raises ValueError if metadata is None or empty.
    """
    if metadata is None:
        raise ValueError(
            "metadata=None passed to Dataset_Builder. "
            "metadata must be the output of Phase 0's get_all_metadata(year, groups). "
            "ACS variable labels change between vintages — a static fallback file "
            "would silently produce wrong labels for any year other than 2023."
        )
    return metadata_to_dp_mapping(metadata)


# ---------------------------------------------------------------------------
# Stub target builder — TEST / DEVELOPMENT USE ONLY
# ---------------------------------------------------------------------------

def make_stub_user_target_half_dp(
    base_dataset: dict,
    *,
    dp_col: str = "DP05_0094E",
    level: str = "county",
    target_col: str = "user_outcome",
    n_extra_cols: int = 5,
    include_dp_col: bool = True,
    noise_frac: float = 0.1,
    random_state: int = 42,
    metadata: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    Build a synthetic 'user outcome' from a linear combination of DP columns.

    !! TEST / DEVELOPMENT USE ONLY !!
    This function creates a fake target variable for testing the pipeline
    end-to-end when no real outcome data is available.  It must not be called
    in production.  Real user targets come from PhaseB_1(user_target_df=...).

    Construction:
      1. Optionally include dp_col as one component.
      2. Randomly select n_extra_cols additional DP columns from X.
      3. Standardise each component (mean 0, std 1).
      4. Draw random weights and form a linear combination.
      5. Add Gaussian noise proportional to the signal's std.

    Parameters
    ----------
    base_dataset : output of dataset_provider() with no target yet.
    dp_col : anchor column to include (if include_dp_col=True).
    level : "county" or "state" — determines the join key column name.
    target_col : name for the synthetic outcome column.
    n_extra_cols : number of additional random DP columns to mix in.
    include_dp_col : whether to include dp_col as a component.
    noise_frac : noise amplitude as a fraction of signal std.
    random_state : RNG seed for reproducibility.
    metadata : Phase 0 metadata dict for human-readable label printing.
        If None, variable codes are printed instead of labels.

    Returns
    -------
    pd.DataFrame with columns [key_col, target_col], same index as X.
    """
    import numpy as np

    dp_map = _resolve_dp_map(metadata)

    loc = base_dataset["meta"]["location"]
    X = base_dataset["X"]

    if level == "county":
        key_col = "county"
    elif level == "state":
        key_col = "state"
    else:
        raise ValueError(f"Unsupported level={level!r}, expected 'county' or 'state'.")

    if key_col not in loc.columns:
        raise KeyError(
            f"{key_col!r} not found in meta['location'] columns: {list(loc.columns)}"
        )
    if dp_col not in X.columns:
        raise KeyError(f"{dp_col!r} not found in X columns")

    key_series = loc[key_col].astype(str)
    rng = np.random.default_rng(random_state)

    # 1) Choose component columns
    candidate_cols = [c for c in X.columns if c != dp_col]
    n_extras = min(n_extra_cols, len(candidate_cols))
    extra_cols = (
        list(rng.choice(candidate_cols, size=n_extras, replace=False))
        if n_extras > 0
        else []
    )

    component_cols: list[str] = []
    if include_dp_col:
        component_cols.append(dp_col)
    component_cols.extend(extra_cols)

    # Deduplicate while preserving order
    seen: set[str] = set()
    component_cols = [c for c in component_cols if not (c in seen or seen.add(c))]

    if not component_cols:
        raise ValueError("No component columns available to build stub outcome.")

    print("[STUB] Synthetic target built from:")
    for col in component_cols:
        print(f"  {human_label(col, dp_map)}")
    print("---")

    # 2) Build standardised component matrix
    comps = []
    for col in component_cols:
        s = pd.to_numeric(X[col], errors="coerce")
        s_std = s.std(ddof=0)
        if s_std == 0 or pd.isna(s_std):
            continue
        comps.append((s - s.mean()) / s_std)

    if not comps:
        raise ValueError("All candidate components were constant/degenerate.")

    comp_df = pd.concat(comps, axis=1)
    comp_df.columns = [f"comp_{i}" for i in range(comp_df.shape[1])]

    # 3) Random weights → base signal
    weights = rng.normal(loc=0.0, scale=1.0, size=comp_df.shape[1])
    base_signal = comp_df.to_numpy().dot(weights)

    # 4) Add noise
    signal_std = base_signal.std()
    noise = (
        rng.normal(loc=0.0, scale=noise_frac * signal_std, size=base_signal.shape[0])
        if signal_std > 0 and noise_frac > 0
        else 0.0
    )

    stub_outcome = base_signal + noise

    return pd.DataFrame(
        {key_col: key_series, target_col: stub_outcome},
        index=X.index,
    )


# ---------------------------------------------------------------------------
# PhaseB_1 — user HAS a target variable
# ---------------------------------------------------------------------------

def PhaseB_1(
    *,
    level: str = "county",
    selected_state_fips: int = 6,
    year: Optional[int] = None,
    metadata: Optional[Dict[str, Any]] = None,
    exclude_counties_fips=None,
    exclude_states_fips=None,
    user_target_df: Optional[pd.DataFrame] = None,
    dp_col: str = "DP05_0094E",
) -> dict:
    """
    Dataset assembly — path for when the user HAS provided a target variable.

    Parameters
    ----------
    level : "county" or "state"
    selected_state_fips : state FIPS (int) used when level="county"
    year : ACS vintage year; passed to dataset_provider for DuckDB filtering
        and used to ensure metadata is vintage-matched.
    metadata : Phase 0 get_all_metadata(year, groups) output.
        Stored in dataset["meta"]["acs_metadata"] for downstream phases.
    exclude_counties_fips : county FIPS codes to exclude (list of int/str)
    exclude_states_fips : state FIPS codes to exclude (list of int/str)
    user_target_df : DataFrame with [key_col, "user_outcome"] columns.
        If None, a synthetic stub target is generated for testing purposes
        (make_stub_user_target_half_dp).  In production this should always
        be provided.
    dp_col : anchor DP column for the stub target (only used when
        user_target_df is None).

    Returns
    -------
    Filtered dataset dict ready for phase1_screening.
    """
    if metadata is None:
        raise ValueError(
            "metadata=None passed to PhaseB_1. "
            "metadata must be the output of Phase 0's get_all_metadata(year, groups). "
            "ACS variable labels change between vintages — there is no acceptable "
            "static fallback."
        )
    if user_target_df is None:
        # Build base dataset with no target, then generate synthetic target
        base_data = dataset_provider(
            level=level,
            selected_statecounty=selected_state_fips,
            year=year,
            metadata=metadata,
        )

        user_target_df = make_stub_user_target_half_dp(
            base_dataset=base_data,
            dp_col=dp_col,
            level=level,
            target_col="user_outcome",
            random_state=43,
            metadata=metadata,
        )

    target_key = "county" if level == "county" else "state"

    # Infer the target column name from the actual DataFrame — it is whichever
    # column is not the join key. This handles both the stub path ("user_outcome")
    # and any real user-supplied DataFrame with an arbitrary column name
    # (e.g. "cancer_incidence").
    non_key_cols = [c for c in user_target_df.columns if c != target_key]
    if len(non_key_cols) != 1:
        raise ValueError(
            f"user_target_df must have exactly 2 columns: [{target_key!r}, <target>]. "
            f"Got columns: {list(user_target_df.columns)}"
        )
    target_col = non_key_cols[0]

    dataset_with_user = dataset_provider(
        level=level,
        selected_statecounty=selected_state_fips,
        year=year,
        metadata=metadata,
        target_df=user_target_df,
        target_key=target_key,
        target_col=target_col,
    )

    # Apply geo filters
    if level == "county" and exclude_counties_fips:
        filtered = filter_dataset(
            dataset=dataset_with_user,
            county_fips=exclude_counties_fips,
            invert=True,
        )
    elif level == "state" and exclude_states_fips:
        filtered = filter_dataset(
            dataset=dataset_with_user,
            state_fips=exclude_states_fips,
            invert=True,
        )
    else:
        filtered = dataset_with_user

    print(f"[PhaseB_1] level={level}, X shape: {filtered['X'].shape}")
    return filtered


# ---------------------------------------------------------------------------
# PhaseB_2 — NO user target; uses internal ACS column
# ---------------------------------------------------------------------------

def PhaseB_2(
    *,
    level: str = "county",
    selected_state_fips: int = 6,
    year: Optional[int] = None,
    metadata: Optional[Dict[str, Any]] = None,
    dp_col: str = "DP05_0094E",
    exclude_counties_fips=None,
    exclude_states_fips=None,
) -> dict:
    """
    Dataset assembly — path for when there is NO user-supplied target.

    Uses an internal ACS column (dp_col) as the target variable.

    Parameters
    ----------
    level, selected_state_fips, year, metadata, exclude_*_fips :
        same as PhaseB_1.
    dp_col : ACS variable code to use as the regression target.
        Must be an E-suffix column present in the data.

    Returns
    -------
    Filtered dataset dict ready for phase1_screening.
    """
    if metadata is None:
        raise ValueError(
            "metadata=None passed to PhaseB_2. "
            "metadata must be the output of Phase 0's get_all_metadata(year, groups). "
            "ACS variable labels change between vintages — there is no acceptable "
            "static fallback."
        )
    dataset = dataset_provider(
        level=level,
        selected_statecounty=selected_state_fips,
        year=year,
        metadata=metadata,
        target_col=dp_col,
    )

    if level == "county" and exclude_counties_fips:
        filtered = filter_dataset(
            dataset=dataset,
            county_fips=exclude_counties_fips,
            invert=True,
        )
    elif level == "state" and exclude_states_fips:
        filtered = filter_dataset(
            dataset=dataset,
            state_fips=exclude_states_fips,
            invert=True,
        )
    else:
        filtered = dataset

    print(f"[PhaseB_2] level={level}, X shape: {filtered['X'].shape}")
    return filtered


# ---------------------------------------------------------------------------
# Dev smoke-test — run directly to validate wiring
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # NOTE: In a real run you would get `metadata` from Phase 0:
    #
    # metadata is required — metadata=None now raises ValueError.
    # To run this smoke test, provide metadata from Phase 0:
    #
    from data_server.acs_loader import get_all_metadata
    metadata = get_all_metadata(year=2023)

    CENSUS_TARGET_COL = "DP05_0094E"
    YEAR = 2023

    print("=== PhaseB_2 State Level ===")
    state_ds = PhaseB_2(
        level="state",
        year=YEAR,
        metadata=metadata,
        dp_col=CENSUS_TARGET_COL,
        exclude_states_fips=[2, 15],
    )
    print(f"X shape: {state_ds['X'].shape}")
    print()

    print("=== PhaseB_1 State Level (stub target) ===")
    state_user_ds = PhaseB_1(
        level="state",
        year=YEAR,
        metadata=metadata,
        user_target_df=None,    # triggers stub generation (TEST ONLY)
        dp_col=CENSUS_TARGET_COL,
        exclude_states_fips=[],
    )
    print(f"X shape: {state_user_ds['X'].shape}")