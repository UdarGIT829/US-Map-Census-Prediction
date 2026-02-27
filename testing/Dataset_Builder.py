from Dataset_Provider import dataset_provider, filter_dataset
from dp_labels import load_dp_mapping, make_readable_view, split_dp_var, human_label
import pandas as pd

dp_map = load_dp_mapping("dp_mapping.json")

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
) -> pd.DataFrame:
    """
    Build a harder stub 'user outcome' using a *combination* of DP columns:

        1) Optionally include dp_col as one component.
        2) Randomly select additional DP columns from X.
        3) Standardize each component (mean 0, std 1).
        4) Draw random weights and form a linear combination.
        5) Add Gaussian noise proportional to the signal's std.

    This produces a synthetic target that:
      - depends on multiple features
      - is not trivially reconstructable from a single column
      - gives the recipe sampler something non-trivial to discover.

    Works for both county and state level:
      - For county level, key_col = 'county'
      - For state level,  key_col = 'state'
    """
    import numpy as np
    import pandas as pd

    loc = base_dataset["meta"]["location"]
    X = base_dataset["X"]

    if level == "county":
        key_col = "county"
    elif level == "state":
        key_col = "state"
    else:
        raise ValueError(f"Unsupported level={level!r}, expected 'county' or 'state'.")

    if key_col not in loc.columns:
        raise KeyError(f"{key_col!r} not found in meta['location'] columns: {list(loc.columns)}")

    if dp_col not in X.columns:
        raise KeyError(f"{dp_col!r} not found in X columns")

    key_series = loc[key_col].astype(str)

    rng = np.random.default_rng(random_state)

    # --- 1) Choose component columns ---------------------------------------
    candidate_cols = [c for c in X.columns if c != dp_col]

    # If there aren't enough candidates, just use whatever we have
    n_extras = min(n_extra_cols, len(candidate_cols))
    extra_cols = list(rng.choice(candidate_cols, size=n_extras, replace=False)) if n_extras > 0 else []

    component_cols: list[str] = []
    if include_dp_col:
        component_cols.append(dp_col)
    component_cols.extend(extra_cols)

    # Ensure uniqueness & preserve order
    seen = set()
    component_cols = [c for c in component_cols if not (c in seen or seen.add(c))]

    if not component_cols:
        raise ValueError("No component columns available to build stub outcome.")
    else:
        print(f"Stubbing with columns: ",end="")
        for _col in component_cols:
            print(f"{human_label(_col, mapping=dp_map)}")
        print("___")


    # --- 2) Build standardized component matrix ----------------------------
    # Coerce to numeric, drop constant columns
    comps = []
    for col in component_cols:
        s = pd.to_numeric(X[col], errors="coerce")
        # Standardize: mean 0, std 1 (if std > 0)
        s_mean = s.mean()
        s_std = s.std(ddof=0)
        if s_std == 0 or pd.isna(s_std):
            # Constant / degenerate, skip this component
            continue
        z = (s - s_mean) / s_std
        comps.append(z)

    if not comps:
        raise ValueError("All candidate components were constant/degenerate.")

    # Align components on the same index and stack into a matrix
    comp_df = pd.concat(comps, axis=1)
    comp_df.columns = [f"comp_{i}" for i in range(comp_df.shape[1])]

    # --- 3) Random weights & base signal -----------------------------------
    weights = rng.normal(loc=0.0, scale=1.0, size=comp_df.shape[1])
    base_signal = comp_df.to_numpy().dot(weights)

    # --- 4) Add noise proportional to the signal's std ---------------------
    signal_std = np.std(base_signal)
    if signal_std > 0 and noise_frac > 0:
        noise = rng.normal(loc=0.0, scale=noise_frac * signal_std, size=base_signal.shape[0])
    else:
        noise = 0.0

    stub_outcome = base_signal + noise

    # Optional: you could rescale to something more "count-like", but
    # it is fine to leave as a real-valued outcome for regression.

    user_df = pd.DataFrame(
        {
            key_col: key_series,
            target_col: stub_outcome,
        },
        index=X.index,
    )

    return user_df



def PhaseB_1(
    *,
    level: str = "county",
    selected_state_fips: int = 6,
    exclude_counties_fips=None,
    exclude_states_fips=None,
    user_target_df=None,
    dp_col: str = "DP05_0094E",
):
    """
    Build Dataset Phase – (HAS USER DATA) path.

    For now, we stub user data as:
        user_outcome = 0.5 * X[dp_col]

    This works for both county and state level.
    """

    dp_map = load_dp_mapping("dp_mapping.json")





    if not user_target_df:
        # Step 1: base ACS dataset with NO target
        base_data = dataset_provider(
            level=level,
            selected_statecounty=selected_state_fips,
            # no target_df / target_col here
        )

        print(f"Base location meta (PhaseB_1, level={level}):")
        print(base_data["meta"]["location"].head())

        # Step 2: stub user outcome from DP column
        user_target_df = make_stub_user_target_half_dp(
            base_dataset=base_data,
            dp_col=dp_col,
            level=level,
            target_col="user_outcome",
            random_state= 43
        )





    # Step 3: re-run dataset_provider with user target
    if level == "county":
        target_key = "county"
    else:
        target_key = "state"

    dataset_with_user = dataset_provider(
        level=level,
        selected_statecounty=selected_state_fips,
        target_df=user_target_df,
        target_key=target_key,
        target_col="user_outcome",
    )

    loc = dataset_with_user["meta"]["location"]
    print("Unique states:", loc["state"].unique())
    if "county" in loc.columns:
        print("Unique counties:", loc["county"].unique())

    # Step 4: apply filters depending on level
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
        filtered = dataset_with_user  # no extra filtering

    print(f"Filtered X shape (PhaseB_1, level={level}):", filtered["X"].shape)
    print("Filtered locations (PhaseB_1):")
    print(filtered["meta"]["location"].head())

    # print("Feature columns (PhaseB_1):")
    # print(sorted(list(filtered["X"].columns)))

    # print("First few user_outcome values:")
    # print(list(filtered["y"])[:10])

    return filtered


def PhaseB_2(
    *,
    level: str = "county",
    selected_state_fips: int = 6,
    dp_col: str = "DP05_0094E",
    exclude_counties_fips=None,
    exclude_states_fips=None,
):
    """
    Build Dataset Phase – (NO USER DATA) path.

    Uses an internal Census column as the target (dp_col).
    Works for both county and state level.
    """

    dp_map = load_dp_mapping("dp_mapping.json")

    dataset = dataset_provider(
        level=level,
        selected_statecounty=selected_state_fips,
        target_col=dp_col,
    )

    loc = dataset["meta"]["location"]
    print(f"Base location meta (PhaseB_2, level={level}):")
    print(loc.head())

    # Apply filters depending on level
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

    print(f"Filtered X shape (PhaseB_2, level={level}):", filtered["X"].shape)
    print("Filtered locations (PhaseB_2):")
    print(filtered["meta"]["location"].head())

    print("Feature columns (PhaseB_2):")
    print(sorted(list(filtered["X"].columns)))

    print(f"First few target values ({dp_col}):")
    print(list(filtered["y"])[:10])

    return filtered


if __name__ == "__main__":
    CENSUS_TARGET_COL = "DP05_0094E"

    print("Phase B_2 State Level")

    # NO user data, state level
    state_ds = PhaseB_2(
        level="state",
        dp_col=CENSUS_TARGET_COL,
        exclude_states_fips=[2, 15],  # AK, HI for example
    )
    print("___"*6)
    print()
    print("Phase B_1 State Level")

    # Stubbed user data, state level
    state_user_ds = PhaseB_1(
        level="state",
        dp_col=CENSUS_TARGET_COL,
        exclude_states_fips=[2, 15],
        exclude_counties_fips=None,
        exclude_states_fips=None,
        user_target_df=None
    )

    print("___"*6)


    # # NO user data, county level
    # county_ds = PhaseB_2(
    #     level="county",
    #     selected_state_fips=6,
    #     dp_col=CENSUS_TARGET_COL,
    #     exclude_counties_fips=[1, 37, 59],
    # )

    # # Stubbed user data, county level
    # county_user_ds = PhaseB_1(
    #     level="county",
    #     selected_state_fips=6,
    #     dp_col=CENSUS_TARGET_COL,
    #     exclude_counties_fips=[1, 37, 59],
    # )
