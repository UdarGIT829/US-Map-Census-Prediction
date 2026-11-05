# phase1/input/opioid_loader.py
#
# Loads CDC opioid overdose death data and produces a target DataFrame
# compatible with Dataset_Builder's PhaseB_1 interface.
#
# Expected file location: phase1/input/<filename>.csv
#
# CSV shape (CDC Wonder opioid export):
#   State Code | Year | State | Deaths | Population |
#   Crude Rate | Crude Rate Lower 95% CI | Crude Rate Upper 95% CI |
#   Age Adjusted Rate | Age Adjusted Rate Lower 95% CI | Age Adjusted Rate Upper 95% CI
#
# Key facts about this data:
#   - State Code is already zero-padded (e.g. "06" for California)
#   - Year range: 2018–2023 — direct 1:1 pairing with ACS vintages, no fallback
#     needed (unlike cancer_loader where 2023 ACS → 2022 cancer data)
#   - CDC suppresses death counts below 10: Deaths and rate columns are NaN
#     for those states/years. Suppressed rows are dropped with a warning and
#     counted in last_load_info["n_suppressed"].
#   - 51 states (50 + DC). Puerto Rico not present.
#
# Metric options:
#   "age_adjusted_rate" — deaths per 100k, age-adjusted. RECOMMENDED DEFAULT.
#                         Controls for differing age structures between states,
#                         which is the primary driver of spurious correlations
#                         with ACS DP05 demographic variables.
#   "crude_rate"        — deaths per 100k, unadjusted
#   "deaths"            — raw death count. Strongly correlated with state
#                         population size — large-state ACS variables will
#                         score highly in screening for the wrong reasons.
#                         Use only if that is intentional.
#
# Window aggregation:
#   window=1 (default) loads a single year — the standard behaviour.
#   window=5 aggregates across the 5-year window ending at the resolved year.
#   Aggregation by metric type:
#     "age_adjusted_rate" / "crude_rate" → mean (rates don't sum meaningfully)
#     "deaths"                           → sum  (counts aggregate naturally)
#   Suppressed states (CDC count < 10) are dropped per-year before aggregation.
#   States suppressed in ANY window year are excluded from the output.
#   Example: ACS 2023, window=5 → aggregates opioid years 2019–2023.
#   pd.DataFrame with exactly two columns:
#     "state"       — zero-padded 2-digit FIPS string, e.g. "06"
#     <target_col>  — float64, the chosen metric per state
#
#   target_col name is supplied by the caller — nothing is hardcoded here.
#   This DataFrame is passed to PhaseB_1(user_target_df=...) which joins
#   it to ACS data on target_key="state".

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from phase_1.base_loader import BaseTargetLoader


DEFAULT_CSV = Path("phase1/input/opioid_deaths_2018_2023.csv")

# Maps metric name → CSV column name
_METRIC_COLUMNS: Dict[str, str] = {
    "age_adjusted_rate": "Age Adjusted Rate",
    "crude_rate":        "Crude Rate",
    "deaths":            "Deaths",
}


class OpioidLoader(BaseTargetLoader):
    """
    Loads CDC opioid overdose death data for a given year and metric.

    Parameters
    ----------
    csv_path   : Path to the CDC opioid CSV.
    target_col : Name for the output metric column. Supplied by the caller.
                 Example: "opioid_age_adjusted_death_rate_2022".
    metric     : Which column to use as the target value.
                 One of: "age_adjusted_rate" (default), "crude_rate", "deaths".
    window     : Number of years to aggregate. Default 1 (single year, current
                 behaviour). Use 5 to aggregate across the 5-year window ending
                 at the resolved data year, matching the ACS 5-year scope.
                 Rates (age_adjusted_rate, crude_rate) are averaged; deaths are summed.
    min_states : Raise if fewer than this many states survive suppression filtering.
    """

    # Direct 1:1 pairing — 2023 opioid data exists, no fallback needed.
    ACS_PAIRINGS: Dict[int, int] = {
        2018: 2018,
        2019: 2019,
        2020: 2020,
        2021: 2021,
        2022: 2022,
        2023: 2023,
    }

    ACS_CAUTION_VINTAGES: Dict[int, str] = {
        2020: (
            "ACS 2020 5-year is marked experimental by the Census Bureau due to "
            "COVID-19 data collection disruptions. Opioid deaths in 2020 are also "
            "genuinely anomalous (supply chain disruptions, fentanyl surge, reduced "
            "treatment access). Use with caution."
        ),
    }

    def __init__(
        self,
        *,
        csv_path: str | Path = DEFAULT_CSV,
        target_col: str,
        metric: str = "age_adjusted_rate",
        window: int = 1,
        min_states: int = 45,
    ):
        super().__init__(csv_path=csv_path, target_col=target_col, min_states=min_states)

        if metric not in _METRIC_COLUMNS:
            raise ValueError(
                f"OpioidLoader: metric={metric!r} is not supported. "
                f"Choose from: {sorted(_METRIC_COLUMNS.keys())}."
            )
        if window < 1:
            raise ValueError(f"OpioidLoader: window must be >= 1, got {window}.")

        self.metric = metric
        self.window = window

    def load(
        self,
        *,
        acs_year: Optional[int] = None,
        year: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Load opioid death data and return a two-column DataFrame.

        When window=1 (default), loads a single year.
        When window>1, aggregates across the window years ending at the
        resolved data year. Rates are averaged; death counts are summed.
        States suppressed in ANY window year are excluded from the output.

        See BaseTargetLoader.load() for the full parameter and return contract.

        After a successful load, self.last_load_info is set to a dict with:
            loader, metric, window, acs_year, data_year, window_years,
            n_states, n_suppressed, suppressed_states, target_col
        """
        data_year = self._resolve_year(acs_year, year)
        self._check_csv_exists()

        df = pd.read_csv(self.csv_path)
        available = sorted(df["Year"].dropna().unique().astype(int).tolist())

        # --- Resolve window years ---
        window_years = self._resolve_window(data_year, self.window)
        missing_years = [y for y in window_years if y not in available]
        if missing_years:
            raise ValueError(
                f"OpioidLoader: years {missing_years} not found in CSV "
                f"(window={self.window}, end_year={data_year}). "
                f"Available years: {available[0]}–{available[-1]}."
            )

        # --- Load and clean each year in the window ---
        metric_csv_col = _METRIC_COLUMNS[self.metric]
        yearly_frames  = []
        total_suppressed: List[str] = []

        for yr in window_years:
            subset = df[df["Year"] == yr].copy()
            subset[metric_csv_col] = pd.to_numeric(subset[metric_csv_col], errors="coerce")

            suppressed_mask = subset[metric_csv_col].isna()
            n_sup = int(suppressed_mask.sum())
            if n_sup > 0:
                sup_states = subset.loc[suppressed_mask, "State"].tolist()
                total_suppressed.extend(sup_states)
                print(
                    f"[OpioidLoader] WARNING: {n_sup} state(s) suppressed "
                    f"(CDC count < 10) for metric={self.metric!r}, year={yr}. "
                    f"Dropping: {sup_states}"
                )
                subset = subset[~suppressed_mask].copy()

            subset[self.target_col] = subset[metric_csv_col].astype(float)
            subset["state"] = subset["State Code"].astype(str).str.zfill(2)
            yearly_frames.append((yr, subset[["state", self.target_col]]))

        # --- Aggregate ---
        # Rates are averaged across years; deaths are summed.
        agg = "sum" if self.metric == "deaths" else "mean"

        if self.window == 1:
            _, aggregated = yearly_frames[0]
        else:
            aggregated = self._aggregate_window(
                yearly_frames, value_col=self.target_col, agg=agg
            )

        self._enforce_min_states(
            aggregated,
            context=(
                f"metric={self.metric!r}, window_years={window_years}, "
                f"{len(set(total_suppressed))} unique states suppressed"
            )
        )

        result = self._validate_output(aggregated[["state", self.target_col]])

        window_str = (
            str(window_years[0]) if self.window == 1
            else f"{window_years[0]}–{window_years[-1]}"
        )

        self.last_load_info = {
            "loader":            self.__class__.__name__,
            "metric":            self.metric,
            "window":            self.window,
            "agg":               agg,
            "acs_year":          acs_year,
            "data_year":         data_year,
            "window_years":      window_years,
            "n_states":          len(result),
            "n_suppressed":      len(total_suppressed),
            "suppressed_states": sorted(set(total_suppressed)),
            "target_col":        self.target_col,
        }

        print(
            f"[OpioidLoader] Loaded: metric={self.metric!r}, "
            f"acs_year={acs_year}, years={window_str}, "
            f"n_states={len(result)}, target_col={self.target_col!r}"
        )

        return result