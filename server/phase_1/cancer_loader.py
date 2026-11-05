# phase1/input/cancer_loader.py
#
# Loads the CDC cancer incidence CSV and produces a target DataFrame
# compatible with Dataset_Builder's PhaseB_1 interface.
#
# Expected file location:  phase1/input/<filename>.csv
#
# CSV shape (CDC USCS incidence export):
#   Notes | Cancer Sites | Cancer Sites Code | States | States Code | Count | Year
#
# Key facts about this data:
#   - States Code is FIPS but NOT zero-padded (California = 6, not "06")
#   - Count is always a positive integer — no suppression markers in this export
#   - 51 states (50 + DC), no Puerto Rico
#   - 104 cancer site types
#   - Years: 1999–2022
#   - Not all (cancer site, year) combos have all 51 states — some rare sites
#     have as few as 1 state in some years
#
# Output contract (enforced by BaseTargetLoader._validate_output):
#   pd.DataFrame with exactly two columns:
#     "state"       — zero-padded 2-digit FIPS string, e.g. "06"
#     <target_col>  — float64, raw incidence count per state
#
#   target_col name is supplied by the caller — nothing is hardcoded here.
#   This DataFrame is passed to PhaseB_1(user_target_df=...) which joins
#   it to ACS data on target_key="state".
#
# Metric note:
#   Only "count" (raw incidence) is available in this export.
#   Population-normalised rates must be derived post-join by dividing by
#   ACS DP05_0001E (total population). This is a known limitation of the
#   CDC USCS export format; see Phase_1.md "KNOWN RISKS" section 5.
#
# Window aggregation:
#   window=1 (default) loads a single cancer year — the standard behaviour.
#   window=5 sums counts across the 5-year window ending at the resolved
#   cancer year, matching the ACS 5-year survey scope exactly. This is the
#   methodologically correct approach for correlating with ACS 5-year estimates.
#   Example: ACS 2022, window=5 → sums cancer years 2018–2022.
#
# Backward compatibility:
#   The module-level load_cancer_target() function is kept so that existing
#   callers and tests (test_phase1.py Section 3) continue to work unchanged.
#   New code should use CancerLoader directly.

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from phase_1.base_loader import BaseTargetLoader


DEFAULT_CSV = Path("phase1/input/United_States_and_Puerto_Rico_Cancer_Statistics__1999-2022_Incidence.csv")
ALL_SITES   = "All Invasive Cancer Sites Combined"


class CancerLoader(BaseTargetLoader):
    """
    Loads CDC USCS cancer incidence data for a given site and year.

    Parameters
    ----------
    csv_path    : Path to the CDC USCS incidence CSV.
    target_col  : Name for the output metric column. Supplied by the caller.
                  Example: "cancer_incidence_all_sites_2022".
    cancer_site : Cancer site name as it appears in the 'Cancer Sites' column.
                  Default: ALL_SITES ("All Invasive Cancer Sites Combined").
                  Call CancerLoader.list_sites(csv_path) to enumerate valid values.
    metric      : Only "count" is supported in this export. Population-normalised
                  rates are not available; derive them post-join from DP05_0001E.
    window      : Number of years to aggregate. Default 1 (single year, current
                  behaviour). Use 5 to sum counts across the 5-year window ending
                  at the resolved cancer year, matching the ACS 5-year scope.
                  window=5 with ACS 2022 sums cancer years 2018–2022.
    min_states  : Raise if fewer than this many states are found after filtering.
                  Lower this for rare cancer sites (some have <10 states).
    """

    # ACS vintage → cancer data year
    # ACS 2023 uses 2022 cancer data — no 2023 CDC USCS publication yet.
    ACS_PAIRINGS: Dict[int, int] = {
        2009: 2009,
        2010: 2010,
        2011: 2011,
        2012: 2012,
        2013: 2013,
        2014: 2014,
        2015: 2015,
        2016: 2016,
        2017: 2017,
        2018: 2018,
        2019: 2019,   # last pre-redesign ACS vintage
        2020: 2020,   # experimental ACS vintage — COVID disruption
        2021: 2021,
        2022: 2022,
    }

    ACS_CAUTION_VINTAGES: Dict[int, str] = {
        2020: (
            "ACS 2020 5-year is marked experimental by the Census Bureau due to "
            "COVID-19 data collection disruptions. Cancer counts in 2020 are also "
            "genuinely lower (delayed diagnoses during lockdowns — California dropped "
            "from ~187k to ~170k). Use with caution."
        ),
    }

    def __init__(
        self,
        *,
        csv_path: str | Path = DEFAULT_CSV,
        target_col: str,
        cancer_site: str = ALL_SITES,
        metric: str = "count",
        window: int = 1,
        min_states: int = 45,
    ):
        super().__init__(csv_path=csv_path, target_col=target_col, min_states=min_states)

        if metric != "count":
            raise ValueError(
                f"metric={metric!r} is not supported. Only 'count' is available "
                f"in this export. Population-normalised rates can be derived by "
                f"dividing by ACS DP05_0001E after joining."
            )
        if window < 1:
            raise ValueError(f"CancerLoader: window must be >= 1, got {window}.")

        self.cancer_site = cancer_site
        self.metric      = metric
        self.window      = window

    def load(
        self,
        *,
        acs_year: Optional[int] = None,
        year: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Load cancer incidence data and return a two-column DataFrame.

        When window=1 (default), loads a single cancer year.
        When window>1, sums counts across the window years ending at the
        resolved cancer year. States missing in ANY window year are dropped.

        See BaseTargetLoader.load() for the full parameter and return contract.

        After a successful load, self.last_load_info is set to a dict with:
            loader, cancer_site, metric, window, acs_year, data_year,
            window_years, n_states, target_col
        """
        cancer_year = self._resolve_year(acs_year, year)
        self._check_csv_exists()

        df = pd.read_csv(self.csv_path)

        # --- Validate cancer_site ---
        available_sites = df["Cancer Sites"].unique()
        if self.cancer_site not in available_sites:
            raise ValueError(
                f"CancerLoader: cancer_site={self.cancer_site!r} not found in CSV.\n"
                f"Available sites ({len(available_sites)} total):\n"
                + "\n".join(f"  {s}" for s in sorted(available_sites))
            )

        # --- Resolve window years ---
        window_years = self._resolve_window(cancer_year, self.window)

        csv_years = sorted(df["Year"].unique())
        missing_years = [y for y in window_years if y not in csv_years]
        if missing_years:
            raise ValueError(
                f"CancerLoader: years {missing_years} not found in CSV "
                f"(window={self.window}, end_year={cancer_year}). "
                f"Available years: {csv_years[0]}–{csv_years[-1]}."
            )

        # --- Load each year in the window ---
        yearly_frames = []
        for yr in window_years:
            mask   = (df["Cancer Sites"] == self.cancer_site) & (df["Year"] == yr)
            subset = df[mask].copy()
            if len(subset) == 0:
                yearly_frames.append((yr, None))
                continue
            subset["state"] = subset["States Code"].astype(int).apply(
                lambda x: str(x).zfill(2)
            )
            subset = subset.rename(columns={"Count": self.target_col})
            subset[self.target_col] = subset[self.target_col].astype(float)
            yearly_frames.append((yr, subset[["state", self.target_col]]))

        # --- Aggregate ---
        # Counts are summed across years; a 5-year total is more stable than
        # any single year and matches the ACS 5-year survey scope.
        if self.window == 1:
            _, single = yearly_frames[0]
            aggregated = single
        else:
            aggregated = self._aggregate_window(
                yearly_frames, value_col=self.target_col, agg="sum"
            )

        self._enforce_min_states(
            aggregated,
            context=f"cancer_site={self.cancer_site!r}, window_years={window_years}."
        )

        result = self._validate_output(aggregated[["state", self.target_col]])

        window_str = (
            str(window_years[0]) if self.window == 1
            else f"{window_years[0]}–{window_years[-1]}"
        )

        self.last_load_info = {
            "loader":       self.__class__.__name__,
            "cancer_site":  self.cancer_site,
            "metric":       self.metric,
            "window":       self.window,
            "acs_year":     acs_year,
            "data_year":    cancer_year,
            "window_years": window_years,
            "n_states":     len(result),
            "target_col":   self.target_col,
        }

        print(
            f"[CancerLoader] Loaded: cancer_site={self.cancer_site!r}, "
            f"acs_year={acs_year}, years={window_str}, "
            f"n_states={len(result)}, target_col={self.target_col!r}"
        )

        return result

    # ------------------------------------------------------------------
    # Discovery helpers (no instance needed)
    # ------------------------------------------------------------------

    @staticmethod
    def list_sites(csv_path: str | Path = DEFAULT_CSV) -> list[str]:
        """Return sorted list of all cancer site names in the CSV."""
        p = Path(csv_path)
        if not p.exists():
            raise FileNotFoundError(f"CSV not found at {p.resolve()}")
        df = pd.read_csv(p, usecols=["Cancer Sites"])
        return sorted(df["Cancer Sites"].unique().tolist())

    @staticmethod
    def available_years(
        csv_path: str | Path = DEFAULT_CSV,
        cancer_site: Optional[str] = None,
    ) -> list[int]:
        """
        Return sorted list of years in the CSV.
        If cancer_site is provided, restricts to years available for that site.
        """
        p = Path(csv_path)
        if not p.exists():
            raise FileNotFoundError(f"CSV not found at {p.resolve()}")
        df = pd.read_csv(p, usecols=["Cancer Sites", "Year"])
        if cancer_site is not None:
            df = df[df["Cancer Sites"] == cancer_site]
        return sorted(df["Year"].unique().tolist())


# ---------------------------------------------------------------------------
# Backward-compatible module-level function
# ---------------------------------------------------------------------------
# Kept so that existing callers and test_phase1.py Section 3 continue to work.
# New code should use CancerLoader directly.

def load_cancer_target(
    *,
    csv_path: str | Path = DEFAULT_CSV,
    cancer_site: str = ALL_SITES,
    acs_year: Optional[int] = None,
    year: Optional[int] = None,
    metric: str = "count",
    target_col: str = "cancer_incidence",
    min_states: int = 45,
) -> pd.DataFrame:
    """
    Backward-compatible wrapper around CancerLoader.

    Prefer CancerLoader directly in new code — this function exists only so
    existing callers and tests do not need to change.
    """
    loader = CancerLoader(
        csv_path=csv_path,
        target_col=target_col,
        cancer_site=cancer_site,
        metric=metric,
        min_states=min_states,
    )
    return loader.load(acs_year=acs_year, year=year)


# Keep old discovery functions at module level for backward compat
def list_cancer_sites(csv_path: str | Path = DEFAULT_CSV) -> list[str]:
    return CancerLoader.list_sites(csv_path)


def available_years(
    csv_path: str | Path = DEFAULT_CSV,
    cancer_site: Optional[str] = None,
) -> list[int]:
    return CancerLoader.available_years(csv_path, cancer_site)