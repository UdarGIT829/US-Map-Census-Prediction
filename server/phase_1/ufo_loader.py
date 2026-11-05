# phase1/input/ufo_loader.py
#
# Loads NUFORC UFO sighting data and produces a target DataFrame compatible
# with Dataset_Builder's PhaseB_1 interface.
#
# Expected file location: phase_1/input/nuforc_events_with_state_code.csv
#
# CSV shape (NUFORC export with FIPS added):
#   Event_Time | Event_Date | Year | Month | Day | Hour | Minute |
#   City | State | Shape | Duration | Summary | Event_URL | State_Code
#
# Key facts about this data:
#   - State_Code is an unpadded integer FIPS (e.g. 6 for California)
#   - Year range: 1930s–2017. Only 2009–2016 overlap with supported ACS
#     vintages AND have full 51-state coverage.
#   - 2017 is a PARTIAL year (~1,200 sightings vs ~5,000–8,000 for full
#     years) — it is excluded from ACS pairings and raises ValueError if
#     requested directly.
#   - All 51 states present for every year 2009–2016.
#
# Metric:
#   "sightings_per_100k" — raw sighting count divided by ACS total population
#     (DP05_0001E). This is the ONLY valid metric for cross-state comparison.
#     Raw counts are strongly correlated with state population size and must
#     not be used as the target variable.
#
#   Population normalisation strategy:
#     The loader aggregates sightings over the 5-year window corresponding
#     to the ACS vintage (e.g. ACS 2015 covers survey years 2011–2015),
#     then divides by the ACS 5-year total population estimate for that
#     vintage. This matches the temporal scope of the ACS data exactly.
#
#     Window pairing:
#       ACS 2013 → sightings 2009–2013  (earliest usable — 2008 and earlier
#                                         have sparse/incomplete data)
#       ACS 2014 → sightings 2010–2014
#       ACS 2015 → sightings 2011–2015
#       ACS 2016 → sightings 2012–2016
#       ACS 2017 → NOT SUPPORTED (2017 data is partial year only)
#
#     Population data source:
#       Passed in by the caller as a pd.Series indexed by zero-padded FIPS
#       string, containing DP05_0001E values. This avoids a second database
#       call inside the loader — the caller already has ACS data from PhaseB_1.
#       Alternatively, use metric="raw_count" and normalise post-join using
#       the pipeline's X matrix (DP05_0001E is always in X).
#
# Output contract (enforced by BaseTargetLoader._validate_output):
#   pd.DataFrame with exactly two columns:
#     "state"      — zero-padded 2-digit FIPS string
#     <target_col> — float64, sightings per 100k population per state
#
#   target_col name is supplied by the caller — nothing is hardcoded here.

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from phase_1.base_loader import BaseTargetLoader


DEFAULT_CSV = Path("phase_1/input/nuforc_events_with_state_code.csv")

# ACS vintage → 5-year sighting window
# The ACS 5-year product pools survey years N-4 through N.
# We match sightings to the same window.
# 2017 is excluded — the CSV is a partial year (cut off ~April 2017).
ACS_SIGHTING_WINDOWS: Dict[int, tuple[int, int]] = {
    2013: (2009, 2013),
    2014: (2010, 2014),
    2015: (2011, 2015),
    2016: (2012, 2016),
}

ACS_CAUTION_VINTAGES: Dict[int, str] = {
    2013: (
        "ACS 2013 uses the earliest available sighting window (2009-2013). "
        "Pre-2010 NUFORC data may be less complete due to lower internet "
        "adoption — reporting bias is more pronounced in earlier years."
    ),
}

_VALID_METRICS = ("sightings_per_100k", "raw_count")

class UFOLoader(BaseTargetLoader):
    """
    Loads NUFORC UFO sighting data and returns per-state sightings per 100k.

    Parameters
    ----------
    csv_path    : Path to the NUFORC CSV.
    target_col  : Name for the output metric column. Supplied by the caller.
                  Example: "ufo_sightings_per_100k_2015".
    population  : pd.Series indexed by zero-padded 2-digit FIPS string,
                  containing ACS DP05_0001E (total population) values.
                  Required when metric="sightings_per_100k" (the default).
                  Pass None only when metric="raw_count".
    metric      : "sightings_per_100k" (default, recommended) or "raw_count".
                  raw_count is provided for diagnostic use only — it must not
                  be used as a target variable in ACS screening because it is
                  dominated by state population size.
    min_states  : Raise if fewer than this many states are returned.
    """

    # Defined at class level for BaseTargetLoader._resolve_year()
    ACS_PAIRINGS: Dict[int, int] = {
        # Maps acs_year → acs_year (identity — year resolution just validates
        # the vintage is supported; the actual window logic is in load())
        2013: 2013,
        2014: 2014,
        2015: 2015,
        2016: 2016,
    }

    ACS_CAUTION_VINTAGES: Dict[int, str] = ACS_CAUTION_VINTAGES

    def __init__(
        self,
        *,
        csv_path: str | Path = DEFAULT_CSV,
        target_col: str,
        population: Optional[pd.Series] = None,
        metric: str = "sightings_per_100k",
        min_states: int = 45,
    ):
        super().__init__(csv_path=csv_path, target_col=target_col, min_states=min_states)

        valid_metrics = ("sightings_per_100k", "raw_count")
        if metric not in valid_metrics:
            raise ValueError(
                f"UFOLoader: metric={metric!r} is not supported. "
                f"Choose from: {sorted(valid_metrics)}."
            )
        if metric == "sightings_per_100k" and population is None:
            raise ValueError(
                "UFOLoader: metric='sightings_per_100k' requires a population "
                "Series (ACS DP05_0001E indexed by zero-padded FIPS). "
                "Pass population=ds['X']['DP05_0001E'] after building the dataset, "
                "or use metric='raw_count' and normalise post-join."
            )

        self.metric     = metric
        self.population = population

    def load(
        self,
        *,
        acs_year: Optional[int] = None,
        year: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Load UFO sighting data and return a two-column DataFrame.

        Aggregates sightings over the 5-year window matching the ACS vintage,
        then normalises by population if metric="sightings_per_100k".

        See BaseTargetLoader.load() for the full parameter and return contract.

        After a successful load, self.last_load_info is set to a dict with:
            loader, metric, acs_year, sighting_window, n_states,
            total_sightings, target_col
        """
        data_year = self._resolve_year(acs_year, year)
        self._check_csv_exists()

        # --- Resolve sighting window ---
        if data_year not in ACS_SIGHTING_WINDOWS:
            supported = sorted(ACS_SIGHTING_WINDOWS.keys())
            raise ValueError(
                f"UFOLoader: ACS vintage {data_year} is not supported. "
                f"Supported vintages: {supported[0]}–{supported[-1]}. "
                f"Note: ACS 2017 is excluded because the CSV is a partial year "
                f"(cut off ~April 2017, only ~1,200 sightings vs ~5,000–8,000 "
                f"for full years)."
            )

        window_start, window_end = ACS_SIGHTING_WINDOWS[data_year]

        df = pd.read_csv(self.csv_path, usecols=["Year", "State_Code"], low_memory=False)

        # --- Filter to sighting window ---
        mask   = df["Year"].between(window_start, window_end)
        subset = df[mask].copy()

        if len(subset) == 0:
            raise ValueError(
                f"UFOLoader: no sightings found for window "
                f"{window_start}–{window_end}."
            )

        # --- Zero-pad FIPS ---
        subset["state"] = subset["State_Code"].astype(int).apply(
            lambda x: str(x).zfill(2)
        )

        # --- Aggregate: count sightings per state over the full window ---
        counts = (
            subset.groupby("state")
            .size()
            .reset_index(name="raw_count")
        )

        total_sightings = int(counts["raw_count"].sum())

        # --- Build metric column ---
        if self.metric == "raw_count":
            counts[self.target_col] = counts["raw_count"].astype(float)

        else:  # sightings_per_100k
            # population is a Series indexed by zero-padded FIPS
            pop_df = (
                self.population
                .reset_index()
                .rename(columns={"index": "state", self.population.name: "population"})
            )
            # Handle both Series with named index and unnamed index
            if pop_df.columns.tolist() == [0, "population"]:
                pop_df = pop_df.rename(columns={0: "state"})

            pop_df["state"] = pop_df["state"].astype(str).str.zfill(2)
            pop_df["population"] = pd.to_numeric(pop_df["population"], errors="coerce")

            counts = counts.merge(pop_df, on="state", how="inner")

            zero_pop = (counts["population"] <= 0) | counts["population"].isna()
            if zero_pop.any():
                bad = counts.loc[zero_pop, "state"].tolist()
                print(
                    f"[UFOLoader] WARNING: {len(bad)} states have zero/NaN population "
                    f"and will be dropped: {bad}"
                )
                counts = counts[~zero_pop].copy()

            counts[self.target_col] = (
                counts["raw_count"] / counts["population"] * 100_000
            ).round(4)

        self._enforce_min_states(
            counts,
            context=f"acs_year={data_year}, window={window_start}–{window_end}"
        )

        result = self._validate_output(counts[["state", self.target_col]])

        self.last_load_info = {
            "loader":          self.__class__.__name__,
            "metric":          self.metric,
            "acs_year":        acs_year,
            "data_year":       data_year,
            "sighting_window": f"{window_start}–{window_end}",
            "n_states":        len(result),
            "total_sightings": total_sightings,
            "target_col":      self.target_col,
        }

        print(
            f"[UFOLoader] Loaded: metric={self.metric!r}, "
            f"acs_year={acs_year}, window={window_start}–{window_end}, "
            f"total_sightings={total_sightings:,}, "
            f"n_states={len(result)}, target_col={self.target_col!r}"
        )

        return result

    @staticmethod
    def available_years(csv_path: str | Path = DEFAULT_CSV) -> list[int]:
        """Return sorted list of full-coverage years in the CSV (excludes 2017)."""
        return sorted(ACS_SIGHTING_WINDOWS.keys())

    @staticmethod
    def sighting_window(acs_year: int) -> tuple[int, int]:
        """Return the (start, end) sighting year window for a given ACS vintage."""
        if acs_year not in ACS_SIGHTING_WINDOWS:
            raise ValueError(
                f"acs_year={acs_year} is not supported. "
                f"Supported: {sorted(ACS_SIGHTING_WINDOWS.keys())}."
            )
        return ACS_SIGHTING_WINDOWS[acs_year]