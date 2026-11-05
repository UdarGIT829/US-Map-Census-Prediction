# phase1/input/base_loader.py
#
# Abstract base class for all Phase 1 target variable loaders.
#
# Every concrete loader (CancerLoader, OpioidLoader, etc.) must:
#   1. Subclass BaseTargetLoader
#   2. Implement load() returning a two-column DataFrame
#   3. Define ACS_PAIRINGS and ACS_CAUTION_VINTAGES class attributes
#
# Output contract (enforced by _validate_output):
#   pd.DataFrame with exactly two columns:
#     "state"          — zero-padded 2-digit FIPS string, e.g. "06"
#     <self.target_col> — float64, one row per state
#
# The target_col name is supplied by the caller and flows through the entire
# pipeline without modification. PhaseB_1 infers it automatically from
# whichever column in user_target_df is not "state". Nothing downstream is
# hardcoded to any specific column name.
#
# Year resolution contract:
#   - Pass acs_year to use the loader's recommended data-year pairing.
#   - Pass year to specify a data year directly (bypass the pairing table).
#   - Passing both is allowed only if they agree; disagreement raises ValueError.
#   - Passing neither raises ValueError.
#
# Window aggregation:
#   Loaders that support window= will aggregate over a multi-year window
#   ending at the resolved data year. window=1 (default) preserves the
#   existing single-year behaviour exactly.
#   Use _resolve_window() to compute the year range, then pass the resulting
#   DataFrame slices to _aggregate_window() which handles sum vs mean logic.
#
# Loader info:
#   After a successful load(), self.last_load_info is populated with a dict
#   describing what was loaded. This is passed into run_screening() as
#   loader_info so the screening result is self-documenting.

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd


class BaseTargetLoader(ABC):
    """
    Abstract base class for all Phase 1 target variable loaders.

    Class attributes to define in subclasses
    -----------------------------------------
    ACS_PAIRINGS : dict[int, int]
        Maps ACS vintage year → recommended data year for this dataset.
        Must cover all supported vintages.
    ACS_CAUTION_VINTAGES : dict[int, str]
        Maps ACS vintage year → caution message for flagged vintages.
        May be empty.

    Constructor parameters
    ----------------------
    csv_path   : Path to the source CSV.
    target_col : Name for the output metric column. Supplied by the caller —
                 the front-end will eventually pass this. Loaders never
                 hardcode a default; it must always be provided explicitly.
    min_states : Raise ValueError if fewer rows than this survive all filters.
                 Default 45 allows for suppression without a silent bad join.
    """

    # Subclasses must define these
    ACS_PAIRINGS: Dict[int, int] = {}
    ACS_CAUTION_VINTAGES: Dict[int, str] = {}

    def __init__(
        self,
        *,
        csv_path: str | Path,
        target_col: str,
        min_states: int = 45,
    ):
        if not target_col or not target_col.strip():
            raise ValueError(
                f"{self.__class__.__name__}: target_col must be a non-empty string. "
                f"The caller (or front-end) must supply the column name explicitly — "
                f"loaders do not provide defaults."
            )
        self.csv_path   = Path(csv_path)
        self.target_col = target_col
        self.min_states = min_states

        # Populated after a successful load(); None until then.
        self.last_load_info: Optional[Dict[str, Any]] = None

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    def load(
        self,
        *,
        acs_year: Optional[int] = None,
        year: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Load and return a per-state target DataFrame.

        Year resolution
        ---------------
        Exactly one of acs_year or year must be provided.
          acs_year : ACS vintage (e.g. 2023). The loader resolves the
                     recommended data year via self.ACS_PAIRINGS.
          year     : Data year specified directly. Use only when you need
                     a year that differs from the recommended pairing.

        Returns
        -------
        pd.DataFrame with exactly two columns: ["state", self.target_col]
          "state"           — zero-padded 2-digit FIPS string
          self.target_col   — float64 metric, one row per state, no duplicates

        Side effects
        ------------
        Populates self.last_load_info with a dict describing the load.
        Callers should pass this to run_screening() as loader_info.

        Raises
        ------
        FileNotFoundError : csv_path does not exist.
        ValueError        : year resolution fails, metric unavailable,
                            or fewer than min_states rows returned.
        """
        ...

    # ------------------------------------------------------------------
    # Shared year-resolution logic
    # ------------------------------------------------------------------

    def _resolve_year(
        self,
        acs_year: Optional[int],
        year: Optional[int],
    ) -> int:
        """
        Resolve acs_year / year to a concrete data year.

        Uses self.ACS_PAIRINGS. Prints caution notes from
        self.ACS_CAUTION_VINTAGES when relevant.
        """
        if not self.ACS_PAIRINGS:
            raise NotImplementedError(
                f"{self.__class__.__name__}.ACS_PAIRINGS is empty. "
                f"Subclasses must define a vintage → data-year mapping."
            )

        if acs_year is not None and year is not None:
            recommended = self._recommended_year(acs_year)
            if year != recommended:
                raise ValueError(
                    f"{self.__class__.__name__}: acs_year={acs_year} recommends "
                    f"data year={recommended}, but year={year} was also supplied "
                    f"and they disagree. Pass only acs_year to use the recommended "
                    f"pairing, or only year to override explicitly."
                )
            return year

        if acs_year is not None:
            data_year = self._recommended_year(acs_year)
            note = self.ACS_CAUTION_VINTAGES.get(acs_year)
            if note:
                print(f"[{self.__class__.__name__}] CAUTION (acs_year={acs_year}): {note}")
            return data_year

        if year is not None:
            return year

        raise ValueError(
            f"{self.__class__.__name__}: exactly one of acs_year or year must be "
            f"provided. Use acs_year to use the recommended pairing, or year to "
            f"specify a data year directly."
        )

    def _recommended_year(self, acs_year: int) -> int:
        """Look up the recommended data year for an ACS vintage."""
        if acs_year not in self.ACS_PAIRINGS:
            supported = sorted(self.ACS_PAIRINGS.keys())
            raise ValueError(
                f"{self.__class__.__name__}: acs_year={acs_year} is not a supported "
                f"ACS 5-year vintage. Supported: {supported[0]}–{supported[-1]}."
            )
        return self.ACS_PAIRINGS[acs_year]

    # ------------------------------------------------------------------
    # Window aggregation helpers
    # ------------------------------------------------------------------

    def _resolve_window(self, end_year: int, window: int) -> list[int]:
        """
        Return the list of data years in the aggregation window.

        The window always ends at end_year (inclusive) and extends
        window years back. window=1 returns [end_year] exactly,
        preserving single-year behaviour.

        Example: end_year=2022, window=5 → [2018, 2019, 2020, 2021, 2022]
        """
        if window < 1:
            raise ValueError(
                f"{self.__class__.__name__}: window must be >= 1, got {window}."
            )
        return list(range(end_year - window + 1, end_year + 1))

    def _aggregate_window(
        self,
        yearly_frames: list,         # list of (year, pd.DataFrame) with "state" + value_col
        value_col: str,
        agg: str = "mean",           # "mean" or "sum"
    ) -> pd.DataFrame:
        """
        Aggregate a list of per-year DataFrames into a single per-state DataFrame.

        Parameters
        ----------
        yearly_frames : list of (year, df) where df has columns ["state", value_col].
                        Years with no data (None df) are skipped with a warning.
        value_col     : the numeric column to aggregate.
        agg           : "mean" averages across years (for rates),
                        "sum" totals across years (for counts).

        Returns a DataFrame with columns ["state", value_col], one row per state.
        States that are missing in ANY year are dropped (inner join semantics),
        ensuring every returned state has complete coverage across the window.
        A warning is printed listing any dropped states.
        """
        valid_frames = []
        skipped_years = []

        for yr, df in yearly_frames:
            if df is None or len(df) == 0:
                skipped_years.append(yr)
                continue
            valid_frames.append(df[["state", value_col]].copy().assign(_year=yr))

        if skipped_years:
            print(
                f"[{self.__class__.__name__}] WARNING: no data for years "
                f"{skipped_years} — excluded from window aggregation."
            )

        if not valid_frames:
            raise ValueError(
                f"{self.__class__.__name__}: no valid data found across the "
                f"requested window. Check that the CSV covers the required years."
            )

        combined = pd.concat(valid_frames, ignore_index=True)

        # Inner join semantics: only states present in ALL years
        years_present = combined.groupby("state")["_year"].nunique()
        n_years       = len(valid_frames)
        complete      = years_present[years_present == n_years].index
        dropped       = sorted(set(combined["state"].unique()) - set(complete))

        if dropped:
            print(
                f"[{self.__class__.__name__}] WARNING: {len(dropped)} state(s) "
                f"dropped — missing in at least one window year: {dropped}"
            )

        combined = combined[combined["state"].isin(complete)]

        if agg == "mean":
            result = (
                combined.groupby("state")[value_col]
                .mean()
                .reset_index()
            )
        elif agg == "sum":
            result = (
                combined.groupby("state")[value_col]
                .sum()
                .reset_index()
            )
        else:
            raise ValueError(
                f"{self.__class__.__name__}: agg={agg!r} is not supported. "
                f"Use 'mean' or 'sum'."
            )

        return result

    # ------------------------------------------------------------------
    # Shared validation utilities
    # ------------------------------------------------------------------

    def _check_csv_exists(self) -> None:
        if not self.csv_path.exists():
            raise FileNotFoundError(
                f"{self.__class__.__name__}: CSV not found at "
                f"{self.csv_path.resolve()}."
            )

    def _enforce_min_states(self, df: pd.DataFrame, context: str = "") -> None:
        """Raise ValueError if df has fewer rows than self.min_states."""
        if len(df) < self.min_states:
            raise ValueError(
                f"{self.__class__.__name__}: only {len(df)} states found"
                + (f" ({context})" if context else "")
                + f". Minimum required: {self.min_states}."
            )

    def _validate_output(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Assert the two-column contract and return a clean, normalised copy.

        Enforces:
          - Exactly the columns {"state", self.target_col}
          - "state" is zero-padded 2-digit FIPS strings
          - target_col is float64
          - No duplicate state FIPS codes
        """
        expected = {"state", self.target_col}
        actual   = set(df.columns)
        if actual != expected:
            raise ValueError(
                f"{self.__class__.__name__}.load() must return columns "
                f"{sorted(expected)}, got {sorted(actual)}."
            )

        out = df[["state", self.target_col]].copy()
        out["state"]          = out["state"].astype(str).str.zfill(2)
        out[self.target_col]  = out[self.target_col].astype(float)

        dupes = out["state"].duplicated().sum()
        if dupes > 0:
            raise ValueError(
                f"{self.__class__.__name__}: {dupes} duplicate state FIPS codes "
                f"in output — each state must appear exactly once."
            )

        return out.reset_index(drop=True)