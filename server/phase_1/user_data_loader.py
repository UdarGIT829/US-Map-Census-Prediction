# phase_1/user_data_loader.py
#
# Wraps user-uploaded tabular data (from Godot) in the BaseTargetLoader
# interface so it can flow through the same screening pipeline as any
# built-in loader.
#
# The user selects in Godot:
#   - fips_col    : which column contains state FIPS codes
#   - target_col  : which column is the target variable
#   - year_col    : which column contains years (optional)
#   - single_year : a single year typed by the user (used when year_col=None)
#
# ACS_PAIRINGS is built dynamically from the data:
#   - year_col provided  → {yr: yr} for every unique year found in that column
#   - single_year only   → {single_year: single_year}
#
# load(acs_year=...) filters rows to that year (or uses all rows when
# single_year was provided and there is no year column).
#
# Output contract (enforced by BaseTargetLoader._validate_output):
#   pd.DataFrame with exactly two columns:
#     "state"         — zero-padded 2-digit FIPS string, e.g. "06"
#     <target_col>    — float64, one row per state
#
# Notes:
#   - ACS_CAUTION_VINTAGES is empty — user data has no known caution vintages
#   - FIPS zero-padding is handled by _validate_output
#   - Duplicate state rows for a given year raise ValueError via _validate_output
#   - Rows with non-numeric target values are dropped with a warning

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from phase_1.base_loader import BaseTargetLoader


class UserDataLoader(BaseTargetLoader):
    """
    Wraps user-uploaded rows in the BaseTargetLoader interface.

    Parameters
    ----------
    rows        : List of dicts — raw rows as parsed by Godot.
    fips_col    : Column name containing state FIPS codes.
    target_col  : Column name containing the target variable.
    year_col    : Column name containing year values. Pass None if the
                  data has no year column — use single_year instead.
    single_year : The year to assign when year_col is None. Ignored if
                  year_col is provided.
    min_states  : Raise if fewer than this many states survive filtering.
                  Default 10 — user data may intentionally cover fewer
                  states than a national dataset.
    """

    ACS_CAUTION_VINTAGES: Dict[int, str] = {}

    def __init__(
        self,
        *,
        rows: List[Dict],
        fips_col: str,
        target_col: str,
        year_col: Optional[str] = None,
        single_year: Optional[int] = None,
        min_states: int = 10,
    ):
        if not rows:
            raise ValueError("UserDataLoader: rows must be a non-empty list.")
        if not fips_col:
            raise ValueError("UserDataLoader: fips_col must be provided.")
        if not target_col:
            raise ValueError("UserDataLoader: target_col must be provided.")
        if year_col is None and single_year is None:
            raise ValueError(
                "UserDataLoader: either year_col or single_year must be provided."
            )

        # BaseTargetLoader expects csv_path — pass a sentinel since we have
        # in-memory data. _check_csv_exists() is never called on this loader.
        super().__init__(
            csv_path="__in_memory__",
            target_col=target_col,
            min_states=min_states,
        )

        self.fips_col    = fips_col
        self.year_col    = year_col
        self.single_year = single_year

        # Build DataFrame from rows once — reused across all load() calls
        self._df = pd.DataFrame(rows)

        # Validate required columns exist
        for col, name in [(fips_col, "fips_col"), (target_col, "target_col")]:
            if col not in self._df.columns:
                raise ValueError(
                    f"UserDataLoader: {name}={col!r} not found in rows. "
                    f"Available columns: {sorted(self._df.columns.tolist())}"
                )
        if year_col is not None and year_col not in self._df.columns:
            raise ValueError(
                f"UserDataLoader: year_col={year_col!r} not found in rows. "
                f"Available columns: {sorted(self._df.columns.tolist())}"
            )

        # Build ACS_PAIRINGS dynamically
        if year_col is not None:
            years = sorted(
                self._df[year_col]
                .dropna()
                .astype(int)
                .unique()
                .tolist()
            )
            if not years:
                raise ValueError(
                    f"UserDataLoader: year_col={year_col!r} contains no valid years."
                )
            self.ACS_PAIRINGS = {yr: yr for yr in years}
        else:
            self.ACS_PAIRINGS = {single_year: single_year}

    def load(
        self,
        *,
        acs_year: Optional[int] = None,
        year: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Filter rows to acs_year and return a two-column DataFrame.

        When single_year was provided (no year_col), all rows are used
        regardless of any year-like column that may exist in the data.

        After a successful load, self.last_load_info is set to a dict with:
            loader, fips_col, target_col, year_col, acs_year, n_states
        """
        data_year = self._resolve_year(acs_year, year)

        # Filter to the requested year (or use all rows for single_year mode)
        if self.year_col is not None:
            subset = self._df[
                self._df[self.year_col].astype(int) == data_year
            ].copy()
            if len(subset) == 0:
                raise ValueError(
                    f"UserDataLoader: no rows found for year={data_year} "
                    f"in column {self.year_col!r}."
                )
        else:
            # single_year mode — entire dataset belongs to this one year
            subset = self._df.copy()

        # Build state column from fips_col
        subset["state"] = subset[self.fips_col].astype(str).str.zfill(2)

        # Cast target to numeric, drop rows where it fails
        subset[self.target_col] = pd.to_numeric(
            subset[self.target_col], errors="coerce"
        )
        n_before = len(subset)
        subset = subset.dropna(subset=[self.target_col])
        n_dropped = n_before - len(subset)
        if n_dropped > 0:
            print(
                f"[UserDataLoader] WARNING: {n_dropped} row(s) dropped — "
                f"non-numeric values in target_col={self.target_col!r}."
            )

        self._enforce_min_states(
            subset,
            context=f"year={data_year}, fips_col={self.fips_col!r}, "
                    f"target_col={self.target_col!r}"
        )

        result = self._validate_output(subset[["state", self.target_col]])

        self.last_load_info = {
            "loader":     self.__class__.__name__,
            "fips_col":   self.fips_col,
            "target_col": self.target_col,
            "year_col":   self.year_col,
            "acs_year":   acs_year,
            "data_year":  data_year,
            "n_states":   len(result),
        }

        print(
            f"[UserDataLoader] Loaded: year={data_year}, "
            f"n_states={len(result)}, target_col={self.target_col!r}"
        )

        return result
