#!/usr/bin/env python3
# dry_run_iterative_screening.py
#
# Iterative family exclusion screening with auto-convergence.
# Runs all years defined in loader.ACS_PAIRINGS and returns a single
# multi-year report JSON keyed by year string, plus a top-level "report" key.
#
# HOW IT WORKS
# ------------
# For each ACS year:
#
#   Round 1a (calibration) — screens all columns with min_abs_r=0.0 to capture
#   the full abs_r distribution, then derives per-year thresholds:
#     min_abs_r    = mean or Q3 of abs_r distribution, clamped by --r-floor-fdr
#     convergence_r = Q1 of abs_r distribution, clamped by --r-floor-convergence
#
#   Rounds 1, 2, 3, ... — screens remaining columns with the computed thresholds,
#   excludes the dominant subfamily (highest mean_abs_r) each round.
#
#   Stops when EITHER:
#     1. Top subfamily mean_abs_r drops below convergence_r
#     2. No FDR-passing columns for --stale-rounds consecutive rounds (default 3)
#     3. No columns remaining
#     4. Round cap reached (default 999 — effectively unlimited)
#
# THRESHOLDS
# ----------
#   --threshold-mode mean   min_abs_r = mean(abs_r)   [default, recommended]
#   --threshold-mode q3     min_abs_r = Q3(abs_r)     [stricter, fewer passing]
#   convergence_r is always Q1(abs_r) regardless of mode
#
#   --r-floor-fdr         floor for min_abs_r    (default 0.05 — safety net only)
#   --r-floor-convergence floor for convergence_r (default 0.10 — prevents runaway)
#
#   Both floors are intentionally low. They are safety nets, not quality bars.
#   The quality bar comes from the data-driven calibration. Setting floors too
#   high suppresses legitimate weak signal (see opioid findings).
#
# OUTPUT
# ------
#   Per year: config, rounds (with full all_columns per round), convergence_reason,
#             rounds_completed, summary
#   report key: convergence_summary, cross_year_summary, stable_features
#     stable_features.columns        — FDR-passing columns stable across years
#     stable_features.fallback_columns — non-FDR columns above computed threshold,
#                                        stable across years (useful for sparse targets)
#     stable_features.subfamilies    — subfamilies consistently excluded as dominant
#

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd


CANCER_CSV   = Path("phase_1/input/United States and Puerto Rico Cancer Statistics, 1999-2022 Incidence.csv")
OPIOID_CSV   = Path("phase_1/input/opioid_deaths_2018_2023.csv")
UFO_CSV      = Path("phase_1/input/nuforc_events_with_state_code.csv")
STATE_DB     = Path("data_server/db/acs_states.duckdb")


# ---------------------------------------------------------------------------
# Convergence reasons (used in the returned dict and printed summary)
# ---------------------------------------------------------------------------
CONVERGE_MEAN_ABS_R  = "mean_abs_r_below_threshold"
CONVERGE_STALE_FDR   = "no_fdr_passing_columns_for_n_consecutive_rounds"
CONVERGE_HIT_CAP     = "reached_round_cap"
CONVERGE_NO_COLS     = "no_columns_remaining"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def short_label(label: str) -> str:
    """Strip ACS boilerplate prefix, return the terminal descriptor."""
    if "!!" in label:
        return label.split("!!")[-1]
    return label


def print_top(column_stats: pd.DataFrame, n: int = 5, indent: str = "  ") -> None:
    for _, row in column_stats.head(n).iterrows():
        flag = "✓" if row["passes_fdr"] else " "
        print(f"{indent}{flag} {row['feature']}  r={row['abs_r']:.3f}  "
              f"[{row['group']}]  {short_label(row['label'])}")


def print_family_summary(family_stats: pd.DataFrame, n: int = 5, indent: str = "  ") -> None:
    top = family_stats.sort_values("mean_abs_r", ascending=False).head(n)
    for _, row in top.iterrows():
        print(f"{indent}{row['top_family']} / {row['subfamily'][:60]:<60} "
              f"mean_r={row['mean_abs_r']:.3f}  sig={row['n_significant']}/{row['n_total']}")


def _row_to_dict(row: pd.Series) -> dict:
    """Convert a DataFrame row to a plain dict, coercing numpy scalars."""
    return {k: (bool(v) if hasattr(v, "item") and isinstance(v.item(), bool)
                else v.item() if hasattr(v, "item") else v)
            for k, v in row.items()}


def _series_get(row: pd.Series, key: str, default=None):
    """Safely read a value from a pandas Series, preserving the default for missing/NA."""
    if key not in row.index:
        return default
    value = row[key]
    if pd.isna(value):
        return default
    return value


def _column_result_to_dict(row: pd.Series) -> dict:
    """
    Serialize one screened ACS feature row for JSON output.
    """
    raw_label = str(_series_get(row, "label", ""))
    return {
        "feature":    str(row["feature"]),
        "abs_r":      round(float(row["abs_r"]), 4),
        "passes_fdr": bool(row["passes_fdr"]),
        "group":      str(_series_get(row, "group", "")),
        "label":      short_label(raw_label),
        "raw_label":  raw_label,
        "concept":    str(_series_get(row, "concept", "")),
    }


# ---------------------------------------------------------------------------
# Loader factory
# ---------------------------------------------------------------------------

def build_loader(loader_name: str, cancer_site: str, metric: str,
                 window: int, acs_year: int):
    """
    Instantiate and return the appropriate loader for the requested dataset.

    Returns (loader, target_col, resolved_acs_year).
    resolved_acs_year is the ACS vintage that will be passed to loader.load().
    """
    if loader_name == "cancer":
        from phase_1.cancer_loader import CancerLoader
        site_slug  = cancer_site.lower().replace(" ", "_").replace(",", "")[:40]
        target_col = f"cancer_incidence_{site_slug}"
        loader = CancerLoader(
            csv_path=CANCER_CSV,
            target_col=target_col,
            cancer_site=cancer_site,
            metric=metric if metric else "count",
            window=window,
        )
        return loader, target_col, acs_year

    if loader_name == "opioid":
        from phase_1.opioid_loader import OpioidLoader
        resolved_metric = metric if metric else "age_adjusted_rate"
        target_col = f"opioid_{resolved_metric}"
        loader = OpioidLoader(
            csv_path=OPIOID_CSV,
            target_col=target_col,
            metric=resolved_metric,
            window=window,
        )
        return loader, target_col, acs_year

    if loader_name == "ufo":
        from phase_1.ufo_loader import UFOLoader
        ufo_supported = [2013, 2014, 2015, 2016]
        if acs_year not in ufo_supported:
            print(
                f"WARNING: --acs-year {acs_year} is not supported by UFOLoader. "
                f"Supported vintages: {ufo_supported}. Defaulting to 2016."
            )
            acs_year = 2016
        target_col = f"ufo_sightings_per_100k_{acs_year}"
        loader = UFOLoader(
            csv_path=UFO_CSV,
            target_col=target_col,
            metric="raw_count",
        )
        return loader, target_col, acs_year

    raise ValueError(
        f"Unknown loader {loader_name!r}. "
        f"Supported: 'cancer', 'opioid', 'ufo'."
    )


# ---------------------------------------------------------------------------
# Core run function
# ---------------------------------------------------------------------------

def run(
    loader_name:         str,
    cancer_site:         str,
    metric:              str,
    n_rounds:            int,
    top_n:               int,
    r_floor_fdr:         float,
    r_floor_convergence: float,
    window:              int,
    threshold_mode:      str        = "mean",  # "mean" or "q3" — drives computed min_abs_r
    stale_rounds:        int        = 3,
    years:               list[int] | None = None,  # optional subset of ACS_PAIRINGS keys to run
    loader_instance=None,                          # optional pre-built loader (skips build_loader)
) -> dict:
    """
    Run iterative family exclusion screening for years in loader.ACS_PAIRINGS,
    until convergence or round cap per year. Years that fail are skipped with a
    warning and their error is recorded in the output.

    Parameters
    ----------
    years          : Optional list of ACS years to run. Must be a subset of the
                     loader's ACS_PAIRINGS keys. If None, all available years run.
    loader_instance: Optional pre-built loader instance (e.g. UserDataLoader).
                     When provided, build_loader() is skipped entirely and
                     loader_name / cancer_site / metric / window are ignored.
                     ACS_PAIRINGS is read directly from the instance.

    Returns a dict keyed by year string:
      {
        "2018": { "config": {...}, "rounds": [...], "convergence_reason": str,
                  "rounds_completed": int, "summary": {...} },
        "2019": { ... },
        "2021": {"error": "..."},   # skipped years record the exception message
      }
    """
    from data_server.acs_loader import get_all_metadata
    from phase_1.Dataset_Builder import PhaseB_1
    from phase_1.generate_feature_families import generate_feature_families
    from phase_1.feature_families import FeatureFamilies
    import phase_1.phase1_screening as p1

    # ---- instantiate loader ------------------------------------------------
    if loader_instance is not None:
        # Path B: pre-built loader passed in directly (e.g. UserDataLoader)
        loader     = loader_instance
        target_col = loader.target_col
    else:
        # Path A: build from loader_name + params
        # Two-step: probe with 2018 to read ACS_PAIRINGS keys, then build
        # properly with the actual minimum year.
        _probe, target_col, _ = build_loader(
            loader_name, cancer_site, metric, window, acs_year=2018
        )
        _all_years = sorted(_probe.ACS_PAIRINGS.keys())
        loader, target_col, _ = build_loader(
            loader_name, cancer_site, metric, window,
            acs_year=_all_years[0],
        )

    # ---- resolve years to run -----------------------------------------------
    available_years = sorted(loader.ACS_PAIRINGS.keys())

    if years is not None:
        unknown = sorted(set(years) - set(available_years))
        if unknown:
            raise ValueError(
                f"run(): requested years {unknown} are not in "
                f"{loader.__class__.__name__}.ACS_PAIRINGS. "
                f"Available: {available_years}"
            )
        available_years = sorted(set(years) & set(available_years))
        if not available_years:
            raise ValueError(
                f"run(): years filter {years} produced an empty intersection "
                f"with available years {sorted(loader.ACS_PAIRINGS.keys())}."
            )

    print(f"  Loader: {loader.__class__.__name__}  |  Years to run: {available_years}")

    # ---- outer year loop ----------------------------------------------------
    all_years_output: dict = {}

    for acs_year in available_years:

        print(f"\n{'='*70}")
        print(f"  Iterative family exclusion screening  (auto-convergence)")
        print(f"  Loader:         {loader.__class__.__name__}")
        print(f"  Target col:     {target_col}")
        if loader_name == "cancer":
            print(f"  Site:           {cancer_site}")
        if loader_name == "ufo":
            print(f"  Note:           raw_count metric — normalise by population post-join")
        print(f"  ACS year:       {acs_year}")
        print(f"  Window:         {window} year(s)")
        print(f"  Round cap:      {n_rounds}")
        print(f"  threshold_mode:      {threshold_mode}  (drives computed min_abs_r)")
        print(f"  r_floor_fdr:         {r_floor_fdr}  (min clamp for min_abs_r)")
        print(f"  r_floor_convergence: {r_floor_convergence}  (min clamp for convergence_r)")
        print(f"  stale_rounds:   {stale_rounds}  (condition 2: 0 FDR-passing for N consecutive rounds)")
        print(f"  (min_abs_r and convergence_r will be computed from round 1a)")
        print(f"{'='*70}\n")

        try:
            # ---- setup ----------------------------------------------------------
            print(f"Loading metadata and dataset for ACS year {acs_year}...")
            metadata  = get_all_metadata(acs_year)
            target_df = loader.load(acs_year=acs_year)

            ds = PhaseB_1(
                level="state", year=acs_year, metadata=metadata,
                user_target_df=target_df, exclude_states_fips=[],
            )

            X = ds["X"].copy()
            y = ds["y"]

            full_tree = generate_feature_families(metadata)
            full_ff   = FeatureFamilies(full_tree)

            # ---- round 1a: calibration run (min_abs_r=0.0) ----------------------
            # Run a permissive screening pass to capture the full abs_r
            # distribution, then derive min_abs_r and convergence_r from it.
            print(f"  Running round 1a (calibration, min_abs_r=0.0)...")
            calib_result = p1.run_screening(
                X=X,
                y=y,
                metadata=metadata,
                config=p1.ScreeningConfig(min_abs_r=0.0),
                loader_info=loader.last_load_info,
            )
            calib_abs_r = calib_result["column_stats"]["abs_r"]

            if threshold_mode == "q3":
                computed_min_abs_r = float(calib_abs_r.quantile(0.75))
            else:  # "mean"
                computed_min_abs_r = float(calib_abs_r.mean())

            computed_convergence_r = float(calib_abs_r.quantile(0.25))

            # Apply separate floor clamps
            effective_min_abs_r     = max(computed_min_abs_r,     r_floor_fdr)
            effective_convergence_r = max(computed_convergence_r, r_floor_convergence)

            print(f"  Calibration complete:")
            print(f"    threshold_mode:      {threshold_mode}")
            print(f"    computed min_abs_r:     {computed_min_abs_r:.4f}  →  clamped to {effective_min_abs_r:.4f}")
            print(f"    computed convergence_r: {computed_convergence_r:.4f}  →  clamped to {effective_convergence_r:.4f}")
            print()

            # ---- per-year state -------------------------------------------------
            excluded_subfamilies:      list[tuple[str, str]] = []
            excluded_cols:             set[str]              = set()
            fdr_passed_and_excluded:   set[str]              = set()
            rounds_output:             list[dict]            = []
            stale_counter:             int                   = 0
            convergence_reason:        str                   = CONVERGE_HIT_CAP

            # ---- config block ---------------------------------------------------
            resolved_metric = metric
            if not resolved_metric:
                if loader_name == "cancer":
                    resolved_metric = "count"
                elif loader_name == "opioid":
                    resolved_metric = "age_adjusted_rate"
                elif loader_name == "ufo":
                    resolved_metric = "raw_count"

            config_block = {
                "loader":                loader_name,
                "loader_class":          loader.__class__.__name__,
                "target_col":            target_col,
                "acs_year":              acs_year,
                "window":                window,
                "metric":                resolved_metric,
                "round_cap":             n_rounds,
                "threshold_mode":        threshold_mode,
                "r_floor_fdr":           r_floor_fdr,
                "r_floor_convergence":   r_floor_convergence,
                "computed_min_abs_r":    round(computed_min_abs_r, 4),
                "min_abs_r":             effective_min_abs_r,
                "convergence_r":         effective_convergence_r,
                "stale_rounds":          stale_rounds,
                "top_n":                 top_n,
            }
            if loader_name == "cancer":
                config_block["cancer_site"] = cancer_site

            # ---- screening rounds -----------------------------------------------
            for round_num in range(1, n_rounds + 1):
                X_round = X.drop(columns=[c for c in excluded_cols if c in X.columns])

                if X_round.shape[1] == 0:
                    print(f"--- Round {round_num} --- STOPPED: no columns remaining")
                    convergence_reason = CONVERGE_NO_COLS
                    break

                print(f"--- Round {round_num} ---")
                print(f"    Input columns: {X_round.shape[1]}  "
                      f"(excluded so far: {len(excluded_cols)} cols from "
                      f"{len(excluded_subfamilies)} subfamilies)")

                result = p1.run_screening(
                    X=X_round,
                    y=y,
                    metadata=metadata,
                    config=p1.ScreeningConfig(min_abs_r=effective_min_abs_r),
                    loader_info=loader.last_load_info,
                )

                cs      = result["column_stats"]
                fs      = result["family_stats"]
                passing = result["passing_columns"]

                print(f"    Passing columns: {len(passing)} / {len(cs)}")
                print(f"\n    Top {top_n} columns:")
                print_top(cs, n=top_n)
                print(f"\n    Top {top_n} subfamilies by mean |r|:")
                print_family_summary(fs, n=top_n)

                fs_sorted = fs.sort_values("mean_abs_r", ascending=False)

                if fs_sorted.empty:
                    print(f"\n    ⚑ CONVERGENCE: family_stats is empty — no columns left to screen.")
                    round_dict["convergence_triggered"] = True
                    round_dict["convergence_reason"]    = CONVERGE_NO_COLS
                    rounds_output.append(round_dict)
                    convergence_reason = CONVERGE_NO_COLS
                    break

                top_sf_row = fs_sorted.iloc[0]
                top_family = top_sf_row["top_family"]
                subfamily  = top_sf_row["subfamily"]
                top_mean_r = float(top_sf_row["mean_abs_r"])
                sf_cols    = full_ff.columns_in_subfamily(top_family, subfamily)

                round_dict: dict = {
                    "round":            round_num,
                    "input_columns":    int(X_round.shape[1]),
                    "passing_columns":  int(len(passing)),
                    "total_screened":   int(len(cs)),
                    "top_columns": [
                        _column_result_to_dict(row)
                        for _, row in cs.head(top_n).iterrows()
                    ],
                    "all_columns": [
                        _column_result_to_dict(row)
                        for _, row in cs.iterrows()
                    ],
                    "top_subfamilies": [
                        {
                            "top_family":    r["top_family"],
                            "subfamily":     r["subfamily"],
                            "mean_abs_r":    round(float(r["mean_abs_r"]), 4),
                            "n_significant": int(r["n_significant"]),
                            "n_total":       int(r["n_total"]),
                        }
                        for _, r in fs_sorted.head(top_n).iterrows()
                    ],
                    "dominant_subfamily": {
                        "top_family":    top_family,
                        "subfamily":     subfamily,
                        "mean_abs_r":    round(top_mean_r, 4),
                        "cols_excluded": len(sf_cols),
                    },
                    "convergence_triggered": False,
                    "convergence_reason":    None,
                }

                # Condition 2: stale FDR counter
                if len(passing) == 0:
                    stale_counter += 1
                else:
                    stale_counter = 0

                # Condition 1: mean_abs_r threshold
                if top_mean_r < effective_convergence_r:
                    print(f"\n    ⚑ CONVERGENCE (condition 1): top subfamily mean_abs_r "
                          f"{top_mean_r:.4f} < convergence_r {effective_convergence_r}. Stopping.")
                    round_dict["convergence_triggered"] = True
                    round_dict["convergence_reason"]    = CONVERGE_MEAN_ABS_R
                    rounds_output.append(round_dict)
                    convergence_reason = CONVERGE_MEAN_ABS_R
                    break

                # Condition 2: stale FDR
                if stale_counter >= stale_rounds:
                    print(f"\n    ⚑ CONVERGENCE (condition 2): 0 FDR-passing columns for "
                          f"{stale_counter} consecutive rounds. Stopping.")
                    round_dict["convergence_triggered"] = True
                    round_dict["convergence_reason"]    = CONVERGE_STALE_FDR
                    rounds_output.append(round_dict)
                    convergence_reason = CONVERGE_STALE_FDR
                    break

                print(f"\n    → Dominant subfamily: [{top_family}] {subfamily}")
                print(f"      {len(sf_cols)} columns will be excluded in round {round_num + 1}")
                print()

                excluded_subfamilies.append((top_family, subfamily))
                excluded_cols.update(sf_cols)
                fdr_passed_and_excluded.update(set(passing) & set(sf_cols))
                rounds_output.append(round_dict)

            else:
                convergence_reason = CONVERGE_HIT_CAP
                print(f"\n  Round cap ({n_rounds}) reached.")

            # ---- per-year summary -----------------------------------------------
            rounds_completed = len(rounds_output)
            total_passing    = sum(r["passing_columns"] for r in rounds_output)

            print(f"\n{'='*70}")
            print(f"  ACS year {acs_year} — Convergence: {convergence_reason}")
            print(f"  Rounds completed: {rounds_completed}")
            print(f"  Summary of excluded subfamilies:")
            for i, (tf, sf) in enumerate(excluded_subfamilies, 1):
                print(f"    Round {i}: [{tf}] {sf}")
            print(f"  Total columns excluded: {len(excluded_cols)}")
            print(f"  FDR-passing columns that were excluded: {len(fdr_passed_and_excluded)}")
            print(f"  Total passing columns across all rounds (FDR): {total_passing}")
            print(f"{'='*70}\n")

            all_years_output[str(acs_year)] = {
                "config":             config_block,
                "rounds":             rounds_output,
                "convergence_reason": convergence_reason,
                "rounds_completed":   rounds_completed,
                "summary": {
                    "excluded_subfamilies": [
                        {"round": i + 1, "top_family": tf, "subfamily": sf}
                        for i, (tf, sf) in enumerate(excluded_subfamilies)
                    ],
                    "total_columns_excluded":          len(excluded_cols),
                    "fdr_passed_and_excluded_columns": len(fdr_passed_and_excluded),
                    "total_passing_columns":           total_passing,
                },
            }

        except Exception as exc:
            print(f"\n  WARNING: ACS year {acs_year} failed — skipping. Error: {exc}\n")
            all_years_output[str(acs_year)] = {"error": str(exc)}

    # ---- build report ------------------------------------------------------
    import statistics

    successful_years = {
        yr: data for yr, data in all_years_output.items()
        if "error" not in data
    }
    errored_years = {
        yr: data for yr, data in all_years_output.items()
        if "error" in data
    }

    # feature_metadata — first-seen lookup for Phase 2 recipe/model reports.
    # This lets stable_features carry labels even though their counts are
    # calculated from compact per-feature tallies below.
    feature_metadata: dict[str, dict] = {}
    for data in successful_years.values():
        for rnd in data["rounds"]:
            for col in rnd["all_columns"]:
                feat = col["feature"]
                if feat not in feature_metadata:
                    feature_metadata[feat] = {
                        "group":     col.get("group", ""),
                        "label":     col.get("label", ""),
                        "raw_label": col.get("raw_label", ""),
                        "concept":   col.get("concept", ""),
                    }

    # 1. convergence_summary — one entry per year
    convergence_summary = {}
    for yr, data in all_years_output.items():
        if "error" in data:
            convergence_summary[yr] = {"status": "error", "error": data["error"]}
        else:
            convergence_summary[yr] = {
                "status":             "ok",
                "convergence_reason": data["convergence_reason"],
                "rounds_completed":   data["rounds_completed"],
            }

    # 2. cross_year_summary — aggregate stats across successful years
    if successful_years:
        rounds_list   = [d["rounds_completed"]                      for d in successful_years.values()]
        excluded_list = [d["summary"]["total_columns_excluded"]      for d in successful_years.values()]
        passing_list  = [d["summary"]["total_passing_columns"]       for d in successful_years.values()]
        cross_year_summary = {
            "successful_years":        len(successful_years),
            "errored_years":           len(errored_years),
            "rounds_to_convergence":   {"mean": round(statistics.mean(rounds_list), 2),   "min": min(rounds_list),   "max": max(rounds_list)},
            "columns_excluded":        {"mean": round(statistics.mean(excluded_list), 2), "min": min(excluded_list), "max": max(excluded_list)},
            "total_passing_columns":   {"mean": round(statistics.mean(passing_list), 2),  "min": min(passing_list),  "max": max(passing_list)},
        }
    else:
        cross_year_summary = {
            "successful_years": 0,
            "errored_years":    len(errored_years),
        }

    # 3. stable_features — columns and subfamilies consistent across years
    #    threshold = median of per-item appearance counts (data-driven)

    # columns: count years each column passed FDR (across all_columns, every round)
    col_year_counts: dict[str, int] = {}
    for data in successful_years.values():
        seen_this_year: set[str] = set()
        for rnd in data["rounds"]:
            for col in rnd["all_columns"]:
                if col["passes_fdr"] and col["feature"] not in seen_this_year:
                    col_year_counts[col["feature"]] = col_year_counts.get(col["feature"], 0) + 1
                    seen_this_year.add(col["feature"])

    # fallback_columns: count years each column's abs_r >= computed_min_abs_r
    # (pre-clamp threshold — the data-driven bar before the floor is applied)
    # tracks FDR-passers and non-passers alike, deduplicated per year
    fallback_year_counts: dict[str, int] = {}
    fallback_fdr_ever:    dict[str, bool] = {}
    for data in successful_years.values():
        threshold = data["config"]["computed_min_abs_r"]
        seen_this_year: set[str] = set()
        for rnd in data["rounds"]:
            for col in rnd["all_columns"]:
                feat = col["feature"]
                if col["abs_r"] >= threshold and feat not in seen_this_year:
                    fallback_year_counts[feat] = fallback_year_counts.get(feat, 0) + 1
                    fallback_fdr_ever[feat]    = fallback_fdr_ever.get(feat, False) or col["passes_fdr"]
                    seen_this_year.add(feat)

    # subfamilies: count years each subfamily was excluded as dominant
    sf_year_counts: dict[str, int] = {}
    for data in successful_years.values():
        for entry in data["summary"]["excluded_subfamilies"]:
            key = f"{entry['top_family']} / {entry['subfamily']}"
            sf_year_counts[key] = sf_year_counts.get(key, 0) + 1

    col_threshold      = statistics.median(col_year_counts.values())      if col_year_counts      else 0
    fallback_threshold = statistics.median(fallback_year_counts.values()) if fallback_year_counts else 0
    sf_threshold       = statistics.median(sf_year_counts.values())       if sf_year_counts       else 0

    stable_columns = sorted(
        [
            {
                "feature": k,
                "years_passed_fdr": v,
                **feature_metadata.get(k, {}),
            }
            for k, v in col_year_counts.items()
            if v >= col_threshold
        ],
        key=lambda x: x["years_passed_fdr"], reverse=True,
    )
    fallback_columns = sorted(
        [
            {
                "feature":            k,
                "years_above_threshold": v,
                "passes_fdr_ever":    fallback_fdr_ever.get(k, False),
                **feature_metadata.get(k, {}),
            }
            for k, v in fallback_year_counts.items()
            if v >= fallback_threshold and k not in col_year_counts
        ],
        key=lambda x: x["years_above_threshold"], reverse=True,
    )
    stable_subfamilies = sorted(
        [{"subfamily": k, "years_dominant": v} for k, v in sf_year_counts.items() if v >= sf_threshold],
        key=lambda x: x["years_dominant"], reverse=True,
    )

    print(f"\n{'='*70}")
    print(f"  REPORT")
    print(f"  Successful years: {len(successful_years)}  |  Errored: {len(errored_years)}")
    print(f"  Stable columns (>= median {col_threshold:.1f} years): {len(stable_columns)}")
    print(f"  Fallback columns (>= median {fallback_threshold:.1f} years, non-FDR): {len(fallback_columns)}")
    print(f"  Stable subfamilies (>= median {sf_threshold:.1f} years): {len(stable_subfamilies)}")
    print(f"{'='*70}\n")

    all_years_output["report"] = {
        "convergence_summary":  convergence_summary,
        "cross_year_summary":   cross_year_summary,
        "stable_features": {
            "col_threshold":       col_threshold,
            "fallback_threshold":  fallback_threshold,
            "sf_threshold":        sf_threshold,
            "columns":             stable_columns,
            "fallback_columns":    fallback_columns,
            "subfamilies":         stable_subfamilies,
        },
        "feature_metadata": feature_metadata,
    }

    return all_years_output


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Iterative family exclusion screening — runs until convergence.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--loader", default="cancer", choices=["cancer", "opioid", "ufo"],
        help="Which target dataset to use (default: cancer)",
    )
    parser.add_argument(
        "--site", default="All Invasive Cancer Sites Combined",
        help="Cancer site name — only used when --loader=cancer "
             "(default: All Invasive Cancer Sites Combined)",
    )
    parser.add_argument(
        "--metric", default="",
        help="Metric to use. cancer: 'count' (only option). "
             "opioid: 'age_adjusted_rate' (default), 'crude_rate', 'deaths'. "
             "ufo: always raw_count.",
    )
    parser.add_argument(
        "--window", type=int, default=1,
        help="Number of years to aggregate for cancer and opioid loaders (default: 1). "
             "Use 5 to aggregate a 5-year window ending at the resolved data year, "
             "matching the ACS 5-year survey scope. Not applicable to ufo.",
    )
    parser.add_argument(
        "--rounds", type=int, default=999,
        help="Hard cap on exclusion rounds (default: 999 — effectively unlimited). "
             "Convergence will normally stop the run well before this cap.",
    )
    parser.add_argument(
        "--stale-rounds", type=int, default=3, dest="stale_rounds",
        help="Stop after this many consecutive rounds with 0 FDR-passing columns "
             "(convergence condition 2, default: 3).",
    )
    parser.add_argument(
        "--top", type=int, default=5,
        help="Number of top columns/families to print and record per round (default: 5)",
    )
    parser.add_argument(
        "--threshold-mode", default="mean", choices=["mean", "q3"], dest="threshold_mode",
        help="Statistic used to derive min_abs_r from the round 1a abs_r distribution. "
             "'mean' uses the mean, 'q3' uses the 75th percentile (default: mean). "
             "convergence_r is always derived from Q1 of the same distribution.",
    )
    parser.add_argument(
        "--r-floor-fdr", type=float, default=0.05, dest="r_floor_fdr",
        help="Minimum clamp applied to computed min_abs_r (default: 0.05). "
             "Keeps the FDR passing bar from dropping into pure noise territory.",
    )
    parser.add_argument(
        "--r-floor-convergence", type=float, default=0.1, dest="r_floor_convergence",
        help="Minimum clamp applied to computed convergence_r (default: 0.1). "
             "Prevents the screener from running indefinitely on genuine noise.",
    )
    parser.add_argument(
        "--output", default=None, metavar="PATH",
        help="Optional path to write the result dict as JSON "
             "(e.g. --output results/opioid_w5.json). "
             "Directory will be created if it does not exist.",
    )
    args = parser.parse_args()

    # ---- input validation ---------------------------------------------------
    if args.loader == "cancer" and not CANCER_CSV.exists():
        print(f"ERROR: Cancer CSV not found at {CANCER_CSV}")
        sys.exit(1)
    if args.loader == "opioid" and not OPIOID_CSV.exists():
        print(f"ERROR: Opioid CSV not found at {OPIOID_CSV}")
        sys.exit(1)
    if args.loader == "ufo" and not UFO_CSV.exists():
        print(f"ERROR: UFO CSV not found at {UFO_CSV}")
        sys.exit(1)
    if not STATE_DB.exists():
        print(f"ERROR: State DuckDB not found at {STATE_DB}")
        sys.exit(1)

    # ---- run ----------------------------------------------------------------
    result = run(
        loader_name=args.loader,
        cancer_site=args.site,
        metric=args.metric,
        n_rounds=args.rounds,
        top_n=args.top,
        r_floor_fdr=args.r_floor_fdr,
        r_floor_convergence=args.r_floor_convergence,
        threshold_mode=args.threshold_mode,
        window=args.window,
        stale_rounds=args.stale_rounds,
    )

    # ---- optional JSON output -----------------------------------------------
    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        print(f"  Result written to: {out_path}")

    return result


if __name__ == "__main__":
    main()