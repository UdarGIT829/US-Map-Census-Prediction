# phase1_screening.py
#
# Phase 1 univariate screening module.
#
# Exports a single public function:
#
#   run_screening(X, y, metadata, config=None, loader_info=None) -> dict
#
# The returned dict has five keys:
#   "column_stats"    — pd.DataFrame, one row per tested column
#   "family_stats"    — pd.DataFrame, one row per (top_family, subfamily)
#   "passing_columns" — list[str], variable codes where passes_fdr=True
#   "filtered_ff"     — FeatureFamilies built from passing_columns only
#   "loader_info"     — dict from BaseTargetLoader.last_load_info, or None
#
# loader_info is purely descriptive — it documents what target variable was
# used, which year, which metric, how many states, etc. It does not affect
# any screening logic. Pass loader.last_load_info after calling loader.load().
#
# Typical usage:
#
#   loader = OpioidLoader(target_col="opioid_age_adjusted_rate")
#   target_df = loader.load(acs_year=2023)
#   dataset   = PhaseB_1(..., user_target_df=target_df)
#   results   = run_screening(
#       X           = dataset["X"],
#       y           = dataset["y"],
#       metadata    = metadata,
#       loader_info = loader.last_load_info,
#   )
#
# See Phase_1.md (PHASE 1 MODULE SPECIFICATION) for the full contract.

from __future__ import annotations

import warnings
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from scipy import stats
from statsmodels.stats.multitest import multipletests

from phase_1.feature_families import FeatureFamilies
from phase_1.generate_feature_families import generate_feature_families
from phase_1.dp_labels import human_label, split_dp_var


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class ScreeningConfig:
    """
    Configuration for run_screening().

    fdr_threshold   : BH-FDR q threshold. A column passes if EITHER its
                      BH-adjusted Pearson p OR its BH-adjusted Spearman p
                      is below this value.
    min_abs_r       : Minimum |r| floor.  Columns whose abs_r is below this
                      are excluded regardless of FDR result.
                      Default 0.3 (moderate-correlation floor).  This is the
                      primary lever for controlling how many columns survive
                      screening.  With n=51 state-level rows and raw cancer
                      counts as the target, BH-FDR alone passes ~90% of
                      columns because ACS population-size variables have
                      astronomically small p-values.  The 0.3 floor gives
                      the "much smaller, more reliable feature set" described
                      in Phase_1.md while staying well above noise.
    use_pearson     : Include Pearson |r| when computing abs_r.
    use_spearman    : Include Spearman |r| when computing abs_r.
    min_nonnan_frac : Columns with a non-NaN fraction below this are dropped
                      before testing (too sparse to produce reliable stats).
    """
    fdr_threshold:   float = 0.1
    min_abs_r:       float = 0.3
    use_pearson:     bool  = True
    use_spearman:    bool  = True
    min_nonnan_frac: float = 0.8


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _bh_adjust(pvals: np.ndarray) -> np.ndarray:
    """
    Apply Benjamini-Hochberg FDR correction to a 1-D array of p-values.

    NaN entries are excluded from the correction, then restored as NaN in
    the output so the result array has the same length as the input.
    """
    adjusted = np.full(len(pvals), np.nan)
    valid_mask = ~np.isnan(pvals)
    if valid_mask.sum() == 0:
        return adjusted
    _, padj, _, _ = multipletests(pvals[valid_mask], method="fdr_bh")
    adjusted[valid_mask] = padj
    return adjusted


def _group_from_varcode(col: str) -> str:
    """Return the DP group (e.g. 'DP05') from a variable code, or '' if unknown."""
    parts = split_dp_var(col)
    return parts[0] if parts else ""


def _safe_pearsonr(x: np.ndarray, y: np.ndarray):
    """Return (r, p) or (NaN, NaN), suppressing ConstantInputWarning."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            r, p = stats.pearsonr(x, y)
            if np.isnan(r):
                return np.nan, np.nan
            return float(r), float(p)
        except Exception:
            return np.nan, np.nan


def _safe_spearmanr(x: np.ndarray, y: np.ndarray):
    """Return (rho, p) or (NaN, NaN), suppressing ConstantInputWarning."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            result = stats.spearmanr(x, y)
            r, p = float(result.statistic), float(result.pvalue)
            if np.isnan(r):
                return np.nan, np.nan
            return r, p
        except Exception:
            return np.nan, np.nan


def _prune_tree(
    full_tree: Dict[str, Any],
    keep_cols: set,
) -> Dict[str, Any]:
    """
    Return a copy of full_tree containing only columns present in keep_cols.

    Empty subfamilies and families are omitted so that
    FeatureFamilies(pruned).col_index.keys() == keep_cols exactly.
    """
    pruned: Dict[str, Any] = {}

    for top_family, subfamilies in full_tree.items():
        pruned_sfs: Dict[str, Any] = {}

        for sf_name, payload in subfamilies.items():
            orig_cols   = payload.get("columns", [])
            orig_labels = payload.get("labels", [])

            sf_cols   = [c for c in orig_cols if c in keep_cols]
            sf_labels = [lbl for c, lbl in zip(orig_cols, orig_labels) if c in keep_cols]

            # Prune inner subfamilies
            pruned_inner: Dict[str, Any] = {}
            for inner_name, inner_payload in (payload.get("inner") or {}).items():
                i_orig_cols   = inner_payload.get("columns", [])
                i_orig_labels = inner_payload.get("labels", [])
                i_cols   = [c for c in i_orig_cols if c in keep_cols]
                i_labels = [lbl for c, lbl in zip(i_orig_cols, i_orig_labels) if c in keep_cols]
                if i_cols:
                    pruned_inner[inner_name] = {"columns": i_cols, "labels": i_labels}

            if not sf_cols:
                continue

            pruned_sf: Dict[str, Any] = {"columns": sf_cols, "labels": sf_labels}
            if pruned_inner:
                pruned_sf["inner"] = pruned_inner

            pruned_sfs[sf_name] = pruned_sf

        if pruned_sfs:
            pruned[top_family] = pruned_sfs

    return pruned


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_screening(
    X: pd.DataFrame,
    y: pd.Series,
    metadata: Dict[str, Any],
    config: Optional[ScreeningConfig] = None,
    loader_info: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Run Phase 1 univariate screening.

    Parameters
    ----------
    X           : Feature matrix. Rows = geographies, columns = DP variable codes.
                  Expected to contain E-suffix columns only (done by Dataset_Provider).
    y           : Target variable aligned to X's index.
    metadata    : Phase 0 get_all_metadata() output. Used for labels and for
                  regenerating the feature family tree.
    config      : ScreeningConfig (uses defaults if None).
    loader_info : Optional dict from BaseTargetLoader.last_load_info.
                  Stamped onto the result dict as-is — documents what target
                  variable was used without affecting any screening logic.
                  Pass loader.last_load_info after calling loader.load().

                  Example contents (OpioidLoader):
                    {"loader": "OpioidLoader", "metric": "age_adjusted_rate",
                     "acs_year": 2023, "data_year": 2023, "n_states": 50,
                     "n_suppressed": 1, "suppressed_states": ["Vermont"],
                     "target_col": "opioid_age_adjusted_rate"}

                  Example contents (CancerLoader):
                    {"loader": "CancerLoader", "cancer_site": "All Invasive...",
                     "metric": "count", "acs_year": 2023, "data_year": 2022,
                     "n_states": 51, "target_col": "cancer_incidence"}

    Returns
    -------
    dict with keys:
        "column_stats"    — pd.DataFrame, one row per column in X, sorted by abs_r desc
        "family_stats"    — pd.DataFrame, one row per (top_family, subfamily)
        "passing_columns" — list[str]
        "filtered_ff"     — FeatureFamilies (passing columns only)
        "loader_info"     — dict from loader.last_load_info, or None if not provided
    """
    if config is None:
        config = ScreeningConfig()

    # ------------------------------------------------------------------
    # 1. Align X and y on shared index; drop rows where y is NaN.
    # ------------------------------------------------------------------
    X, y = X.align(y, join="inner", axis=0)
    valid_y_mask = ~y.isna()
    X = X.loc[valid_y_mask]
    y = y.loc[valid_y_mask]

    y_vals = y.values.astype(float)

    # ------------------------------------------------------------------
    # 2. Drop columns too sparse to test reliably.
    # ------------------------------------------------------------------
    nonnan_fracs = X.notna().mean()
    usable_cols  = nonnan_fracs[nonnan_fracs >= config.min_nonnan_frac].index.tolist()
    X_test       = X[usable_cols]

    # ------------------------------------------------------------------
    # 3. Compute Pearson and Spearman correlations per column.
    #    Constant columns produce NaN r/p via _safe_pearsonr/_safe_spearmanr.
    # ------------------------------------------------------------------
    records = []
    for col in X_test.columns:
        col_vals = X_test[col].values.astype(float)

        mask    = ~np.isnan(col_vals)
        n_valid = mask.sum()

        if n_valid < 3:
            records.append(dict(feature=col,
                                pearson_r=np.nan, pearson_p=np.nan,
                                spearman_r=np.nan, spearman_p=np.nan))
            continue

        x_valid = col_vals[mask]
        y_valid = y_vals[mask]

        pr, pp = _safe_pearsonr(x_valid, y_valid)  if config.use_pearson  else (np.nan, np.nan)
        sr, sp = _safe_spearmanr(x_valid, y_valid) if config.use_spearman else (np.nan, np.nan)

        records.append(dict(feature=col,
                            pearson_r=pr, pearson_p=pp,
                            spearman_r=sr, spearman_p=sp))

    df = pd.DataFrame(records)

    # ------------------------------------------------------------------
    # 4. BH-FDR correction applied separately to Pearson and Spearman.
    # ------------------------------------------------------------------
    df["pearson_p_adj"]  = _bh_adjust(df["pearson_p"].values)
    df["spearman_p_adj"] = _bh_adjust(df["spearman_p"].values)

    # ------------------------------------------------------------------
    # 5. abs_r = max(|pearson_r|, |spearman_r|) across enabled metrics.
    #    pd.DataFrame.max(axis=1) propagates NaN only if ALL values are NaN;
    #    if at least one is finite the finite value wins.
    # ------------------------------------------------------------------
    abs_candidates = []
    if config.use_pearson:
        abs_candidates.append(df["pearson_r"].abs())
    if config.use_spearman:
        abs_candidates.append(df["spearman_r"].abs())

    if abs_candidates:
        df["abs_r"] = pd.concat(abs_candidates, axis=1).max(axis=1)
    else:
        df["abs_r"] = np.nan

    # ------------------------------------------------------------------
    # 6. passes_fdr:
    #    FDR gate  — adjusted p <= fdr_threshold (NaN treated as 1.0 → fails)
    #    abs_r gate — abs_r >= min_abs_r          (NaN treated as 0.0 → fails)
    #    Both must be satisfied.
    # ------------------------------------------------------------------
    pearson_sig  = df["pearson_p_adj"].fillna(1.0)  <= config.fdr_threshold
    spearman_sig = df["spearman_p_adj"].fillna(1.0) <= config.fdr_threshold
    fdr_passes   = pearson_sig | spearman_sig
    absr_passes  = df["abs_r"].fillna(0.0) >= config.min_abs_r

    df["passes_fdr"] = (fdr_passes & absr_passes).astype(bool)

    # ------------------------------------------------------------------
    # 7. Human-readable labels and group code.
    # ------------------------------------------------------------------
    df["label"] = df["feature"].apply(lambda c: human_label(c, metadata))
    df["group"] = df["feature"].apply(_group_from_varcode)

    # ------------------------------------------------------------------
    # 8. Sort by abs_r descending; NaN abs_r values go to the bottom.
    # ------------------------------------------------------------------
    df = df.sort_values("abs_r", ascending=False, na_position="last").reset_index(drop=True)

    col_order = [
        "feature", "label", "group",
        "pearson_r", "pearson_p", "pearson_p_adj",
        "spearman_r", "spearman_p", "spearman_p_adj",
        "abs_r", "passes_fdr",
    ]
    df = df[col_order]

    # ------------------------------------------------------------------
    # 9. passing_columns
    # ------------------------------------------------------------------
    # Drop columns where correlation could not be computed (constant input, etc.)
    df = df.dropna(subset=["abs_r", "pearson_p", "spearman_p"]).reset_index(drop=True)

    passing_columns: List[str] = df.loc[df["passes_fdr"], "feature"].tolist()

    # ------------------------------------------------------------------
    # 10. family_stats — one row per (top_family, subfamily).
    #     n_total counts columns that were actually tested.
    # ------------------------------------------------------------------
    full_tree = generate_feature_families(metadata)
    full_ff   = FeatureFamilies(full_tree)

    feature_to_absr = dict(zip(df["feature"], df["abs_r"]))
    feature_to_sig  = dict(zip(df["feature"], df["passes_fdr"]))

    family_rows = []
    for top_family in full_ff.families():
        for sf in full_ff.subfamilies(top_family):
            sf_cols     = full_ff.columns_in_subfamily(top_family, sf)
            tested_cols = [c for c in sf_cols if c in feature_to_absr]
            if not tested_cols:
                continue
            abs_rs = [feature_to_absr[c] for c in tested_cols
                      if not np.isnan(feature_to_absr.get(c, np.nan))]
            n_sig  = sum(1 for c in tested_cols if feature_to_sig.get(c, False))
            family_rows.append({
                "top_family":    top_family,
                "subfamily":     sf,
                "mean_abs_r":    float(np.mean(abs_rs)) if abs_rs else 0.0,
                "max_abs_r":     float(np.max(abs_rs))  if abs_rs else 0.0,
                "n_significant": n_sig,
                "n_total":       len(tested_cols),
            })

    family_stats = pd.DataFrame(family_rows, columns=[
        "top_family", "subfamily", "mean_abs_r", "max_abs_r",
        "n_significant", "n_total",
    ])

    # ------------------------------------------------------------------
    # 11. filtered_ff — FeatureFamilies pruned to passing_columns only.
    # ------------------------------------------------------------------
    pruned_tree = _prune_tree(full_tree, set(passing_columns))
    filtered_ff = FeatureFamilies(pruned_tree)

    return {
        "column_stats":    df,
        "family_stats":    family_stats,
        "passing_columns": passing_columns,
        "filtered_ff":     filtered_ff,
        "loader_info":     loader_info,   # None if not provided; never affects logic
    }