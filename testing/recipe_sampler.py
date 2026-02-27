#!/usr/bin/env python3
"""
Phase 2: Combination-Based Feature Evaluation ("Recipe Sampling")

Given:
  - An ACS feature matrix X (from Dataset_Provider)
  - A target vector y
  - A FeatureFamilies instance (Phase 1 tree)

This module:
  - Randomly samples "recipes" of features by:
        1) picking a small set of top-level families
        2) sampling columns from those families
  - Trains a fast ElasticNet model on each recipe using cross-validation
  - Records each recipe's performance
  - Aggregates combination-aware importance scores at:
        - column level
        - family/subfamily level

Intended usage:
  from feature_families import FeatureFamilies
  from recipe_sampler import RecipeConfig, run_recipe_sampling

  ff = FeatureFamilies.from_json("feature_families.json")
  results = run_recipe_sampling(X, y, ff, RecipeConfig(...))

  # results["column_scores"] → ranked columns
  # results["family_scores"]  → ranked families/subfamilies
  # results["recipes"]        → per-recipe details
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from sklearn.linear_model import ElasticNet
from sklearn.model_selection import cross_val_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from feature_families import FeatureFamilies, FamilyPath


# ---------------------------------------------------------------------------
# Configuration & result data structures
# ---------------------------------------------------------------------------

current_evaluation_num = 1

@dataclass
class RecipeConfig:
    """
    Configuration for the recipe sampling process.

    n_recipes:        target total number of recipes (coverage + random)
    min_families:     minimum number of top-level families per random recipe
    max_families:     maximum number of top-level families per random recipe
    min_features:     minimum total number of features per random recipe
    max_features:     maximum total number of features per recipe (also used
                      when chunking coverage recipes)
    cv_folds:         number of CV folds for scoring each recipe
    random_state:     seed for reproducible sampling (None = non-deterministic)
    coverage_rounds:  how many times to “pass over” all features in coverage.
                      Each round shuffles and re-chunks, so each feature appears
                      in ~coverage_rounds recipes with different neighbors.
    """
    n_recipes: int = 200
    min_families: int = 1
    max_families: int = 3
    min_features: int = 20
    max_features: int = 60
    cv_folds: int = 3
    random_state: Optional[int] = 42
    coverage_rounds: int = 5


@dataclass
class RecipeResult:
    """
    Summary of a single recipe evaluation.
    """
    recipe_id: int
    features: List[str]
    families: List[str]           # top-level family names used in this recipe
    score_mean: float             # mean CV score (e.g., R^2)
    score_std: float              # std of CV scores
    n_features: int
    n_rows_used: int              # number of rows after NaN dropping
    error: Optional[str] = None   # if something went wrong, store message


# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------

def _make_rng(seed: Optional[int]) -> np.random.Generator:
    if seed is None:
        return np.random.default_rng()
    return np.random.default_rng(seed)

# in recipe_sampler.py or a new utilities module

from typing import Optional
import pandas as pd

from dp_labels import human_label  # you already have this
from feature_families import FeatureFamilies, FamilyPath


def annotate_column_scores(
    column_scores: pd.DataFrame,
    ff: FeatureFamilies,
    dp_map: dict,
    *,
    include_concept: bool = False,
    top_n: Optional[int] = None,
) -> pd.DataFrame:
    """
    Take the plain column_scores DataFrame and augment it with:

      - human-readable DP label
      - DP concept (optional)
      - top_family
      - subfamily
      - inner_subfamily

    Returns a new DataFrame sorted by mean_score, then count.
    If top_n is provided, returns only the first N rows.
    """
    rows = []

    for _, row in column_scores.iterrows():
        feat = row["feature"]
        mean_score = float(row["mean_score"])
        count = int(row["count"])

        # Get human label from dp_mapping
        label = human_label(feat, dp_map, include_concept=include_concept)

        # Get family info from FeatureFamilies
        path: Optional[FamilyPath] = ff.path_for_column(feat)
        if path is not None:
            top_family = path.top_family
            subfamily = path.subfamily
            inner_subfamily = path.inner_subfamily
        else:
            top_family = None
            subfamily = None
            inner_subfamily = None

        rows.append(
            {
                "feature": feat,
                "label": label,
                "top_family": top_family,
                "subfamily": subfamily,
                "inner_subfamily": inner_subfamily,
                "mean_score": mean_score,
                "count": count,
            }
        )

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    df = df.sort_values(
        by=["mean_score", "count"],
        ascending=[False, False],
    ).reset_index(drop=True)

    if top_n is not None:
        df = df.head(top_n)

    return df


def _build_coverage_recipes(
    fam_to_cols: Dict[str, List[str]],
    cfg: RecipeConfig,
    rng: np.random.Generator,
) -> List[List[str]]:
    """
    Build a list of feature lists ("coverage recipes") such that every
    usable feature appears in multiple recipes.

    Strategy:
      - Flatten all columns from all families into a single deduped list.
      - For each coverage round:
          - shuffle the list
          - partition into chunks of size up to cfg.max_features
        → Each round adds a new set of recipes where every feature appears
          once, with different neighbors.

    Each feature will appear in ~coverage_rounds recipes (subject to chunking).
    """
    # Flatten and de-duplicate features (preserve initial order)
    all_cols: List[str] = []
    for cols in fam_to_cols.values():
        all_cols.extend(cols)
    seen = set()
    usable_features: List[str] = []
    for c in all_cols:
        if c not in seen:
            seen.add(c)
            usable_features.append(c)

    if not usable_features:
        return []

    max_size = max(1, cfg.max_features)
    coverage_recipes: List[List[str]] = []

    for _ in range(max(1, cfg.coverage_rounds)):
        # Shuffle a copy each round so neighbors change
        shuffled = usable_features.copy()
        rng.shuffle(shuffled)
        # Chunk
        for i in range(0, len(shuffled), max_size):
            chunk = shuffled[i : i + max_size]
            coverage_recipes.append(chunk)

    return coverage_recipes


def _build_family_to_columns(
    X_cols: Sequence[str],
    ff: FeatureFamilies,
) -> Dict[str, List[str]]:
    """
    Map each top-level family to the list of columns in X that belong to it.

    Only includes columns that:
      - appear in X
      - appear in the feature family index
    """
    fam_to_cols: Dict[str, List[str]] = {}
    for col in X_cols:
        path: Optional[FamilyPath] = ff.path_for_column(col)
        if path is None:
            continue
        fam_to_cols.setdefault(path.top_family, []).append(col)
    return fam_to_cols


def _filter_constant_columns(X: pd.DataFrame, cols: List[str]) -> List[str]:
    """
    Remove columns that are constant (or all NaN), which don't help learning.
    """
    out: List[str] = []
    for c in cols:
        series = X[c]
        # Drop NaNs and see if more than one unique value remains
        non_null = series.dropna()
        if non_null.nunique() > 1:
            out.append(c)
    return out


def _sample_recipe_features(
    rng: np.random.Generator,
    fam_to_cols: Dict[str, List[str]],
    cfg: RecipeConfig,
) -> Tuple[List[str], List[str]]:
    """
    Sample a set of features for one recipe.

    Returns:
        (feature_list, family_list)

    feature_list: list of column names to use in this recipe
    family_list:  list of top-level families from which we sampled
    """
    if not fam_to_cols:
        raise ValueError("No families with usable columns found for recipe sampling.")

    all_families = list(fam_to_cols.keys())

    # Number of families for this recipe
    max_fam = min(cfg.max_families, len(all_families))
    min_fam = min(cfg.min_families, max_fam)
    n_families = rng.integers(min_fam, max_fam + 1)

    families = list(rng.choice(all_families, size=n_families, replace=False))

    # Total number of features for this recipe
    max_possible_features = sum(len(fam_to_cols[f]) for f in families)
    if max_possible_features <= 0:
        raise ValueError("Chosen families have no columns.")

    # Clamp min/max to what is feasible
    min_feat = min(cfg.min_features, max_possible_features)
    max_feat = min(cfg.max_features, max_possible_features)
    if min_feat > max_feat:
        # If we have fewer columns than min_features, just use all of them
        target_features = max_possible_features
    else:
        target_features = int(rng.integers(min_feat, max_feat + 1))

    # Distribute the target_features across the chosen families
    features: List[str] = []
    remaining = target_features
    remaining_families = len(families)

    for family in families:
        available_cols = fam_to_cols[family]
        if remaining_families <= 0:
            break

        # Rough per-family quota (ceil to ensure we can hit target)
        quota = int(np.ceil(remaining / remaining_families))
        quota = min(quota, len(available_cols))

        if quota <= 0:
            remaining_families -= 1
            continue

        sampled = list(rng.choice(available_cols, size=quota, replace=False))
        features.extend(sampled)

        remaining -= quota
        remaining_families -= 1

        if remaining <= 0:
            break

    # De-duplicate while preserving order, just in case
    seen = set()
    dedup_features: List[str] = []
    for c in features:
        if c not in seen:
            seen.add(c)
            dedup_features.append(c)

    if not dedup_features:
        # Fallback: pick one random column from all families
        any_family = rng.choice(all_families)
        dedup_features = [rng.choice(fam_to_cols[any_family])]

    return dedup_features, families


from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score

def _evaluate_recipe(
    X: pd.DataFrame,
    y: pd.Series | np.ndarray,
    feature_list: List[str],
    cfg: RecipeConfig,
) -> Tuple[float, float, int]:
    """
    Fast, approximate evaluation of a recipe:

    - Use a single train/validation split instead of K-fold CV.
    - Use a smaller max_iter for ElasticNet.
    - Return (mean_score, std_score, n_rows_used), where std_score is 0.0
      because we're not doing multiple folds.

    This is good enough for RELATIVE ranking of recipes and massively faster.
    """
    if len(feature_list) == 0:
        return np.nan, np.nan, 0

    # Subset features (no .copy() needed; values will be copied by .values anyway)
    X_sub = X[feature_list]

    # Wrap y as Series once
    if isinstance(y, pd.Series):
        y_series = y
    else:
        y_series = pd.Series(y, index=X.index)

    # Drop rows with NaNs in any selected feature or in y
    mask = X_sub.notna().all(axis=1) & y_series.notna()
    X_clean = X_sub.loc[mask]
    y_clean = y_series.loc[mask]

    n_rows = len(y_clean)
    # Need at least a handful of samples to split meaningfully
    if n_rows < 10:
        return np.nan, np.nan, n_rows

    # Single train/validation split (fast)
    X_train, X_val, y_train, y_val = train_test_split(
        X_clean.values,
        y_clean.values,
        test_size=0.25,
        random_state=cfg.random_state,
    )

    model = make_pipeline(
        StandardScaler(),
        ElasticNet(
            alpha=1.0,
            l1_ratio=0.5,
            random_state=cfg.random_state,
            max_iter=250,   # much lower than 2000
        ),
    )

    model.fit(X_train, y_train)
    y_pred = model.predict(X_val)
    score = r2_score(y_val, y_pred)

    global current_evaluation_num
    current_evaluation_num += 1

    # std_score is 0.0 because we no longer do multiple folds
    return float(score), 0.0, n_rows


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_recipe_sampling(
    X: pd.DataFrame,
    y: pd.Series | np.ndarray,
    ff: FeatureFamilies,
    cfg: Optional[RecipeConfig] = None,
) -> Dict[str, Any]:
    """
    Main entry point for Phase 2.

    Now with a coverage phase that ensures every usable feature is included
    in at least one recipe.

    n_recipes in cfg is treated as the *target total* number of recipes.
    If the number of coverage recipes exceeds n_recipes, we still run
    all coverage recipes (to guarantee coverage) and skip random recipes.
    """
    if cfg is None:
        cfg = RecipeConfig()

    rng = _make_rng(cfg.random_state)

    # 1) Build mapping from family → usable columns (and drop constant columns)
    fam_to_cols_raw = _build_family_to_columns(X.columns, ff)
    fam_to_cols: Dict[str, List[str]] = {}
    for fam, cols in fam_to_cols_raw.items():
        good_cols = _filter_constant_columns(X, cols)
        if good_cols:
            fam_to_cols[fam] = good_cols

    if not fam_to_cols:
        raise ValueError("No non-constant columns with valid family assignments were found.")

    # --- NEW: coverage recipes ---
    coverage_recipes = _build_coverage_recipes(fam_to_cols, cfg, rng)
    n_coverage = len(coverage_recipes)
    target_total = max(cfg.n_recipes, n_coverage)
    remaining_random = max(0, target_total - n_coverage)

    recipes: List[RecipeResult] = []
    col_stats: Dict[str, Dict[str, float]] = {}  # feature -> {"count": float, "score_sum": float}
    recipe_id = 0

    # 2) Coverage phase
    # ----------------------------------------------------------------------
    for feat_list in coverage_recipes:
        # Derive the list of families used in this coverage recipe
        families = []
        fam_seen = set()
        for col in feat_list:
            path = ff.path_for_column(col)
            if path is not None and path.top_family not in fam_seen:
                fam_seen.add(path.top_family)
                families.append(path.top_family)

        try:
            if current_evaluation_num % 10 == 0:
                print(f"Current Evaluation #{current_evaluation_num}")

            score_mean, score_std, n_rows = _evaluate_recipe(X, y, feat_list, cfg)
        except Exception as e:
            recipes.append(
                RecipeResult(
                    recipe_id=recipe_id,
                    features=feat_list,
                    families=families,
                    score_mean=float("nan"),
                    score_std=float("nan"),
                    n_features=len(feat_list),
                    n_rows_used=0,
                    error=str(e),
                )
            )
            recipe_id += 1
            continue

        recipes.append(
            RecipeResult(
                recipe_id=recipe_id,
                features=feat_list,
                families=families,
                score_mean=score_mean,
                score_std=score_std,
                n_features=len(feat_list),
                n_rows_used=n_rows,
                error=None,
            )
        )
        recipe_id += 1

        # Update column-level stats
        if not np.isnan(score_mean):
            for col in feat_list:
                if col not in col_stats:
                    col_stats[col] = {"count": 0.0, "score_sum": 0.0}
                col_stats[col]["count"] += 1.0
                col_stats[col]["score_sum"] += score_mean

    current_evaluation_num = 1
    # 3) Random phase (existing logic), with reduced budget
    # ----------------------------------------------------------------------
    for _ in range(remaining_random):
        try:
            feature_list, fam_list = _sample_recipe_features(rng, fam_to_cols, cfg)
        except Exception as e:
            recipes.append(
                RecipeResult(
                    recipe_id=recipe_id,
                    features=[],
                    families=[],
                    score_mean=float("nan"),
                    score_std=float("nan"),
                    n_features=0,
                    n_rows_used=0,
                    error=str(e),
                )
            )
            recipe_id += 1
            continue

        try:
            if current_evaluation_num % 10 == 0:
                print(f"Current Evaluation #{current_evaluation_num}")
            score_mean, score_std, n_rows = _evaluate_recipe(X, y, feature_list, cfg)
        except Exception as e:
            recipes.append(
                RecipeResult(
                    recipe_id=recipe_id,
                    features=feature_list,
                    families=fam_list,
                    score_mean=float("nan"),
                    score_std=float("nan"),
                    n_features=len(feature_list),
                    n_rows_used=0,
                    error=str(e),
                )
            )
            recipe_id += 1
            continue

        recipes.append(
            RecipeResult(
                recipe_id=recipe_id,
                features=feature_list,
                families=fam_list,
                score_mean=score_mean,
                score_std=score_std,
                n_features=len(feature_list),
                n_rows_used=n_rows,
                error=None,
            )
        )
        recipe_id += 1

        if not np.isnan(score_mean):
            for col in feature_list:
                if col not in col_stats:
                    col_stats[col] = {"count": 0.0, "score_sum": 0.0}
                col_stats[col]["count"] += 1.0
                col_stats[col]["score_sum"] += score_mean

    # 4) Aggregate column-level scores (unchanged)
    # ----------------------------------------------------------------------
    column_rows: List[Dict[str, Any]] = []
    for col, agg in col_stats.items():
        count = agg["count"]
        if count <= 0:
            continue
        column_rows.append(
            {
                "feature": col,
                "mean_score": agg["score_sum"] / count,
                "count": int(count),
            }
        )

    column_scores_df = pd.DataFrame(column_rows)
    if not column_scores_df.empty:
        column_scores_df.sort_values(
            by=["mean_score", "count"],
            ascending=[False, False],
            inplace=True,
        )
        column_scores_df.reset_index(drop=True, inplace=True)

    # 5) Aggregate family/subfamily-level scores (unchanged)
    # ----------------------------------------------------------------------
    fam_agg: Dict[Tuple[str, str], Dict[str, float]] = {}
    for _, row in column_scores_df.iterrows():
        feature = row["feature"]
        mean_score = float(row["mean_score"])
        path = ff.path_for_column(feature)
        if path is None:
            continue
        key = (path.top_family, path.subfamily)
        if key not in fam_agg:
            fam_agg[key] = {"score_sum": 0.0, "count": 0.0}
        fam_agg[key]["score_sum"] += mean_score
        fam_agg[key]["count"] += 1.0

    fam_rows: List[Dict[str, Any]] = []
    for (top_fam, subfam), agg in fam_agg.items():
        count = agg["count"]
        if count <= 0:
            continue
        fam_rows.append(
            {
                "top_family": top_fam,
                "subfamily": subfam,
                "mean_score": agg["score_sum"] / count,
                "feature_count": int(count),
            }
        )

    family_scores_df = pd.DataFrame(fam_rows)
    if not family_scores_df.empty:
        family_scores_df.sort_values(
            by=["mean_score", "feature_count"],
            ascending=[False, False],
            inplace=True,
        )
        family_scores_df.reset_index(drop=True, inplace=True)


    return {
        "recipes": recipes,
        "column_scores": column_scores_df,
        "family_scores": family_scores_df,
    }
