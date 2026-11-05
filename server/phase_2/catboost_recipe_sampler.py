#!/usr/bin/env python3
# phase_2/catboost_recipe_sampler.py
#
# Phase 2: CatBoost Recipe Sampling over a screened ACS feature pool.
#
# This is the corrected Phase 2 method:
#
#   Phase 1 screening does NOT create one giant final model recipe.
#   Phase 1 filters the ACS feature space into a screened candidate pool.
#
#   Phase 2 samples many small bounded "recipes" from that screened pool,
#   evaluates each recipe with CatBoost, then aggregates recipe performance
#   back into:
#
#       - recipe scores
#       - column scores
#       - family/subfamily scores
#       - pairwise co-elevation / lift scores
#
# Input:
#   results/phase_2/<dataset_dir>/manifest.json
#
# Produced by:
#   phase_2/ml_dataset_builder.py
#
# Output:
#   results/phase_2/<dataset_dir>/catboost_recipe_sampling_report.json
#
# Example:
#
#   python phase_2/catboost_recipe_sampler.py \
#       --dataset-manifest results/phase_2/cancer_all_screened_dataset/manifest.json \
#       --output results/phase_2/cancer_all_screened_dataset/catboost_recipe_sampling_report.json \
#       --mode per_year \
#       --n-recipes 200 \
#       --coverage-rounds 5 \
#       --min-features 8 \
#       --max-features 30
#
# Pooled:
#
#   python phase_2/catboost_recipe_sampler.py \
#       --dataset-manifest results/phase_2/cancer_all_screened_dataset/manifest.json \
#       --mode both
#
# Notes:
#   State-level ACS datasets are small. Treat scores as exploratory predictive
#   evidence, not causal claims.

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass
from itertools import combinations
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Project root path setup
# ---------------------------------------------------------------------------

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ---------------------------------------------------------------------------
# CatBoost and sklearn
# ---------------------------------------------------------------------------

try:
    from catboost import CatBoostRegressor
except Exception as exc:  # pragma: no cover
    raise RuntimeError(
        "CatBoost is required for Phase 2 recipe sampling. "
        "Install it with: pip install catboost\n"
        f"Original import error: {exc}"
    )

try:
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
    from sklearn.model_selection import train_test_split
except Exception as exc:  # pragma: no cover
    raise RuntimeError(
        "catboost_recipe_sampler.py requires scikit-learn for train/test splitting "
        f"and metrics. Import error: {exc}"
    )


# ---------------------------------------------------------------------------
# FeatureFamilies import
# ---------------------------------------------------------------------------

try:
    from phase_1.feature_families import FeatureFamilies, FamilyPath
except Exception:
    try:
        from feature_families import FeatureFamilies, FamilyPath
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "Could not import FeatureFamilies. Expected phase_1.feature_families "
            "or feature_families to be importable."
        ) from exc


# ---------------------------------------------------------------------------
# Config and result structures
# ---------------------------------------------------------------------------

@dataclass
class RecipeSamplerConfig:
    """
    Configuration for CatBoost recipe sampling.

    n_recipes:
        Target total number of recipes. If coverage recipes exceed this number,
        all coverage recipes are still evaluated.

    min_families / max_families:
        Random recipes choose this many top-level feature families.

    min_features / max_features:
        Random recipes contain this many total features. Coverage recipes are
        chunked using max_features.

    coverage_rounds:
        Number of shuffled coverage passes across all usable screened features.
        Each pass chunks all features, so every feature appears roughly once per
        coverage round with different neighbors.

    test_size:
        Fraction of rows used for validation in each recipe evaluation.

    CatBoost params:
        iterations, depth, learning_rate.
    """
    n_recipes: int = 200
    min_families: int = 1
    max_families: int = 5
    min_features: int = 8
    max_features: int = 30
    coverage_rounds: int = 5
    random_state: Optional[int] = 42
    test_size: float = 0.25

    iterations: int = 300
    depth: int = 3
    learning_rate: float = 0.03

    min_rows: int = 10
    pair_min_count: int = 5
    top_n: int = 50


@dataclass
class RecipeResult:
    recipe_id: int
    recipe_type: str
    features: List[str]
    families: List[str]
    score_r2: Optional[float]
    score_rmse: Optional[float]
    score_mae: Optional[float]
    n_features: int
    n_rows_used: int
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# JSON helpers
# ---------------------------------------------------------------------------

def load_json(path: str | Path) -> dict:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"JSON file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: str | Path, data: dict | list) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _drop_index_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop common CSV index columns created by pandas.to_csv(index=True).
    """
    drop_cols = [
        c for c in df.columns
        if c == "Unnamed: 0" or c.lower() in {"index", "state"}
    ]
    if drop_cols:
        return df.drop(columns=drop_cols)
    return df


def read_x_csv(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = _drop_index_columns(df)
    df = df.apply(pd.to_numeric, errors="coerce")
    return df


def read_y_csv(path: str | Path) -> pd.Series:
    df = pd.read_csv(path)
    df = _drop_index_columns(df)

    if df.shape[1] == 0:
        raise ValueError(f"y CSV has no usable columns: {path}")

    value_col = df.columns[-1]
    return pd.to_numeric(df[value_col], errors="coerce")


def load_feature_metadata(dataset_manifest: dict) -> Dict[str, dict]:
    """
    Load features.json from dataset manifest.
    """
    features_file = dataset_manifest.get("features_file")
    if not features_file:
        return {}

    path = Path(features_file)
    if not path.exists():
        return {}

    records = load_json(path)
    if not isinstance(records, list):
        return {}

    lookup: Dict[str, dict] = {}

    for item in records:
        if not isinstance(item, dict):
            continue
        feat = item.get("feature")
        if not feat:
            continue
        lookup[str(feat)] = item

    return lookup


# ---------------------------------------------------------------------------
# Basic cleaning and metrics
# ---------------------------------------------------------------------------

def clean_xy_for_recipe(
    X: pd.DataFrame,
    y: pd.Series | np.ndarray,
    feature_list: List[str],
) -> Tuple[pd.DataFrame, pd.Series, dict]:
    """
    Subset X to feature_list, align y, drop missing y, fill missing X values,
    and drop constant selected columns.

    Returns:
      X_clean, y_clean, cleaning_info
    """
    if len(feature_list) == 0:
        raise ValueError("Empty recipe feature list.")

    missing_from_X = [c for c in feature_list if c not in X.columns]
    available = [c for c in feature_list if c in X.columns]

    if not available:
        raise ValueError("No recipe features are available in X.")

    X_sub = X[available].copy()

    if isinstance(y, pd.Series):
        y_series = y.copy()
    else:
        y_series = pd.Series(y, index=X.index)

    if len(X_sub) != len(y_series):
        n = min(len(X_sub), len(y_series))
        X_sub = X_sub.iloc[:n].copy()
        y_series = y_series.iloc[:n].copy()

    y_mask = y_series.notna()
    X_sub = X_sub.loc[y_mask].copy()
    y_clean = y_series.loc[y_mask].copy()

    X_sub = X_sub.apply(pd.to_numeric, errors="coerce")
    missing_before = int(X_sub.isna().sum().sum())

    medians = X_sub.median(axis=0, numeric_only=True)
    X_sub = X_sub.fillna(medians)
    X_sub = X_sub.fillna(0.0)

    constant_cols = [
        c for c in X_sub.columns
        if X_sub[c].nunique(dropna=False) <= 1
    ]
    if constant_cols:
        X_sub = X_sub.drop(columns=constant_cols)

    cleaning_info = {
        "requested_features": len(feature_list),
        "missing_from_X": missing_from_X,
        "n_missing_from_X": len(missing_from_X),
        "missing_values_filled": missing_before,
        "constant_columns_dropped": constant_cols,
        "n_constant_columns_dropped": len(constant_cols),
        "n_rows_after_y_drop": int(len(y_clean)),
    }

    if X_sub.shape[1] == 0:
        raise ValueError("No non-constant recipe features remain after cleaning.")

    return X_sub, y_clean, cleaning_info


def regression_metrics(y_true: pd.Series, y_pred: np.ndarray) -> dict:
    y_true_arr = np.asarray(y_true, dtype=float)
    y_pred_arr = np.asarray(y_pred, dtype=float)

    mse = mean_squared_error(y_true_arr, y_pred_arr)
    rmse = math.sqrt(mse)
    mae = mean_absolute_error(y_true_arr, y_pred_arr)

    if len(y_true_arr) >= 2:
        r2 = r2_score(y_true_arr, y_pred_arr)
    else:
        r2 = None

    return {
        "r2": None if r2 is None else round(float(r2), 6),
        "rmse": round(float(rmse), 6),
        "mae": round(float(mae), 6),
        "n": int(len(y_true_arr)),
    }


# ---------------------------------------------------------------------------
# Feature family helpers
# ---------------------------------------------------------------------------

def _make_rng(seed: Optional[int]) -> np.random.Generator:
    if seed is None:
        return np.random.default_rng()
    return np.random.default_rng(seed)


def _feature_top_family_from_metadata(feature: str, feature_metadata: Dict[str, dict]) -> Optional[str]:
    """
    Fallback family assignment from feature metadata if FeatureFamilies does not
    have a path for the feature.
    """
    meta = feature_metadata.get(feature, {})
    group = meta.get("group")
    if group:
        return str(group)
    return None


def _path_for_column(ff: Optional[FeatureFamilies], col: str):
    if ff is None:
        return None
    try:
        return ff.path_for_column(col)
    except Exception:
        return None


def _top_family_for_column(
    col: str,
    ff: Optional[FeatureFamilies],
    feature_metadata: Dict[str, dict],
) -> Optional[str]:
    path = _path_for_column(ff, col)
    if path is not None:
        return path.top_family
    return _feature_top_family_from_metadata(col, feature_metadata)


def _subfamily_for_column(
    col: str,
    ff: Optional[FeatureFamilies],
    feature_metadata: Dict[str, dict],
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    path = _path_for_column(ff, col)
    if path is not None:
        return path.top_family, path.subfamily, getattr(path, "inner_subfamily", None)

    meta = feature_metadata.get(col, {})
    group = meta.get("group")
    label = meta.get("label") or meta.get("raw_label") or ""
    return group, label, None


def _build_family_to_columns(
    X_cols: Sequence[str],
    ff: Optional[FeatureFamilies],
    feature_metadata: Dict[str, dict],
) -> Dict[str, List[str]]:
    """
    Map each top-level family to the list of screened columns in X that belong to it.
    """
    fam_to_cols: Dict[str, List[str]] = {}

    for col in X_cols:
        fam = _top_family_for_column(col, ff, feature_metadata)
        if fam is None:
            continue
        fam_to_cols.setdefault(str(fam), []).append(col)

    return fam_to_cols


def _filter_constant_columns(X: pd.DataFrame, cols: List[str]) -> List[str]:
    """
    Remove columns that are constant or all NaN.
    """
    out: List[str] = []

    for c in cols:
        if c not in X.columns:
            continue
        series = X[c]
        non_null = series.dropna()
        if non_null.nunique() > 1:
            out.append(c)

    return out


# ---------------------------------------------------------------------------
# Recipe construction
# ---------------------------------------------------------------------------

def _build_coverage_recipes(
    fam_to_cols: Dict[str, List[str]],
    cfg: RecipeSamplerConfig,
    rng: np.random.Generator,
) -> List[List[str]]:
    """
    Build coverage recipes so every usable screened feature appears in multiple
    recipes with different neighbors.

    Each coverage round:
      - flattens all columns
      - shuffles
      - chunks by cfg.max_features
    """
    all_cols: List[str] = []

    for cols in fam_to_cols.values():
        all_cols.extend(cols)

    seen: Set[str] = set()
    usable_features: List[str] = []

    for col in all_cols:
        if col in seen:
            continue
        seen.add(col)
        usable_features.append(col)

    if not usable_features:
        return []

    max_size = max(1, int(cfg.max_features))
    coverage_recipes: List[List[str]] = []

    for _ in range(max(1, int(cfg.coverage_rounds))):
        shuffled = usable_features.copy()
        rng.shuffle(shuffled)

        for i in range(0, len(shuffled), max_size):
            chunk = shuffled[i : i + max_size]
            if chunk:
                coverage_recipes.append(chunk)

    return coverage_recipes


def _sample_recipe_features(
    rng: np.random.Generator,
    fam_to_cols: Dict[str, List[str]],
    cfg: RecipeSamplerConfig,
) -> Tuple[List[str], List[str]]:
    """
    Sample one random family-aware recipe.
    """
    if not fam_to_cols:
        raise ValueError("No families with usable columns found for recipe sampling.")

    all_families = list(fam_to_cols.keys())

    max_fam = min(int(cfg.max_families), len(all_families))
    min_fam = min(int(cfg.min_families), max_fam)

    if max_fam <= 0:
        raise ValueError("No usable families available.")

    n_families = int(rng.integers(min_fam, max_fam + 1))
    families = list(rng.choice(all_families, size=n_families, replace=False))

    max_possible_features = sum(len(fam_to_cols[f]) for f in families)
    if max_possible_features <= 0:
        raise ValueError("Chosen families have no columns.")

    min_feat = min(int(cfg.min_features), max_possible_features)
    max_feat = min(int(cfg.max_features), max_possible_features)

    if min_feat > max_feat:
        target_features = max_possible_features
    else:
        target_features = int(rng.integers(min_feat, max_feat + 1))

    features: List[str] = []
    remaining = target_features
    remaining_families = len(families)

    for family in families:
        available_cols = fam_to_cols[family]

        if remaining_families <= 0:
            break

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

    seen: Set[str] = set()
    dedup_features: List[str] = []

    for col in features:
        if col in seen:
            continue
        seen.add(col)
        dedup_features.append(col)

    if not dedup_features:
        any_family = str(rng.choice(all_families))
        dedup_features = [str(rng.choice(fam_to_cols[any_family]))]

    return dedup_features, families


def _families_for_recipe(
    features: List[str],
    ff: Optional[FeatureFamilies],
    feature_metadata: Dict[str, dict],
) -> List[str]:
    seen: Set[str] = set()
    families: List[str] = []

    for col in features:
        fam = _top_family_for_column(col, ff, feature_metadata)
        if fam is None:
            continue
        fam = str(fam)
        if fam in seen:
            continue
        seen.add(fam)
        families.append(fam)

    return families


# ---------------------------------------------------------------------------
# CatBoost evaluation
# ---------------------------------------------------------------------------

def _build_catboost(cfg: RecipeSamplerConfig) -> CatBoostRegressor:
    return CatBoostRegressor(
        iterations=int(cfg.iterations),
        depth=int(cfg.depth),
        learning_rate=float(cfg.learning_rate),
        loss_function="RMSE",
        eval_metric="RMSE",
        random_seed=cfg.random_state if cfg.random_state is not None else 0,
        verbose=False,
        allow_writing_files=False,
    )


def _evaluate_recipe(
    X: pd.DataFrame,
    y: pd.Series | np.ndarray,
    feature_list: List[str],
    cfg: RecipeSamplerConfig,
) -> Tuple[Optional[float], Optional[float], Optional[float], int, Optional[str]]:
    """
    Evaluate one recipe with CatBoost.

    Returns:
      score_r2, score_rmse, score_mae, n_rows_used, error
    """
    try:
        X_clean, y_clean, _ = clean_xy_for_recipe(X, y, feature_list)

        n_rows = len(y_clean)
        if n_rows < int(cfg.min_rows):
            return None, None, None, n_rows, (
                f"Not enough rows after cleaning: {n_rows} < {cfg.min_rows}"
            )

        if n_rows < 6:
            return None, None, None, n_rows, (
                f"Not enough rows for train/test split: {n_rows}"
            )

        X_train, X_val, y_train, y_val = train_test_split(
            X_clean,
            y_clean,
            test_size=float(cfg.test_size),
            random_state=cfg.random_state,
        )

        model = _build_catboost(cfg)
        model.fit(X_train, y_train)

        y_pred = model.predict(X_val)
        metrics = regression_metrics(y_val, y_pred)

        return (
            metrics["r2"],
            metrics["rmse"],
            metrics["mae"],
            int(n_rows),
            None,
        )

    except Exception as exc:
        return None, None, None, 0, str(exc)


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def _score_for_aggregation(recipe: RecipeResult) -> Optional[float]:
    """
    Use R² as the primary recipe score.

    Negative R² is allowed and meaningful, but for pairwise co-elevation we
    may later choose to ignore non-positive recipes to reduce noise.
    """
    return recipe.score_r2


def _recipe_to_dict(recipe: RecipeResult) -> dict:
    return asdict(recipe)


def _build_column_scores(
    recipes: List[RecipeResult],
    feature_metadata: Dict[str, dict],
) -> List[dict]:
    col_stats: Dict[str, Dict[str, float]] = defaultdict(lambda: {"count": 0.0, "score_sum": 0.0})

    for recipe in recipes:
        score = _score_for_aggregation(recipe)
        if score is None:
            continue

        for col in recipe.features:
            col_stats[col]["count"] += 1.0
            col_stats[col]["score_sum"] += float(score)

    rows: List[dict] = []

    for col, agg in col_stats.items():
        count = agg["count"]
        if count <= 0:
            continue

        mean_score = agg["score_sum"] / count
        meta = feature_metadata.get(col, {})

        rows.append(
            {
                "feature": col,
                "mean_score_r2": round(float(mean_score), 6),
                "count": int(count),
                "group": meta.get("group", ""),
                "label": meta.get("label", ""),
                "raw_label": meta.get("raw_label", ""),
                "concept": meta.get("concept", ""),
                "source_reasons": meta.get("source_reasons", []),
                "years_seen": meta.get("years_seen", []),
            }
        )

    rows.sort(
        key=lambda r: (r["mean_score_r2"], r["count"]),
        reverse=True,
    )

    return rows


def _build_family_scores(
    column_scores: List[dict],
    ff: Optional[FeatureFamilies],
    feature_metadata: Dict[str, dict],
) -> List[dict]:
    fam_agg: Dict[Tuple[str, str], Dict[str, float]] = defaultdict(
        lambda: {"score_sum": 0.0, "count": 0.0}
    )

    for row in column_scores:
        feature = row["feature"]
        mean_score = float(row["mean_score_r2"])

        top_family, subfamily, _inner = _subfamily_for_column(feature, ff, feature_metadata)

        if top_family is None:
            continue

        key = (str(top_family), str(subfamily or ""))

        fam_agg[key]["score_sum"] += mean_score
        fam_agg[key]["count"] += 1.0

    rows: List[dict] = []

    for (top_family, subfamily), agg in fam_agg.items():
        count = agg["count"]
        if count <= 0:
            continue

        rows.append(
            {
                "top_family": top_family,
                "subfamily": subfamily,
                "mean_score_r2": round(float(agg["score_sum"] / count), 6),
                "feature_count": int(count),
            }
        )

    rows.sort(
        key=lambda r: (r["mean_score_r2"], r["feature_count"]),
        reverse=True,
    )

    return rows


def _build_pairwise_scores(
    recipes: List[RecipeResult],
    column_scores: List[dict],
    cfg: RecipeSamplerConfig,
    feature_metadata: Dict[str, dict],
) -> List[dict]:
    """
    Pairwise co-elevation / lift.

    pair_mean:
      Mean recipe R² for recipes where both features appear.

    baseline:
      Mean of each individual feature's aggregated score.

    lift:
      pair_mean - baseline

    Interpretation:
      Positive lift means the pair appears in recipes that tend to score better
      than expected from the two features' individual averages.
    """
    feature_baseline = {
        row["feature"]: float(row["mean_score_r2"])
        for row in column_scores
    }

    pair_stats: Dict[Tuple[str, str], Dict[str, float]] = defaultdict(
        lambda: {"count": 0.0, "score_sum": 0.0}
    )

    for recipe in recipes:
        score = _score_for_aggregation(recipe)
        if score is None:
            continue

        # Optional noise guard: pairwise lift is more interpretable when based
        # on recipes that are at least doing better than a zero-R² model.
        if score <= 0:
            continue

        for a, b in combinations(sorted(set(recipe.features)), 2):
            key = (a, b)
            pair_stats[key]["count"] += 1.0
            pair_stats[key]["score_sum"] += float(score)

    rows: List[dict] = []

    for (a, b), agg in pair_stats.items():
        count = int(agg["count"])
        if count < int(cfg.pair_min_count):
            continue

        pair_mean = agg["score_sum"] / count
        baseline = (feature_baseline.get(a, 0.0) + feature_baseline.get(b, 0.0)) / 2.0
        lift = pair_mean - baseline

        meta_a = feature_metadata.get(a, {})
        meta_b = feature_metadata.get(b, {})

        rows.append(
            {
                "feature_a": a,
                "feature_b": b,
                "mean_score_r2": round(float(pair_mean), 6),
                "baseline_score_r2": round(float(baseline), 6),
                "lift": round(float(lift), 6),
                "co_occurrence_count": count,
                "feature_a_label": meta_a.get("label", ""),
                "feature_b_label": meta_b.get("label", ""),
                "feature_a_group": meta_a.get("group", ""),
                "feature_b_group": meta_b.get("group", ""),
            }
        )

    rows.sort(
        key=lambda r: (r["lift"], r["co_occurrence_count"]),
        reverse=True,
    )

    return rows


# ---------------------------------------------------------------------------
# Core sampler
# ---------------------------------------------------------------------------

def run_catboost_recipe_sampling(
    X: pd.DataFrame,
    y: pd.Series | np.ndarray,
    ff: Optional[FeatureFamilies],
    feature_metadata: Dict[str, dict],
    cfg: Optional[RecipeSamplerConfig] = None,
) -> Dict[str, Any]:
    """
    Main in-memory recipe sampler.

    Inputs:
      X:
        Screened feature universe for one year or pooled dataset.

      y:
        Target vector.

      ff:
        FeatureFamilies instance. May be None if metadata group fallback is acceptable.

      feature_metadata:
        Lookup loaded from features.json.

      cfg:
        RecipeSamplerConfig.

    Returns JSON-ready dict.
    """
    if cfg is None:
        cfg = RecipeSamplerConfig()

    rng = _make_rng(cfg.random_state)

    # 1. Map screened X columns to families.
    fam_to_cols_raw = _build_family_to_columns(
        X_cols=list(X.columns),
        ff=ff,
        feature_metadata=feature_metadata,
    )

    fam_to_cols: Dict[str, List[str]] = {}

    for fam, cols in fam_to_cols_raw.items():
        good_cols = _filter_constant_columns(X, cols)
        if good_cols:
            fam_to_cols[fam] = good_cols

    if not fam_to_cols:
        raise ValueError("No non-constant screened columns with family assignments were found.")

    # 2. Build coverage recipes.
    coverage_recipes = _build_coverage_recipes(fam_to_cols, cfg, rng)
    n_coverage = len(coverage_recipes)

    target_total = max(int(cfg.n_recipes), n_coverage)
    remaining_random = max(0, target_total - n_coverage)

    recipes: List[RecipeResult] = []
    recipe_id = 0

    # 3. Evaluate coverage recipes.
    for feat_list in coverage_recipes:
        families = _families_for_recipe(feat_list, ff, feature_metadata)

        score_r2, score_rmse, score_mae, n_rows, error = _evaluate_recipe(
            X=X,
            y=y,
            feature_list=feat_list,
            cfg=cfg,
        )

        recipes.append(
            RecipeResult(
                recipe_id=recipe_id,
                recipe_type="coverage",
                features=feat_list,
                families=families,
                score_r2=score_r2,
                score_rmse=score_rmse,
                score_mae=score_mae,
                n_features=len(feat_list),
                n_rows_used=n_rows,
                error=error,
            )
        )
        recipe_id += 1

        if recipe_id % 10 == 0:
            print(f"  Evaluated {recipe_id} recipes...")

    # 4. Evaluate random recipes.
    for _ in range(remaining_random):
        try:
            feat_list, fam_list = _sample_recipe_features(rng, fam_to_cols, cfg)
        except Exception as exc:
            recipes.append(
                RecipeResult(
                    recipe_id=recipe_id,
                    recipe_type="random",
                    features=[],
                    families=[],
                    score_r2=None,
                    score_rmse=None,
                    score_mae=None,
                    n_features=0,
                    n_rows_used=0,
                    error=str(exc),
                )
            )
            recipe_id += 1
            continue

        score_r2, score_rmse, score_mae, n_rows, error = _evaluate_recipe(
            X=X,
            y=y,
            feature_list=feat_list,
            cfg=cfg,
        )

        recipes.append(
            RecipeResult(
                recipe_id=recipe_id,
                recipe_type="random",
                features=feat_list,
                families=fam_list,
                score_r2=score_r2,
                score_rmse=score_rmse,
                score_mae=score_mae,
                n_features=len(feat_list),
                n_rows_used=n_rows,
                error=error,
            )
        )
        recipe_id += 1

        if recipe_id % 10 == 0:
            print(f"  Evaluated {recipe_id} recipes...")

    # 5. Aggregate.
    valid_recipes = [r for r in recipes if r.error is None and r.score_r2 is not None]
    errored_recipes = [r for r in recipes if r.error is not None]

    column_scores = _build_column_scores(valid_recipes, feature_metadata)
    family_scores = _build_family_scores(column_scores, ff, feature_metadata)
    pairwise_scores = _build_pairwise_scores(valid_recipes, column_scores, cfg, feature_metadata)

    recipe_dicts = [_recipe_to_dict(r) for r in recipes]

    # Primary ranked lists for Godot/reporting.
    top_columns = column_scores[: int(cfg.top_n)]
    top_families = family_scores[: int(cfg.top_n)]
    top_pairs = pairwise_scores[: int(cfg.top_n)]
    top_recipes = sorted(
        [r for r in recipe_dicts if r.get("score_r2") is not None],
        key=lambda r: r["score_r2"],
        reverse=True,
    )[: int(cfg.top_n)]

    scores = [float(r.score_r2) for r in valid_recipes if r.score_r2 is not None]

    summary = {
        "screened_feature_count": int(X.shape[1]),
        "usable_family_count": int(len(fam_to_cols)),
        "coverage_recipes": int(n_coverage),
        "random_recipes": int(remaining_random),
        "recipes_evaluated": int(len(recipes)),
        "valid_recipes": int(len(valid_recipes)),
        "errored_recipes": int(len(errored_recipes)),
        "score_r2": {
            "mean": round(float(np.mean(scores)), 6) if scores else None,
            "median": round(float(np.median(scores)), 6) if scores else None,
            "max": round(float(np.max(scores)), 6) if scores else None,
            "min": round(float(np.min(scores)), 6) if scores else None,
        },
    }

    return {
        "sampler": "phase_2.catboost_recipe_sampler",
        "model_family": "catboost",
        "artifact_type": "catboost_recipe_sampling_result",
        "config": asdict(cfg),
        "summary": summary,
        "top_columns": top_columns,
        "top_families": top_families,
        "top_pairs": top_pairs,
        "top_recipes": top_recipes,
        "column_scores": column_scores,
        "family_scores": family_scores,
        "pairwise_scores": pairwise_scores,
        "recipes": recipe_dicts,
    }


# ---------------------------------------------------------------------------
# FeatureFamilies loading
# ---------------------------------------------------------------------------

def try_load_feature_families(path: Optional[str]) -> Optional[FeatureFamilies]:
    """
    Load FeatureFamilies if a JSON path is provided.

    This function intentionally keeps a fallback behavior because the screened
    feature pool metadata can still group by DP table if FeatureFamilies JSON
    is not supplied.
    """
    if not path:
        return None

    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"FeatureFamilies JSON not found: {p}")

    # The exact constructor can vary by your local class implementation.
    # Try common patterns.
    if hasattr(FeatureFamilies, "from_json"):
        return FeatureFamilies.from_json(p)

    data = load_json(p)
    return FeatureFamilies(data)


# ---------------------------------------------------------------------------
# Manifest runners
# ---------------------------------------------------------------------------

def run_for_year(
    *,
    year: int,
    dataset_manifest: dict,
    ff: Optional[FeatureFamilies],
    feature_metadata: Dict[str, dict],
    cfg: RecipeSamplerConfig,
) -> dict:
    year_key = str(year)
    files = dataset_manifest["files"][year_key]

    X = read_x_csv(files["X"])
    y = read_y_csv(files["y"])

    print()
    print(f"=== CatBoost recipe sampling: ACS year {year} ===")
    print(f"  Rows: {len(y)}  Screened features: {X.shape[1]}")

    result = run_catboost_recipe_sampling(
        X=X,
        y=y,
        ff=ff,
        feature_metadata=feature_metadata,
        cfg=cfg,
    )

    result["year"] = int(year)
    result["dataset"] = {
        "n_rows": int(len(y)),
        "n_screened_features": int(X.shape[1]),
        "X_file": files["X"],
        "y_file": files["y"],
    }

    return result


def build_pooled_dataset(dataset_manifest: dict) -> Tuple[pd.DataFrame, pd.Series, dict]:
    """
    Build pooled cross-year dataset using the intersection of available screened
    columns across years.

    Adds __acs_year as a numeric feature.
    """
    yearly_X: Dict[str, pd.DataFrame] = {}
    yearly_y: Dict[str, pd.Series] = {}

    common_cols: Optional[Set[str]] = None

    for year in dataset_manifest.get("years_built", []):
        year_key = str(year)
        files = dataset_manifest["files"][year_key]

        X = read_x_csv(files["X"])
        y = read_y_csv(files["y"])

        # Basic row alignment only here; recipe-level cleaning happens later.
        if len(X) != len(y):
            n = min(len(X), len(y))
            X = X.iloc[:n].copy()
            y = y.iloc[:n].copy()

        yearly_X[year_key] = X
        yearly_y[year_key] = y

        cols = set(X.columns)
        common_cols = cols if common_cols is None else common_cols & cols

    if not common_cols:
        raise ValueError("No common screened features are available across built years.")

    common_cols_list = sorted(common_cols)

    pooled_X_parts: List[pd.DataFrame] = []
    pooled_y_parts: List[pd.Series] = []

    for year_key, X in yearly_X.items():
        y = yearly_y[year_key]

        X_part = X[common_cols_list].copy()
        X_part["__acs_year"] = int(year_key)

        pooled_X_parts.append(X_part)
        pooled_y_parts.append(y)

    pooled_X = pd.concat(pooled_X_parts, axis=0, ignore_index=True)
    pooled_y = pd.concat(pooled_y_parts, axis=0, ignore_index=True)

    info = {
        "years": sorted(int(y) for y in yearly_X.keys()),
        "n_years": len(yearly_X),
        "n_common_screened_features_before_year_column": len(common_cols_list),
        "uses_year_feature": True,
        "common_screened_features": common_cols_list,
    }

    return pooled_X, pooled_y, info


def run_pooled(
    *,
    dataset_manifest: dict,
    ff: Optional[FeatureFamilies],
    feature_metadata: Dict[str, dict],
    cfg: RecipeSamplerConfig,
) -> dict:
    X, y, pooled_info = build_pooled_dataset(dataset_manifest)

    # Give the synthetic year feature minimal metadata.
    feature_metadata = dict(feature_metadata)
    feature_metadata["__acs_year"] = {
        "feature": "__acs_year",
        "group": "MODEL_CONTEXT",
        "label": "ACS year",
        "raw_label": "ACS year",
        "concept": "Pooled cross-year context feature",
        "source_reasons": ["pooled_year_context"],
        "years_seen": pooled_info["years"],
    }

    print()
    print("=== CatBoost recipe sampling: pooled cross-year dataset ===")
    print(f"  Rows: {len(y)}  Common screened features + year: {X.shape[1]}")

    result = run_catboost_recipe_sampling(
        X=X,
        y=y,
        ff=ff,
        feature_metadata=feature_metadata,
        cfg=cfg,
    )

    result["dataset"] = {
        "n_rows": int(len(y)),
        "n_screened_features": int(X.shape[1]),
        "pooled_info": pooled_info,
    }

    return result


def build_sampling_report(
    *,
    dataset_manifest: dict,
    mode: str,
    cfg: RecipeSamplerConfig,
    feature_families_path: Optional[str] = None,
) -> dict:
    """
    Run CatBoost recipe sampling from a dataset manifest.
    """
    ff = try_load_feature_families(feature_families_path)
    feature_metadata = load_feature_metadata(dataset_manifest)

    report: dict = {
        "status": "ok",
        "runner": "phase_2.catboost_recipe_sampler",
        "artifact_type": "catboost_recipe_sampling_report",
        "research_framing": {
            "phase_1": "correlation_screening_defines_candidate_feature_pool",
            "phase_2": "catboost_recipe_sampling_over_screened_pool",
            "note": (
                "The screened feature pool is not trained as one oversized model. "
                "Instead, CatBoost evaluates many bounded feature recipes sampled "
                "from the screened pool, and recipe performance is aggregated back "
                "to columns, families, and feature pairs."
            ),
        },
        "dataset_manifest": {
            "output_dir": dataset_manifest.get("output_dir", ""),
            "dataset_type": dataset_manifest.get("dataset_type", ""),
            "pool_name": dataset_manifest.get("pool_name", ""),
            "n_years_built": dataset_manifest.get("n_years_built", 0),
            "years_built": dataset_manifest.get("years_built", []),
            "n_features_in_pool": dataset_manifest.get("n_features_in_pool", None),
        },
        "settings": {
            "mode": mode,
            "feature_families_path": feature_families_path,
            **asdict(cfg),
        },
        "results": {},
        "warnings": [],
    }

    if ff is None:
        report["warnings"].append(
            {
                "type": "feature_families_not_loaded",
                "message": (
                    "FeatureFamilies JSON was not supplied. Sampler will group "
                    "features using metadata fallback such as ACS DP group."
                ),
            }
        )

    years_built = [int(y) for y in dataset_manifest.get("years_built", [])]

    if mode in {"per_year", "both"}:
        per_year: Dict[str, dict] = {}

        for year in years_built:
            try:
                per_year[str(year)] = run_for_year(
                    year=year,
                    dataset_manifest=dataset_manifest,
                    ff=ff,
                    feature_metadata=feature_metadata,
                    cfg=cfg,
                )
            except Exception as exc:
                per_year[str(year)] = {
                    "status": "error",
                    "year": year,
                    "error": str(exc),
                }
                report["warnings"].append(
                    {
                        "type": "year_sampling_failed",
                        "year": year,
                        "message": str(exc),
                    }
                )

        report["results"]["per_year"] = per_year

    if mode in {"pooled", "both"}:
        try:
            report["results"]["pooled"] = run_pooled(
                dataset_manifest=dataset_manifest,
                ff=ff,
                feature_metadata=feature_metadata,
                cfg=cfg,
            )
        except Exception as exc:
            report["results"]["pooled"] = {
                "status": "error",
                "error": str(exc),
            }
            report["warnings"].append(
                {
                    "type": "pooled_sampling_failed",
                    "message": str(exc),
                }
            )

    return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> dict:
    parser = argparse.ArgumentParser(
        description="Run CatBoost recipe sampling over screened Phase 2 datasets."
    )

    parser.add_argument(
        "--dataset-manifest",
        required=True,
        help="Path to manifest.json from phase_2/ml_dataset_builder.py.",
    )

    parser.add_argument(
        "--output",
        default=None,
        help=(
            "Output path for sampling report JSON. Defaults to "
            "<dataset_dir>/catboost_recipe_sampling_report.json."
        ),
    )

    parser.add_argument(
        "--feature-families",
        default=None,
        help=(
            "Optional path to feature_families.json. If omitted, grouping falls "
            "back to feature metadata, usually ACS DP group."
        ),
    )

    parser.add_argument(
        "--mode",
        default="per_year",
        choices=["per_year", "pooled", "both"],
        help="Run per-year sampling, pooled sampling, or both.",
    )

    parser.add_argument("--n-recipes", type=int, default=200)
    parser.add_argument("--min-families", type=int, default=1)
    parser.add_argument("--max-families", type=int, default=5)
    parser.add_argument("--min-features", type=int, default=8)
    parser.add_argument("--max-features", type=int, default=30)
    parser.add_argument("--coverage-rounds", type=int, default=5)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--test-size", type=float, default=0.25)
    parser.add_argument("--iterations", type=int, default=300)
    parser.add_argument("--depth", type=int, default=3)
    parser.add_argument("--learning-rate", type=float, default=0.03)
    parser.add_argument("--min-rows", type=int, default=10)
    parser.add_argument("--pair-min-count", type=int, default=5)
    parser.add_argument("--top-n", type=int, default=50)

    args = parser.parse_args()

    dataset_manifest_path = Path(args.dataset_manifest)
    dataset_manifest = load_json(dataset_manifest_path)

    cfg = RecipeSamplerConfig(
        n_recipes=args.n_recipes,
        min_families=args.min_families,
        max_families=args.max_families,
        min_features=args.min_features,
        max_features=args.max_features,
        coverage_rounds=args.coverage_rounds,
        random_state=args.random_state,
        test_size=args.test_size,
        iterations=args.iterations,
        depth=args.depth,
        learning_rate=args.learning_rate,
        min_rows=args.min_rows,
        pair_min_count=args.pair_min_count,
        top_n=args.top_n,
    )

    report = build_sampling_report(
        dataset_manifest=dataset_manifest,
        mode=args.mode,
        cfg=cfg,
        feature_families_path=args.feature_families,
    )

    output_path = (
        Path(args.output)
        if args.output
        else dataset_manifest_path.parent / "catboost_recipe_sampling_report.json"
    )

    write_json(output_path, report)

    print()
    print(f"CatBoost recipe sampling report written to: {output_path}")

    return report


if __name__ == "__main__":
    main()
