#!/usr/bin/env python3
# phase_2/ml_dataset_builder.py
#
# Builds ML-ready screened feature-universe datasets from:
#   1. a Phase 1 screening report
#   2. a Phase 2 screened feature pool
#
# IMPORTANT:
#   This file does NOT build one final model recipe.
#   It rebuilds the same ACS + target X/y path used by Phase 1, then subsets X
#   to the screened feature pool. The resulting X files are the feature universe
#   that the Phase 2 CatBoost recipe sampler will draw small recipes from.
#
# Correct pipeline:
#   phase_1/dry_run_iterative_screening.py
#       -> results/<screening_report>.json
#   phase_2/screened_feature_pool_builder.py
#       -> results/phase_2/<screened_pool>.json
#   phase_2/ml_dataset_builder.py
#       -> results/phase_2/<dataset_dir>/manifest.json + yearly X/y CSVs
#   phase_2/catboost_recipe_sampler.py
#       -> samples small bounded recipes from the screened X files
#
# Example:
#   python phase_2/ml_dataset_builder.py \
#       --screening-report results/cancer_all_invasive_cancer_sites_combined_all.json \
#       --screened-feature-pool results/phase_2/cancer_all_screened_pool.json \
#       --output-dir results/phase_2/cancer_all_screened_dataset

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Project root path setup
# ---------------------------------------------------------------------------

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ---------------------------------------------------------------------------
# Imports from Phase 1
# ---------------------------------------------------------------------------

from dry_run_iterative_screening import build_loader
from data_server.acs_loader import get_all_metadata
from phase_1.Dataset_Builder import PhaseB_1


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
# Screening report helpers
# ---------------------------------------------------------------------------

def is_year_key(key: str) -> bool:
    return str(key).isdigit()


def get_successful_years(screening_report: dict) -> List[int]:
    years: List[int] = []

    for key, value in screening_report.items():
        if not is_year_key(key):
            continue
        if not isinstance(value, dict):
            continue
        if "error" in value:
            continue
        if "config" not in value:
            continue
        if "rounds" not in value:
            continue
        years.append(int(key))

    return sorted(years)


def get_errored_years(screening_report: dict) -> Dict[str, str]:
    out: Dict[str, str] = {}

    for key, value in screening_report.items():
        if not is_year_key(key):
            continue
        if isinstance(value, dict) and "error" in value:
            out[str(key)] = str(value["error"])

    return out


def parse_years_arg(years_arg: Optional[str]) -> Optional[List[int]]:
    if not years_arg:
        return None

    years: List[int] = []
    for part in years_arg.split(","):
        part = part.strip()
        if not part:
            continue
        years.append(int(part))

    return sorted(set(years))


def filter_years(available_years: List[int], requested_years: Optional[List[int]]) -> List[int]:
    if requested_years is None:
        return available_years

    available_set = set(available_years)
    requested_set = set(requested_years)

    missing = sorted(requested_set - available_set)
    if missing:
        raise ValueError(
            f"Requested years are not available as successful screening years: {missing}. "
            f"Available successful years: {available_years}"
        )

    return sorted(available_set & requested_set)


# ---------------------------------------------------------------------------
# Screened feature pool helpers
# ---------------------------------------------------------------------------

def validate_screened_feature_pool(pool: dict) -> None:
    artifact_type = pool.get("artifact_type")
    if artifact_type and artifact_type != "screened_feature_pool":
        raise ValueError(
            f"Expected artifact_type='screened_feature_pool', got {artifact_type!r}."
        )

    if "feature_codes" not in pool and "features" not in pool:
        raise ValueError(
            "Screened feature pool must contain either 'feature_codes' or 'features'."
        )


def get_feature_codes(screened_feature_pool: dict) -> List[str]:
    validate_screened_feature_pool(screened_feature_pool)

    if "feature_codes" in screened_feature_pool:
        codes = [str(c) for c in screened_feature_pool["feature_codes"]]
    else:
        codes = []
        for item in screened_feature_pool.get("features", []):
            if isinstance(item, dict) and item.get("feature"):
                codes.append(str(item["feature"]))

    # Deduplicate while preserving order.
    seen = set()
    out: List[str] = []
    for code in codes:
        if code in seen:
            continue
        seen.add(code)
        out.append(code)

    return out


def get_feature_records(screened_feature_pool: dict) -> List[dict]:
    validate_screened_feature_pool(screened_feature_pool)

    if "features" in screened_feature_pool and isinstance(screened_feature_pool["features"], list):
        return screened_feature_pool["features"]

    return [{"feature": code} for code in get_feature_codes(screened_feature_pool)]


# ---------------------------------------------------------------------------
# Phase 1 config extraction
# ---------------------------------------------------------------------------

def resolve_loader_config(year_data: dict) -> dict:
    config = year_data.get("config", {})

    loader_name = config.get("loader")
    if not loader_name:
        raise ValueError("Year config is missing required field: loader")

    metric = config.get("metric", "")
    if not metric:
        # Backward-compatible fallbacks for old reports.
        if loader_name == "cancer":
            metric = "count"
        elif loader_name == "opioid":
            metric = "age_adjusted_rate"
        elif loader_name == "ufo":
            metric = "raw_count"

    return {
        "loader": loader_name,
        "loader_class": config.get("loader_class", ""),
        "target_col": config.get("target_col", ""),
        "metric": metric,
        "window": int(config.get("window", 1)),
        "cancer_site": config.get("cancer_site", "All Invasive Cancer Sites Combined"),
        "acs_year": int(config.get("acs_year", 0)),
        "threshold_mode": config.get("threshold_mode", ""),
        "computed_min_abs_r": config.get("computed_min_abs_r", None),
        "min_abs_r": config.get("min_abs_r", None),
        "convergence_r": config.get("convergence_r", None),
        "round_cap": config.get("round_cap", None),
        "stale_rounds": config.get("stale_rounds", None),
        "top_n": config.get("top_n", None),
    }


# ---------------------------------------------------------------------------
# Dataset reconstruction
# ---------------------------------------------------------------------------

def rebuild_year_dataset(
    *,
    year: int,
    year_data: dict,
    feature_codes: List[str],
    strict_features: bool = False,
) -> Tuple[Any, Any, dict]:
    """
    Rebuild the same ACS X/y path used by Phase 1 screening for one year.
    Then subset X to the screened feature pool.
    """
    cfg = resolve_loader_config(year_data)

    loader_name = cfg["loader"]
    metric = cfg["metric"]
    window = cfg["window"]
    cancer_site = cfg["cancer_site"]

    # Critical reproducibility point:
    # use the same Phase 1 loader factory used by screening.
    loader, target_col, resolved_acs_year = build_loader(
        loader_name=loader_name,
        cancer_site=cancer_site,
        metric=metric,
        window=window,
        acs_year=year,
    )

    metadata = get_all_metadata(year)
    target_df = loader.load(acs_year=year)

    ds = PhaseB_1(
        level="state",
        year=year,
        metadata=metadata,
        user_target_df=target_df,
        exclude_states_fips=[],
    )

    X = ds["X"].copy()
    y = ds["y"].copy()

    available_features = [c for c in feature_codes if c in X.columns]
    missing_features = [c for c in feature_codes if c not in X.columns]

    if strict_features and missing_features:
        raise ValueError(
            f"Year {year}: {len(missing_features)} screened pool features "
            f"are missing from rebuilt X. Missing examples: {missing_features[:20]}"
        )

    if not available_features:
        raise ValueError(
            f"Year {year}: no screened pool features were available in rebuilt X."
        )

    X_screened = X[available_features].copy()

    year_manifest = {
        "year": year,
        "status": "ok",
        "dataset_role": "screened_feature_universe_for_recipe_sampling",
        "rebuild_source": "phase_1_config",
        "loader": loader_name,
        "loader_class": loader.__class__.__name__,
        "metric": metric,
        "window": window,
        "target_col_from_config": cfg.get("target_col", ""),
        "target_col_rebuilt": target_col,
        "resolved_acs_year": resolved_acs_year,
        "n_rows": int(X_screened.shape[0]),
        "n_features_in_pool": int(len(feature_codes)),
        "n_features_available": int(len(available_features)),
        "n_features_missing": int(len(missing_features)),
        "available_features": available_features,
        "missing_features": missing_features,
        "phase_1_thresholds": {
            "threshold_mode": cfg.get("threshold_mode", ""),
            "computed_min_abs_r": cfg.get("computed_min_abs_r", None),
            "min_abs_r": cfg.get("min_abs_r", None),
            "convergence_r": cfg.get("convergence_r", None),
        },
        "phase_1_run_settings": {
            "round_cap": cfg.get("round_cap", None),
            "stale_rounds": cfg.get("stale_rounds", None),
            "top_n": cfg.get("top_n", None),
        },
    }

    if loader_name == "cancer":
        year_manifest["cancer_site"] = cancer_site

    if hasattr(loader, "last_load_info"):
        year_manifest["loader_last_load_info"] = loader.last_load_info

    return X_screened, y, year_manifest


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build_screened_ml_datasets(
    *,
    screening_report: dict,
    screened_feature_pool: dict,
    output_dir: str | Path,
    years: Optional[List[int]] = None,
    strict_features: bool = False,
    write_csv: bool = True,
) -> dict:
    """
    Build screened feature-universe datasets for each selected year.
    """
    validate_screened_feature_pool(screened_feature_pool)

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    feature_codes = get_feature_codes(screened_feature_pool)
    feature_records = get_feature_records(screened_feature_pool)

    if not feature_codes:
        raise ValueError("Screened feature pool contains zero feature codes.")

    successful_years = get_successful_years(screening_report)
    selected_years = filter_years(successful_years, years)
    errored_years = get_errored_years(screening_report)

    if not selected_years:
        raise ValueError("No successful years selected for dataset building.")

    # Persist the exact screened pool used for reproducibility.
    pool_copy_path = output_dir / "screened_feature_pool.json"
    write_json(pool_copy_path, screened_feature_pool)

    # Persist rich feature metadata in a simple file the sampler can use.
    features_path = output_dir / "features.json"
    write_json(features_path, feature_records)

    manifest: dict = {
        "status": "ok",
        "builder": "phase_2.ml_dataset_builder",
        "dataset_type": "screened_feature_universe_for_recipe_sampling",
        "description": (
            "Each yearly X file contains only the screened feature pool columns "
            "available for that ACS year. These columns are the candidate universe "
            "for the Phase 2 recipe sampler, not a single final model recipe."
        ),
        "output_dir": str(output_dir),
        "screened_feature_pool_file": str(pool_copy_path),
        "features_file": str(features_path),
        "pool_name": screened_feature_pool.get("pool_name", ""),
        "pool_selection_rules": screened_feature_pool.get("selection_rules", {}),
        "pool_summary": screened_feature_pool.get("summary", {}),
        "strict_features": strict_features,
        "years_requested": years,
        "years_built": [],
        "phase_1_errored_years": errored_years,
        "n_features_in_pool": len(feature_codes),
        "feature_codes": feature_codes,
        "files": {},
        "year_manifests": {},
        "warnings": [],
    }

    if errored_years:
        manifest["warnings"].append(
            {
                "type": "phase_1_errored_years_present",
                "message": "Some Phase 1 years failed and cannot be rebuilt for Phase 2.",
                "errored_years": errored_years,
            }
        )

    for year in selected_years:
        year_key = str(year)
        year_data = screening_report[year_key]

        try:
            X_screened, y, year_manifest = rebuild_year_dataset(
                year=year,
                year_data=year_data,
                feature_codes=feature_codes,
                strict_features=strict_features,
            )

            x_path = output_dir / f"{year}_X.csv"
            y_path = output_dir / f"{year}_y.csv"

            if write_csv:
                X_screened.to_csv(x_path, index=True)
                y.to_csv(y_path, index=True)

            manifest["years_built"].append(year)
            manifest["files"][year_key] = {
                "X": str(x_path),
                "y": str(y_path),
            }
            manifest["year_manifests"][year_key] = year_manifest

            if year_manifest["n_features_missing"] > 0:
                manifest["warnings"].append(
                    {
                        "year": year,
                        "type": "missing_pool_features",
                        "message": (
                            f"{year_manifest['n_features_missing']} screened pool features "
                            f"were missing from rebuilt X for year {year}."
                        ),
                        "missing_features": year_manifest["missing_features"],
                    }
                )

            print(
                f"[OK] {year}: "
                f"rows={year_manifest['n_rows']} "
                f"available_pool_features={year_manifest['n_features_available']}/"
                f"{year_manifest['n_features_in_pool']} "
                f"missing={year_manifest['n_features_missing']}"
            )

        except Exception as exc:
            manifest["warnings"].append(
                {
                    "year": year,
                    "type": "build_failed",
                    "message": str(exc),
                }
            )
            manifest["year_manifests"][year_key] = {
                "year": year,
                "status": "error",
                "error": str(exc),
            }

            if strict_features:
                raise

            print(f"[WARN] {year}: failed to build screened dataset: {exc}")

    if not manifest["years_built"]:
        manifest["status"] = "error"
        manifest_path = output_dir / "manifest.json"
        write_json(manifest_path, manifest)
        raise RuntimeError(
            "No yearly screened datasets were built successfully. "
            f"Manifest written to {manifest_path}"
        )

    manifest["n_years_built"] = len(manifest["years_built"])

    # Cross-year feature availability summary.
    availability: Dict[str, dict] = {}
    for feature in feature_codes:
        years_available: List[int] = []
        years_missing: List[int] = []

        for year in manifest["years_built"]:
            year_key = str(year)
            ym = manifest["year_manifests"].get(year_key, {})
            available = set(ym.get("available_features", []))
            missing = set(ym.get("missing_features", []))

            if feature in available:
                years_available.append(int(year))
            elif feature in missing:
                years_missing.append(int(year))

        availability[feature] = {
            "years_available": years_available,
            "years_missing": years_missing,
            "n_years_available": len(years_available),
            "n_years_missing": len(years_missing),
        }

    manifest["feature_availability"] = availability

    manifest_path = output_dir / "manifest.json"
    write_json(manifest_path, manifest)

    print()
    print(f"Screened dataset manifest written to: {manifest_path}")

    return manifest


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> dict:
    parser = argparse.ArgumentParser(
        description=(
            "Build Phase 2 screened feature-universe datasets from a Phase 1 "
            "screening report and screened feature pool."
        )
    )

    parser.add_argument(
        "--screening-report",
        required=True,
        help="Path to Phase 1 screening report JSON.",
    )

    parser.add_argument(
        "--screened-feature-pool",
        required=True,
        help="Path to screened_feature_pool.json from screened_feature_pool_builder.py.",
    )

    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory to write screened ML datasets.",
    )

    parser.add_argument(
        "--years",
        default=None,
        help="Optional comma-separated subset of years, e.g. 2014,2015,2016.",
    )

    parser.add_argument(
        "--strict-features",
        action="store_true",
        help="Fail if any screened pool feature is missing from a rebuilt year dataset.",
    )

    parser.add_argument(
        "--no-csv",
        action="store_true",
        help="Build manifest only; do not write X/y CSV files.",
    )

    args = parser.parse_args()

    screening_report = load_json(args.screening_report)
    screened_feature_pool = load_json(args.screened_feature_pool)
    years = parse_years_arg(args.years)

    manifest = build_screened_ml_datasets(
        screening_report=screening_report,
        screened_feature_pool=screened_feature_pool,
        output_dir=args.output_dir,
        years=years,
        strict_features=args.strict_features,
        write_csv=not args.no_csv,
    )

    return manifest


if __name__ == "__main__":
    main()
