#!/usr/bin/env python3
# phase_2/screened_feature_pool_builder.py
#
# Builds a screened candidate feature pool from a Phase 1 screening report.
#
# IMPORTANT:
#   This does NOT build one final ML feature recipe.
#   This builds the set of features that are allowed to enter the Phase 2
#   recipe sampler.
#
# Correct pipeline:
#
#   Phase 1 screening report
#       -> screened_feature_pool_builder.py
#       -> screened_feature_pool.json
#       -> ml_dataset_builder.py
#       -> catboost_recipe_sampler.py
#
# Conceptual role:
#
#   Phase 1 answers:
#       "Which ACS features are statistically plausible enough to keep?"
#
#   This file answers:
#       "Which screened features are allowed into recipe sampling?"
#
#   The later sampler answers:
#       "Which small combinations of those screened features perform well?"
#
# Example:
#
#   python phase_2/screened_feature_pool_builder.py \
#       --input results/cancer_all_invasive_cancer_sites_combined_all.json \
#       --output results/phase_2/cancer_all_screened_pool.json \
#       --last-round-top-n 20
#
# Optional:
#
#   --no-stable-fdr
#   --no-stable-fallback
#   --last-round-top-n 10
#   --min-years-seen 2

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple


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


def get_successful_year_keys(screening_report: dict) -> List[str]:
    """
    Return year keys that completed successfully in a Phase 1 screening report.
    """
    years: List[str] = []

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

        years.append(str(key))

    return sorted(years, key=lambda x: int(x))


def get_errored_years(screening_report: dict) -> Dict[str, str]:
    """
    Return {year: error_message} for failed Phase 1 years.
    """
    out: Dict[str, str] = {}

    for key, value in screening_report.items():
        if not is_year_key(key):
            continue
        if isinstance(value, dict) and "error" in value:
            out[str(key)] = str(value["error"])

    return out


def get_last_nonempty_round(year_data: dict) -> Optional[dict]:
    """
    Return the last round that has an all_columns list.
    """
    rounds = year_data.get("rounds", [])
    if not isinstance(rounds, list):
        return None

    for rnd in reversed(rounds):
        if isinstance(rnd, dict) and rnd.get("all_columns"):
            return rnd

    return None


def get_report_feature_metadata(screening_report: dict) -> Dict[str, dict]:
    """
    Return report-level feature metadata if Phase 1 provides it.

    Expected from the patched Phase 1 report:
      screening_report["report"]["feature_metadata"]
    """
    report = screening_report.get("report", {})
    if not isinstance(report, dict):
        return {}

    metadata = report.get("feature_metadata", {})
    if isinstance(metadata, dict):
        return metadata

    return {}


def normalize_feature_metadata(feature: str, row: Optional[dict], report_meta: Dict[str, dict]) -> dict:
    """
    Build a stable metadata object for a feature.

    Priority:
      1. metadata already present on the row
      2. report-level feature_metadata
      3. empty fallback fields
    """
    row = row or {}
    meta = report_meta.get(feature, {}) if isinstance(report_meta, dict) else {}

    return {
        "group": row.get("group", meta.get("group", "")),
        "label": row.get("label", meta.get("label", "")),
        "raw_label": row.get("raw_label", meta.get("raw_label", "")),
        "concept": row.get("concept", meta.get("concept", "")),
    }


# ---------------------------------------------------------------------------
# Feature pool accumulator
# ---------------------------------------------------------------------------

class FeaturePoolAccumulator:
    """
    Collects feature evidence from multiple selection rules and emits a clean
    JSON-ready feature pool.
    """

    def __init__(self, report_feature_metadata: Dict[str, dict]):
        self.report_feature_metadata = report_feature_metadata

        self._features: Dict[str, dict] = {}
        self._source_reasons: Dict[str, Set[str]] = defaultdict(set)
        self._years_seen: Dict[str, Set[int]] = defaultdict(set)
        self._abs_r_by_year: Dict[str, Dict[str, float]] = defaultdict(dict)
        self._passes_fdr_by_year: Dict[str, Dict[str, bool]] = defaultdict(dict)
        self._rank_by_year: Dict[str, Dict[str, int]] = defaultdict(dict)

    def add_feature(
        self,
        *,
        feature: str,
        source_reason: str,
        year: Optional[int] = None,
        row: Optional[dict] = None,
        rank: Optional[int] = None,
        extra: Optional[dict] = None,
    ) -> None:
        feature = str(feature)

        if feature not in self._features:
            self._features[feature] = {
                "feature": feature,
                **normalize_feature_metadata(
                    feature,
                    row=row,
                    report_meta=self.report_feature_metadata,
                ),
            }

        # If the original row has better metadata than the earlier placeholder,
        # fill missing values.
        if row:
            meta = normalize_feature_metadata(
                feature,
                row=row,
                report_meta=self.report_feature_metadata,
            )
            for key, value in meta.items():
                if value and not self._features[feature].get(key):
                    self._features[feature][key] = value

        self._source_reasons[feature].add(source_reason)

        if year is not None:
            year_int = int(year)
            year_key = str(year_int)
            self._years_seen[feature].add(year_int)

            if row:
                if "abs_r" in row:
                    try:
                        self._abs_r_by_year[feature][year_key] = float(row["abs_r"])
                    except Exception:
                        pass

                if "passes_fdr" in row:
                    self._passes_fdr_by_year[feature][year_key] = bool(row["passes_fdr"])

            if rank is not None:
                self._rank_by_year[feature][year_key] = int(rank)

        if extra:
            for key, value in extra.items():
                if value is None:
                    continue
                self._features[feature][key] = value

    def build(
        self,
        *,
        min_years_seen: int = 1,
    ) -> List[dict]:
        rows: List[dict] = []

        for feature, base in self._features.items():
            years_seen = sorted(self._years_seen.get(feature, set()))
            source_reasons = sorted(self._source_reasons.get(feature, set()))

            # Stable report-level rows may not have explicit year membership
            # if old reports only store aggregate counts. Do not remove them
            # unless min_years_seen demands evidence and we have year evidence.
            if years_seen and len(years_seen) < min_years_seen:
                continue

            row = {
                **base,
                "source_reasons": source_reasons,
                "years_seen": years_seen,
                "n_years_seen": len(years_seen),
                "abs_r_by_year": dict(sorted(self._abs_r_by_year.get(feature, {}).items())),
                "passes_fdr_by_year": dict(sorted(self._passes_fdr_by_year.get(feature, {}).items())),
                "rank_by_year": dict(sorted(self._rank_by_year.get(feature, {}).items())),
            }

            # Convenience score for sorting.
            abs_values = list(row["abs_r_by_year"].values())
            if abs_values:
                row["mean_abs_r_seen"] = round(sum(abs_values) / len(abs_values), 6)
                row["max_abs_r_seen"] = round(max(abs_values), 6)
            else:
                row["mean_abs_r_seen"] = None
                row["max_abs_r_seen"] = None

            rows.append(row)

        rows.sort(
            key=lambda r: (
                r.get("n_years_seen", 0),
                r.get("max_abs_r_seen") if r.get("max_abs_r_seen") is not None else -1.0,
                r.get("feature", ""),
            ),
            reverse=True,
        )

        return rows


# ---------------------------------------------------------------------------
# Selection rule extractors
# ---------------------------------------------------------------------------

def add_stable_fdr_features(
    *,
    screening_report: dict,
    acc: FeaturePoolAccumulator,
) -> int:
    """
    Add features from:
      report.stable_features.columns

    These are features that passed FDR in enough years to meet the screening
    report's stability threshold.
    """
    report = screening_report.get("report", {})
    stable = report.get("stable_features", {}) if isinstance(report, dict) else {}
    columns = stable.get("columns", []) if isinstance(stable, dict) else []

    count = 0

    if not isinstance(columns, list):
        return 0

    for item in columns:
        if not isinstance(item, dict):
            continue

        feature = item.get("feature")
        if not feature:
            continue

        years_passed_fdr = item.get("years_passed_fdr")

        acc.add_feature(
            feature=str(feature),
            source_reason="stable_fdr",
            row=item,
            extra={
                "years_passed_fdr": years_passed_fdr,
            },
        )
        count += 1

    return count


def add_stable_fallback_features(
    *,
    screening_report: dict,
    acc: FeaturePoolAccumulator,
) -> int:
    """
    Add features from:
      report.stable_features.fallback_columns

    These are features that repeatedly exceeded the data-driven threshold but
    did not necessarily pass FDR.
    """
    report = screening_report.get("report", {})
    stable = report.get("stable_features", {}) if isinstance(report, dict) else {}
    columns = stable.get("fallback_columns", []) if isinstance(stable, dict) else []

    count = 0

    if not isinstance(columns, list):
        return 0

    for item in columns:
        if not isinstance(item, dict):
            continue

        feature = item.get("feature")
        if not feature:
            continue

        acc.add_feature(
            feature=str(feature),
            source_reason="stable_fallback",
            row=item,
            extra={
                "years_above_threshold": item.get("years_above_threshold"),
                "passes_fdr_ever": item.get("passes_fdr_ever"),
            },
        )
        count += 1

    return count


def add_final_round_top_n_features(
    *,
    screening_report: dict,
    acc: FeaturePoolAccumulator,
    top_n: int,
) -> int:
    """
    Add the top N features from the final non-empty screening round for each year.

    This keeps year-specific residual signal after the iterative family exclusion
    process has removed dominant subfamilies.
    """
    if top_n <= 0:
        return 0

    count = 0

    for year_key in get_successful_year_keys(screening_report):
        year_data = screening_report[year_key]
        year = int(year_key)

        last_round = get_last_nonempty_round(year_data)
        if not last_round:
            continue

        all_columns = last_round.get("all_columns", [])
        if not isinstance(all_columns, list):
            continue

        for rank, row in enumerate(all_columns[:top_n], start=1):
            if not isinstance(row, dict):
                continue

            feature = row.get("feature")
            if not feature:
                continue

            acc.add_feature(
                feature=str(feature),
                source_reason=f"final_round_top_{top_n}",
                year=year,
                row=row,
                rank=rank,
            )
            count += 1

    return count


def add_all_fdr_passing_features(
    *,
    screening_report: dict,
    acc: FeaturePoolAccumulator,
) -> int:
    """
    Optional broad rule:
    Add any feature that passed FDR in any round/year.

    This is not enabled by default because it can create a larger feature pool.
    It is useful if you want the sampler to consider all FDR-supported features,
    not just stable/report-level and final-round features.
    """
    count = 0

    for year_key in get_successful_year_keys(screening_report):
        year_data = screening_report[year_key]
        year = int(year_key)

        rounds = year_data.get("rounds", [])
        if not isinstance(rounds, list):
            continue

        seen_this_year: Set[str] = set()

        for rnd in rounds:
            if not isinstance(rnd, dict):
                continue

            all_columns = rnd.get("all_columns", [])
            if not isinstance(all_columns, list):
                continue

            for row in all_columns:
                if not isinstance(row, dict):
                    continue
                if not row.get("passes_fdr", False):
                    continue

                feature = row.get("feature")
                if not feature:
                    continue

                feature = str(feature)
                if feature in seen_this_year:
                    continue

                seen_this_year.add(feature)

                acc.add_feature(
                    feature=feature,
                    source_reason="any_round_fdr",
                    year=year,
                    row=row,
                )
                count += 1

    return count


def add_all_above_threshold_features(
    *,
    screening_report: dict,
    acc: FeaturePoolAccumulator,
) -> int:
    """
    Optional broad rule:
    Add any feature whose abs_r is >= that year's computed_min_abs_r in any round.

    This is not enabled by default because it can create a larger feature pool.
    """
    count = 0

    for year_key in get_successful_year_keys(screening_report):
        year_data = screening_report[year_key]
        year = int(year_key)

        config = year_data.get("config", {})
        threshold = config.get("computed_min_abs_r", None)

        if threshold is None:
            threshold = config.get("min_abs_r", None)

        if threshold is None:
            continue

        try:
            threshold_f = float(threshold)
        except Exception:
            continue

        rounds = year_data.get("rounds", [])
        if not isinstance(rounds, list):
            continue

        seen_this_year: Set[str] = set()

        for rnd in rounds:
            if not isinstance(rnd, dict):
                continue

            all_columns = rnd.get("all_columns", [])
            if not isinstance(all_columns, list):
                continue

            for row in all_columns:
                if not isinstance(row, dict):
                    continue

                feature = row.get("feature")
                if not feature:
                    continue

                feature = str(feature)
                if feature in seen_this_year:
                    continue

                try:
                    abs_r = float(row.get("abs_r", 0.0))
                except Exception:
                    continue

                if abs_r < threshold_f:
                    continue

                seen_this_year.add(feature)

                acc.add_feature(
                    feature=feature,
                    source_reason="any_round_above_threshold",
                    year=year,
                    row=row,
                )
                count += 1

    return count


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build_screened_feature_pool(
    screening_report: dict,
    *,
    pool_name: str = "screened_hybrid_pool",
    include_stable_fdr: bool = True,
    include_stable_fallback: bool = True,
    include_final_round_top_n: int = 20,
    include_any_round_fdr: bool = False,
    include_any_round_above_threshold: bool = False,
    min_years_seen: int = 1,
) -> dict:
    """
    Build a screened feature pool from the Phase 1 report.

    The default pool is intentionally moderate:
      - stable FDR features
      - stable fallback features
      - top N final-round features per year

    It does NOT mean all features are used together in one model.
    It only defines the candidate universe for recipe sampling.
    """
    report_meta = get_report_feature_metadata(screening_report)
    acc = FeaturePoolAccumulator(report_feature_metadata=report_meta)

    rule_counts: Dict[str, int] = {}

    if include_stable_fdr:
        rule_counts["stable_fdr"] = add_stable_fdr_features(
            screening_report=screening_report,
            acc=acc,
        )

    if include_stable_fallback:
        rule_counts["stable_fallback"] = add_stable_fallback_features(
            screening_report=screening_report,
            acc=acc,
        )

    if include_final_round_top_n > 0:
        rule_counts[f"final_round_top_{include_final_round_top_n}"] = add_final_round_top_n_features(
            screening_report=screening_report,
            acc=acc,
            top_n=include_final_round_top_n,
        )

    if include_any_round_fdr:
        rule_counts["any_round_fdr"] = add_all_fdr_passing_features(
            screening_report=screening_report,
            acc=acc,
        )

    if include_any_round_above_threshold:
        rule_counts["any_round_above_threshold"] = add_all_above_threshold_features(
            screening_report=screening_report,
            acc=acc,
        )

    features = acc.build(min_years_seen=min_years_seen)
    feature_codes = [row["feature"] for row in features]

    successful_years = [int(y) for y in get_successful_year_keys(screening_report)]
    errored_years = get_errored_years(screening_report)

    warnings: List[dict] = []

    if not features:
        warnings.append(
            {
                "type": "empty_feature_pool",
                "message": "No features were selected into the screened feature pool.",
            }
        )

    if errored_years:
        warnings.append(
            {
                "type": "errored_years_present",
                "message": "Some Phase 1 years failed and were excluded from pool construction.",
                "errored_years": errored_years,
            }
        )

    phase_1_summary = {}
    report = screening_report.get("report", {})
    if isinstance(report, dict):
        phase_1_summary = {
            "convergence_summary": report.get("convergence_summary", {}),
            "cross_year_summary": report.get("cross_year_summary", {}),
            "stable_feature_thresholds": {
                "col_threshold": report.get("stable_features", {}).get("col_threshold")
                if isinstance(report.get("stable_features", {}), dict)
                else None,
                "fallback_threshold": report.get("stable_features", {}).get("fallback_threshold")
                if isinstance(report.get("stable_features", {}), dict)
                else None,
                "sf_threshold": report.get("stable_features", {}).get("sf_threshold")
                if isinstance(report.get("stable_features", {}), dict)
                else None,
            },
        }

    return {
        "pool_name": pool_name,
        "source": "phase_1_screening",
        "artifact_type": "screened_feature_pool",
        "description": (
            "Candidate ACS feature pool for Phase 2 recipe sampling. "
            "These features are allowed into the sampler; they are not meant "
            "to be trained together as one oversized final recipe."
        ),
        "selection_rules": {
            "include_stable_fdr": include_stable_fdr,
            "include_stable_fallback": include_stable_fallback,
            "include_final_round_top_n": include_final_round_top_n,
            "include_any_round_fdr": include_any_round_fdr,
            "include_any_round_above_threshold": include_any_round_above_threshold,
            "min_years_seen": min_years_seen,
        },
        "rule_counts_before_dedup": rule_counts,
        "summary": {
            "n_features": len(features),
            "successful_years": successful_years,
            "errored_years": errored_years,
            "n_successful_years": len(successful_years),
            "n_errored_years": len(errored_years),
        },
        "phase_1_summary": phase_1_summary,
        "feature_codes": feature_codes,
        "features": features,
        "warnings": warnings,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> dict:
    parser = argparse.ArgumentParser(
        description=(
            "Build a screened candidate feature pool from a Phase 1 screening report. "
            "This pool is intended as the input feature universe for Phase 2 recipe sampling."
        )
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Path to Phase 1 screening report JSON.",
    )

    parser.add_argument(
        "--output",
        required=True,
        help="Path to write screened feature pool JSON.",
    )

    parser.add_argument(
        "--pool-name",
        default="screened_hybrid_pool",
        help="Name for this screened feature pool.",
    )

    parser.add_argument(
        "--last-round-top-n",
        type=int,
        default=20,
        help=(
            "Number of top features to take from each year's final non-empty "
            "screening round. Use 0 to disable."
        ),
    )

    parser.add_argument(
        "--min-years-seen",
        type=int,
        default=1,
        help=(
            "Minimum number of explicit years a feature must be seen in. "
            "Default 1. Stable aggregate features from old reports may not "
            "have explicit year membership."
        ),
    )

    parser.add_argument(
        "--no-stable-fdr",
        action="store_true",
        help="Disable stable FDR features.",
    )

    parser.add_argument(
        "--no-stable-fallback",
        action="store_true",
        help="Disable stable fallback features.",
    )

    parser.add_argument(
        "--include-any-round-fdr",
        action="store_true",
        help=(
            "Also include any feature that passed FDR in any round/year. "
            "This can make the pool much larger."
        ),
    )

    parser.add_argument(
        "--include-any-round-above-threshold",
        action="store_true",
        help=(
            "Also include any feature above that year's computed threshold "
            "in any round. This can make the pool much larger."
        ),
    )

    args = parser.parse_args()

    screening_report = load_json(args.input)

    pool = build_screened_feature_pool(
        screening_report,
        pool_name=args.pool_name,
        include_stable_fdr=not args.no_stable_fdr,
        include_stable_fallback=not args.no_stable_fallback,
        include_final_round_top_n=args.last_round_top_n,
        include_any_round_fdr=args.include_any_round_fdr,
        include_any_round_above_threshold=args.include_any_round_above_threshold,
        min_years_seen=args.min_years_seen,
    )

    write_json(args.output, pool)

    print()
    print(f"Screened feature pool written to: {args.output}")
    print(f"Features selected: {pool['summary']['n_features']}")
    print(f"Successful years:  {pool['summary']['successful_years']}")

    if pool.get("warnings"):
        print(f"Warnings:          {len(pool['warnings'])}")

    return pool


if __name__ == "__main__":
    main()