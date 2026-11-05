from __future__ import annotations

import json
import sys
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from sklearn.model_selection import train_test_split, KFold

sys.path.insert(0, str(Path(__file__).parent))

# ---------------------------------------------------------------------------
# Phase 2 imports
# ---------------------------------------------------------------------------

from phase_2.screened_feature_pool_builder import build_screened_feature_pool
from phase_2.ml_dataset_builder import build_screened_ml_datasets
from phase_2.catboost_recipe_sampler import RecipeSamplerConfig, build_sampling_report


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title="Phase 2 CatBoost Recipe Sampling API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Path / JSON helpers
# ---------------------------------------------------------------------------

RESULTS_ROOT = Path("results") / "phase_2"


def load_json(path: str | Path) -> dict:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"JSON file not found: {p}")

    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: str | Path, data: dict | list) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    with open(p, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def slugify(value: str, *, max_len: int = 64) -> str:
    value = str(value or "").strip().lower()
    out: List[str] = []

    for ch in value:
        if ch.isalnum():
            out.append(ch)
        elif ch in {" ", "-", "_", ".", "/"}:
            out.append("_")

    slug = "".join(out)
    while "__" in slug:
        slug = slug.replace("__", "_")

    slug = slug.strip("_")
    if not slug:
        slug = "phase2_run"

    return slug[:max_len]


def derive_base_slug_from_screening_report(screening_report: dict, fallback: str = "phase2_run") -> str:
    """
    Derive a readable output slug from the first successful year config.
    """
    for key, value in screening_report.items():
        if not str(key).isdigit():
            continue
        if not isinstance(value, dict) or "error" in value:
            continue

        config = value.get("config", {})
        loader = config.get("loader", "loader")

        if loader == "cancer":
            site = config.get("cancer_site", "cancer")
            window = config.get("window", 1)
            return slugify(f"{loader}_{site}_w{window}")

        metric = config.get("metric", "")
        if metric:
            return slugify(f"{loader}_{metric}")

        return slugify(loader)

    return slugify(fallback)


def read_report_from_path_or_body(
    *,
    report_path: Optional[str],
    report_body: Optional[Dict[str, Any]],
    field_name: str,
) -> dict:
    """
    Accept either an inline JSON body or a file path.

    Path is preferred if both are supplied, because it is more reproducible.
    """
    if report_path:
        return load_json(report_path)

    if report_body is not None:
        return report_body

    raise ValueError(f"Either {field_name}_path or {field_name} must be supplied.")


def parse_years(years: Optional[List[int]]) -> Optional[List[int]]:
    if years is None:
        return None
    out = sorted({int(y) for y in years})
    return out or None


def handle_exception(exc: Exception, *, debug: bool = False) -> HTTPException:
    detail: Dict[str, Any] = {
        "error": str(exc),
        "type": exc.__class__.__name__,
    }

    if debug:
        detail["traceback"] = traceback.format_exc()

    return HTTPException(status_code=500, detail=detail)


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class PoolBuildRequest(BaseModel):
    """Build screened_feature_pool.json from a Phase 1 screening report."""

    screening_report_path: Optional[str] = Field(
        None,
        description="Path to Phase 1 screening report JSON. Preferred for reproducibility.",
    )
    screening_report: Optional[Dict[str, Any]] = Field(
        None,
        description="Inline Phase 1 screening report JSON.",
    )

    output_path: Optional[str] = Field(
        None,
        description="Optional output path for screened_feature_pool.json.",
    )
    pool_name: str = Field("screened_hybrid_pool")

    include_stable_fdr: bool = True
    include_stable_fallback: bool = True
    include_final_round_top_n: int = Field(20, ge=0)

    include_any_round_fdr: bool = False
    include_any_round_above_threshold: bool = False
    min_years_seen: int = Field(1, ge=1)

    return_full_pool: bool = Field(True, description="If false, return only path and summary.")
    debug: bool = False


class DatasetBuildRequest(BaseModel):
    """Build screened X/y datasets from Phase 1 report + screened feature pool."""

    screening_report_path: Optional[str] = None
    screening_report: Optional[Dict[str, Any]] = None

    screened_feature_pool_path: Optional[str] = None
    screened_feature_pool: Optional[Dict[str, Any]] = None

    output_dir: Optional[str] = Field(
        None,
        description="Output directory. Defaults to results/phase_2/<slug>/screened_dataset.",
    )

    years: Optional[List[int]] = Field(None, description="Optional ACS years to build.")
    strict_features: bool = False
    write_csv: bool = True

    return_full_manifest: bool = True
    debug: bool = False


class SamplerRunRequest(BaseModel):
    """Run CatBoost recipe sampling from a dataset manifest."""

    dataset_manifest_path: Optional[str] = Field(
        None,
        description="Path to manifest.json from ml_dataset_builder.py.",
    )
    dataset_manifest: Optional[Dict[str, Any]] = Field(
        None,
        description="Inline dataset manifest.",
    )

    output_path: Optional[str] = Field(
        None,
        description="Output path for catboost_recipe_sampling_report.json.",
    )

    feature_families_path: Optional[str] = Field(
        None,
        description="Optional path to feature_families.json. If omitted, metadata group fallback is used.",
    )

    mode: str = Field("per_year", description="per_year | pooled | both")

    n_recipes: int = Field(200, ge=1)
    min_families: int = Field(1, ge=1)
    max_families: int = Field(5, ge=1)
    min_features: int = Field(8, ge=1)
    max_features: int = Field(30, ge=1)
    coverage_rounds: int = Field(5, ge=1)
    random_state: int = 42
    test_size: float = Field(0.25, gt=0.0, lt=1.0)

    iterations: int = Field(300, ge=1)
    depth: int = Field(3, ge=1)
    learning_rate: float = Field(0.03, gt=0.0)

    min_rows: int = Field(10, ge=2)
    pair_min_count: int = Field(5, ge=1)
    top_n: int = Field(200, ge=1)

    return_full_report: bool = True
    debug: bool = False


class FullPhase2RunRequest(BaseModel):
    """Convenience endpoint: screening report -> pool -> dataset -> sampler report."""

    screening_report_path: Optional[str] = None
    screening_report: Optional[Dict[str, Any]] = None

    output_root: Optional[str] = Field(
        None,
        description="Defaults to results/phase_2/<derived_slug>.",
    )
    run_name: Optional[str] = Field(
        None,
        description="Optional slug/name used under results/phase_2.",
    )

    pool_name: str = "screened_hybrid_pool"
    include_stable_fdr: bool = True
    include_stable_fallback: bool = True
    include_final_round_top_n: int = Field(20, ge=0)
    include_any_round_fdr: bool = False
    include_any_round_above_threshold: bool = False
    min_years_seen: int = Field(1, ge=1)

    years: Optional[List[int]] = None
    strict_features: bool = False
    write_csv: bool = True

    feature_families_path: Optional[str] = None
    mode: str = Field("per_year", description="per_year | pooled | both")

    n_recipes: int = Field(200, ge=1)
    min_families: int = Field(1, ge=1)
    max_families: int = Field(5, ge=1)
    min_features: int = Field(8, ge=1)
    max_features: int = Field(30, ge=1)
    coverage_rounds: int = Field(5, ge=1)
    random_state: int = 42
    test_size: float = Field(0.25, gt=0.0, lt=1.0)

    iterations: int = Field(300, ge=1)
    depth: int = Field(3, ge=1)
    learning_rate: float = Field(0.03, gt=0.0)

    min_rows: int = Field(10, ge=2)
    pair_min_count: int = Field(5, ge=1)
    top_n: int = Field(50, ge=1)

    return_full_outputs: bool = True
    debug: bool = False


# ---------------------------------------------------------------------------
# Validation and compact responses
# ---------------------------------------------------------------------------

def validate_sampler_mode(mode: str) -> str:
    mode = str(mode).strip()
    if mode not in {"per_year", "pooled", "both"}:
        raise ValueError(f"mode must be one of: per_year, pooled, both. Got {mode!r}.")
    return mode


def validate_sampler_feature_bounds(req: SamplerRunRequest | FullPhase2RunRequest) -> None:
    if req.min_families > req.max_families:
        raise ValueError(
            f"min_families ({req.min_families}) cannot exceed max_families ({req.max_families})."
        )

    if req.min_features > req.max_features:
        raise ValueError(
            f"min_features ({req.min_features}) cannot exceed max_features ({req.max_features})."
        )


def build_sampler_config(req: SamplerRunRequest | FullPhase2RunRequest) -> RecipeSamplerConfig:
    validate_sampler_feature_bounds(req)

    return RecipeSamplerConfig(
        n_recipes=req.n_recipes,
        min_families=req.min_families,
        max_families=req.max_families,
        min_features=req.min_features,
        max_features=req.max_features,
        coverage_rounds=req.coverage_rounds,
        random_state=req.random_state,
        test_size=req.test_size,
        iterations=req.iterations,
        depth=req.depth,
        learning_rate=req.learning_rate,
        min_rows=req.min_rows,
        pair_min_count=req.pair_min_count,
        top_n=req.top_n,
    )


def compact_pool_response(pool: dict, output_path: str) -> dict:
    return {
        "status": "ok",
        "output_path": output_path,
        "pool_name": pool.get("pool_name", ""),
        "artifact_type": pool.get("artifact_type", ""),
        "summary": pool.get("summary", {}),
        "selection_rules": pool.get("selection_rules", {}),
        "rule_counts_before_dedup": pool.get("rule_counts_before_dedup", {}),
        "warnings": pool.get("warnings", []),
    }


def compact_manifest_response(manifest: dict, manifest_path: str) -> dict:
    return {
        "status": manifest.get("status", "ok"),
        "manifest_path": manifest_path,
        "output_dir": manifest.get("output_dir", ""),
        "dataset_type": manifest.get("dataset_type", ""),
        "pool_name": manifest.get("pool_name", ""),
        "years_built": manifest.get("years_built", []),
        "n_years_built": manifest.get("n_years_built", 0),
        "n_features_in_pool": manifest.get("n_features_in_pool", 0),
        "warnings": manifest.get("warnings", []),
    }


def compact_sampler_response(report: dict, output_path: str) -> dict:
    response = {
        "status": report.get("status", "ok"),
        "output_path": output_path,
        "artifact_type": report.get("artifact_type", ""),
        "runner": report.get("runner", ""),
        "settings": report.get("settings", {}),
        "dataset_manifest": report.get("dataset_manifest", {}),
        "warnings": report.get("warnings", []),
        "result_summaries": {},
    }

    results = report.get("results", {})
    per_year = results.get("per_year")
    if isinstance(per_year, dict):
        response["result_summaries"]["per_year"] = {
            year: data.get("summary", {}) if isinstance(data, dict) else {}
            for year, data in per_year.items()
        }

    pooled = results.get("pooled")
    if isinstance(pooled, dict):
        response["result_summaries"]["pooled"] = pooled.get("summary", {})

    return response


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
def health() -> dict:
    return {"status": "ok", "api": "phase_2_ml", "version": "1.0.0"}


@app.get("/template")
def template() -> dict:
    """Return defaults and valid values for Godot UI."""
    return {
        "phase": "phase_2",
        "artifact_flow": [
            "screened_feature_pool",
            "screened_ml_dataset_manifest",
            "catboost_recipe_sampling_report",
        ],
        "endpoints": {
            "pool_build": "POST /pool/build",
            "dataset_build": "POST /dataset/build",
            "sampler_run": "POST /sampler/run",
            "full_run": "POST /run",
        },
        "pool_defaults": {
            "pool_name": "screened_hybrid_pool",
            "include_stable_fdr": True,
            "include_stable_fallback": True,
            "include_final_round_top_n": 20,
            "include_any_round_fdr": False,
            "include_any_round_above_threshold": False,
            "min_years_seen": 1,
        },
        "dataset_defaults": {
            "strict_features": False,
            "write_csv": True,
            "years": None,
        },
        "sampler_defaults": {
            "mode_options": ["per_year", "pooled", "both"],
            "mode": "per_year",
            "n_recipes": 200,
            "min_families": 1,
            "max_families": 5,
            "min_features": 8,
            "max_features": 30,
            "coverage_rounds": 5,
            "random_state": 42,
            "test_size": 0.25,
            "iterations": 300,
            "depth": 3,
            "learning_rate": 0.03,
            "min_rows": 10,
            "pair_min_count": 5,
            "top_n": 50,
        },
        "notes": [
            "Phase 2 samples bounded CatBoost recipes from the screened feature pool.",
            "The screened feature pool is not intended to be trained as one giant model.",
            "Use /run for the full pipeline or individual endpoints for stepwise Godot control.",
        ],
    }


@app.post("/pool/build")
def build_pool(req: PoolBuildRequest) -> dict:
    try:
        screening_report = read_report_from_path_or_body(
            report_path=req.screening_report_path,
            report_body=req.screening_report,
            field_name="screening_report",
        )

        output_path = req.output_path
        if not output_path:
            slug = derive_base_slug_from_screening_report(screening_report)
            output_path = str(RESULTS_ROOT / slug / "screened_feature_pool.json")

        pool = build_screened_feature_pool(
            screening_report,
            pool_name=req.pool_name,
            include_stable_fdr=req.include_stable_fdr,
            include_stable_fallback=req.include_stable_fallback,
            include_final_round_top_n=req.include_final_round_top_n,
            include_any_round_fdr=req.include_any_round_fdr,
            include_any_round_above_threshold=req.include_any_round_above_threshold,
            min_years_seen=req.min_years_seen,
        )

        write_json(output_path, pool)

        if req.return_full_pool:
            return {"status": "ok", "output_path": output_path, "pool": pool}

        return compact_pool_response(pool, output_path)

    except Exception as exc:
        raise handle_exception(exc, debug=req.debug)


@app.post("/dataset/build")
def build_dataset(req: DatasetBuildRequest) -> dict:
    try:
        screening_report = read_report_from_path_or_body(
            report_path=req.screening_report_path,
            report_body=req.screening_report,
            field_name="screening_report",
        )

        screened_feature_pool = read_report_from_path_or_body(
            report_path=req.screened_feature_pool_path,
            report_body=req.screened_feature_pool,
            field_name="screened_feature_pool",
        )

        output_dir = req.output_dir
        if not output_dir:
            slug = derive_base_slug_from_screening_report(screening_report)
            output_dir = str(RESULTS_ROOT / slug / "screened_dataset")

        manifest = build_screened_ml_datasets(
            screening_report=screening_report,
            screened_feature_pool=screened_feature_pool,
            output_dir=output_dir,
            years=parse_years(req.years),
            strict_features=req.strict_features,
            write_csv=req.write_csv,
        )

        manifest_path = str(Path(output_dir) / "manifest.json")

        if req.return_full_manifest:
            return {
                "status": manifest.get("status", "ok"),
                "manifest_path": manifest_path,
                "manifest": manifest,
            }

        return compact_manifest_response(manifest, manifest_path)

    except Exception as exc:
        raise handle_exception(exc, debug=req.debug)


@app.post("/sampler/run")
def run_sampler(req: SamplerRunRequest) -> dict:
    try:
        mode = validate_sampler_mode(req.mode)

        dataset_manifest = read_report_from_path_or_body(
            report_path=req.dataset_manifest_path,
            report_body=req.dataset_manifest,
            field_name="dataset_manifest",
        )

        cfg = build_sampler_config(req)

        report = build_sampling_report(
            dataset_manifest=dataset_manifest,
            mode=mode,
            cfg=cfg,
            feature_families_path=req.feature_families_path,
        )

        output_path = req.output_path
        if not output_path:
            output_dir = dataset_manifest.get("output_dir", "")
            if output_dir:
                output_path = str(Path(output_dir) / "catboost_recipe_sampling_report.json")
            else:
                output_path = str(RESULTS_ROOT / "catboost_recipe_sampling_report.json")

        write_json(output_path, report)

        if req.return_full_report:
            return {"status": report.get("status", "ok"), "output_path": output_path, "report": report}

        return compact_sampler_response(report, output_path)

    except Exception as exc:
        raise handle_exception(exc, debug=req.debug)


@app.post("/run")
def run_full_phase2(req: FullPhase2RunRequest) -> dict:
    """Convenience endpoint: Phase 1 report -> screened pool -> screened dataset -> CatBoost sampler."""
    try:
        mode = validate_sampler_mode(req.mode)
        validate_sampler_feature_bounds(req)

        screening_report = read_report_from_path_or_body(
            report_path=req.screening_report_path,
            report_body=req.screening_report,
            field_name="screening_report",
        )

        if req.output_root:
            output_root = Path(req.output_root)
        else:
            base_slug = slugify(req.run_name) if req.run_name else derive_base_slug_from_screening_report(screening_report)
            output_root = RESULTS_ROOT / base_slug

        output_root.mkdir(parents=True, exist_ok=True)

        pool_path = output_root / "screened_feature_pool.json"
        dataset_dir = output_root / "screened_dataset"
        sampler_report_path = output_root / "catboost_recipe_sampling_report.json"

        pool = build_screened_feature_pool(
            screening_report,
            pool_name=req.pool_name,
            include_stable_fdr=req.include_stable_fdr,
            include_stable_fallback=req.include_stable_fallback,
            include_final_round_top_n=req.include_final_round_top_n,
            include_any_round_fdr=req.include_any_round_fdr,
            include_any_round_above_threshold=req.include_any_round_above_threshold,
            min_years_seen=req.min_years_seen,
        )
        write_json(pool_path, pool)

        manifest = build_screened_ml_datasets(
            screening_report=screening_report,
            screened_feature_pool=pool,
            output_dir=dataset_dir,
            years=parse_years(req.years),
            strict_features=req.strict_features,
            write_csv=req.write_csv,
        )

        manifest_path = dataset_dir / "manifest.json"

        cfg = build_sampler_config(req)

        sampler_report = build_sampling_report(
            dataset_manifest=manifest,
            mode=mode,
            cfg=cfg,
            feature_families_path=req.feature_families_path,
        )
        write_json(sampler_report_path, sampler_report)

        response: Dict[str, Any] = {
            "status": "ok",
            "output_root": str(output_root),
            "pool_path": str(pool_path),
            "dataset_manifest_path": str(manifest_path),
            "sampler_report_path": str(sampler_report_path),
            "pool_summary": pool.get("summary", {}),
            "dataset_summary": {
                "status": manifest.get("status", "ok"),
                "years_built": manifest.get("years_built", []),
                "n_years_built": manifest.get("n_years_built", 0),
                "n_features_in_pool": manifest.get("n_features_in_pool", None),
                "warnings": manifest.get("warnings", []),
            },
            "sampler_summary": compact_sampler_response(sampler_report, str(sampler_report_path)),
        }

        if req.return_full_outputs:
            response["pool"] = pool
            response["dataset_manifest"] = manifest
            response["sampler_report"] = sampler_report

        return response

    except Exception as exc:
        raise handle_exception(exc, debug=req.debug)


# ---------------------------------------------------------------------------
# Dev entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    port = 11651
    uvicorn.run("ml_api:app", host="0.0.0.0", port=port, reload=False)
