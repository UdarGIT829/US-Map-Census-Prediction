#!/usr/bin/env python3
# screening_api.py
#
# FastAPI server exposing the iterative screening pipeline to Godot.
#
# Endpoints:
#   GET  /health       — liveness check
#   GET  /template     — valid params per loader, built from live loader introspection
#   POST /run          — run screening, holds connection, returns full result JSON
#
# Run:
#   uvicorn screening_api:app --host 0.0.0.0 --port 8000
#
# Godot hits /template on startup to build its UI, then POST /run to trigger
# a screening run and receive the result JSON in the response body.
# Results are also written to disk under results/.

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).parent))

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from dry_run_iterative_screening import run, CROP_CSV_DIR, CANCER_CSV, OPIOID_CSV, UFO_CSV

app = FastAPI(title="Phase 1 Screening API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Template — built once at startup from live loader introspection
# ---------------------------------------------------------------------------

def _build_template() -> Dict[str, Any]:
    """
    Introspect each loader to build the full template of valid params.
    Called once at startup and cached.
    """
    template: Dict[str, Any] = {"loaders": {}, "shared": {}}

    # ── Cancer ───────────────────────────────────────────────────────────────
    try:
        from phase_1.cancer_loader import CancerLoader
        cancer_sites = CancerLoader.list_sites(CANCER_CSV) if CANCER_CSV.exists() else []
        template["loaders"]["cancer"] = {
            "acs_years":  sorted(CancerLoader.ACS_PAIRINGS.keys()),
            "sites":      cancer_sites,
            "metrics":    ["count"],
            "window_min": 1,
            "window_max": 5,
            "notes":      "metric='count' only. Population-normalised rates derive post-join.",
        }
    except Exception as e:
        template["loaders"]["cancer"] = {"error": str(e)}

    # ── Opioid ───────────────────────────────────────────────────────────────
    try:
        from phase_1.opioid_loader import OpioidLoader, _METRIC_COLUMNS
        template["loaders"]["opioid"] = {
            "acs_years":  sorted(OpioidLoader.ACS_PAIRINGS.keys()),
            "metrics":    sorted(_METRIC_COLUMNS.keys()),
            "window_min": 1,
            "window_max": 5,
            "notes":      "age_adjusted_rate recommended. CDC suppresses states with <10 deaths.",
        }
    except Exception as e:
        template["loaders"]["opioid"] = {"error": str(e)}


    # ── UFO ──────────────────────────────────────────────────────────────────
    try:
        from phase_1.ufo_loader import UFOLoader, ACS_SIGHTING_WINDOWS
        template["loaders"]["ufo"] = {
            "acs_years":        sorted(UFOLoader.ACS_PAIRINGS.keys()),
            "metrics":          ["raw_count"],
            "sighting_windows": {
                str(yr): f"{w[0]}–{w[1]}"
                for yr, w in ACS_SIGHTING_WINDOWS.items()
            },
            "window_min": 1,
            "window_max": 1,
            "notes": (
                "metric='sightings_per_100k' requires population Series — "
                "not available via API. Use raw_count; pipeline normalises post-join."
            ),
        }
    except Exception as e:
        template["loaders"]["ufo"] = {"error": str(e)}

    # ── Shared params ─────────────────────────────────────────────────────────
    template["shared"] = {
        "threshold_mode":     {"options": ["mean", "q3"], "default": "mean"},
        "r_floor_fdr":        {"default": 0.05, "description": "Floor for min_abs_r. Safety net only — keep low."},
        "r_floor_convergence":{"default": 0.10, "description": "Floor for convergence_r. Prevents runaway on noise."},
        "stale_rounds":       {"default": 3,    "description": "Stop after N consecutive rounds with 0 FDR-passing columns."},
        "top_n":              {"default": 5,     "description": "Top columns/subfamilies recorded per round."},
        "rounds":             {"default": 999,   "description": "Hard cap on exclusion rounds. Convergence triggers first."},
    }

    return template


# Build template at startup
_TEMPLATE: Dict[str, Any] = {}

@app.on_event("startup")
async def startup_event():
    global _TEMPLATE
    _TEMPLATE = _build_template()


# ---------------------------------------------------------------------------
# Request model
# ---------------------------------------------------------------------------

class RunRequest(BaseModel):
    loader:              str   = Field(..., description="cancer | opioid | crop | ufo")
    # cancer
    site:                str   = Field("All Invasive Cancer Sites Combined", description="Cancer site name (cancer loader only)")
    # opioid / crop / ufo
    metric:              str   = Field("", description="Metric to use. Loader-specific.")
    # crop
    crop:                str   = Field("corn", description="Crop slug (crop loader only)")
    # shared
    window:              int   = Field(1,    ge=1, le=5, description="Years to aggregate (cancer and opioid only)")
    threshold_mode:      str   = Field("mean", description="mean or q3")
    r_floor_fdr:         float = Field(0.05, ge=0.0, le=1.0)
    r_floor_convergence: float = Field(0.10, ge=0.0, le=1.0)
    stale_rounds:        int   = Field(3,    ge=1)
    top_n:               int   = Field(5,    ge=1)
    rounds:              int   = Field(999,  ge=1)


def _derive_output_path(req: RunRequest) -> Path:
    """Auto-derive a results filename from the request params."""
    loader = req.loader
    if loader == "cancer":
        slug = req.site.lower().replace(" ", "_").replace(",", "")[:40]
    elif loader == "opioid":
        metric = req.metric or "age_adjusted_rate"
        slug = metric
    elif loader == "crop":
        slug = req.crop.lower().replace(" ", "_")
    else:  # ufo
        slug = "ufo"

    window_str = f"_w{req.window}" if req.window > 1 else ""
    mode_str   = f"_{req.threshold_mode}" if req.threshold_mode != "mean" else ""
    filename   = f"{loader}_{slug}{window_str}{mode_str}_all.json"
    out_path   = Path("results") / filename
    out_path.parent.mkdir(parents=True, exist_ok=True)
    return out_path


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/template")
def template():
    if not _TEMPLATE:
        raise HTTPException(status_code=503, detail="Template not yet built — server still starting.")
    return _TEMPLATE


@app.post("/run")
def run_screening(req: RunRequest):
    """
    Run iterative screening for all years in the loader's ACS_PAIRINGS.
    Holds the connection open until complete, then returns the full result JSON.
    Also writes the result to disk under results/.
    """
    # Validate loader
    valid_loaders = {"cancer", "opioid", "crop", "ufo"}
    if req.loader not in valid_loaders:
        raise HTTPException(
            status_code=422,
            detail=f"Unknown loader {req.loader!r}. Valid: {sorted(valid_loaders)}."
        )

    # Validate threshold_mode
    if req.threshold_mode not in ("mean", "q3"):
        raise HTTPException(
            status_code=422,
            detail=f"threshold_mode must be 'mean' or 'q3', got {req.threshold_mode!r}."
        )

    # Resolve metric defaults per loader
    metric = req.metric
    if not metric:
        if req.loader == "opioid":
            metric = "age_adjusted_rate"
        elif req.loader == "cancer":
            metric = "count"
        elif req.loader == "crop":
            metric = "production_bu"
        elif req.loader == "ufo":
            metric = "raw_count"

    try:
        result = run(
            loader_name=req.loader,
            cancer_site=req.site,
            metric=metric,
            n_rounds=req.rounds,
            top_n=req.top_n,
            r_floor_fdr=req.r_floor_fdr,
            r_floor_convergence=req.r_floor_convergence,
            threshold_mode=req.threshold_mode,
            crop_name=req.crop,
            window=req.window,
            stale_rounds=req.stale_rounds,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    # Write to disk
    out_path = _derive_output_path(req)
    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
    except Exception as e:
        # Non-fatal — Godot still gets the result body
        print(f"WARNING: could not write result to {out_path}: {e}")

    return result


# ---------------------------------------------------------------------------
# Dev entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    port = 32102
    uvicorn.run("screening_api:app", host="0.0.0.0", port=port, reload=False)