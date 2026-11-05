import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).parent))

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, model_validator

from dry_run_iterative_screening import (
    run,
    CANCER_CSV,
    OPIOID_CSV,
    UFO_CSV,
)

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
    template: Dict[str, Any] = {"loaders": {}, "shared": {}}

    # Cancer
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

    # Opioid
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


    # UFO
    try:
        from phase_1.ufo_loader import UFOLoader, ACS_SIGHTING_WINDOWS
        template["loaders"]["ufo"] = {
            "acs_years":        sorted(UFOLoader.ACS_PAIRINGS.keys()),
            "metrics":          ["raw_count"],
            "sighting_windows": {
                str(yr): f"{w[0]}-{w[1]}"
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

    # Shared
    template["shared"] = {
        "threshold_mode":      {"options": ["mean", "q3"], "default": "mean"},
        "r_floor_fdr":         {"default": 0.05,  "description": "Floor for min_abs_r. Safety net only."},
        "r_floor_convergence": {"default": 0.10,  "description": "Floor for convergence_r. Prevents runaway on noise."},
        "stale_rounds":        {"default": 3,      "description": "Stop after N consecutive rounds with 0 FDR-passing columns."},
        "top_n":               {"default": 5,      "description": "Top columns/subfamilies recorded per round."},
        "rounds":              {"default": 999,    "description": "Hard cap on exclusion rounds. Convergence triggers first."},
    }

    return template


_TEMPLATE: Dict[str, Any] = {}


@app.on_event("startup")
async def startup_event():
    global _TEMPLATE
    _TEMPLATE = _build_template()


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class UserDataPayload(BaseModel):
    rows:        List[Dict[str, Any]] = Field(..., description="Raw rows as parsed by Godot.")
    fips_col:    str                  = Field(..., description="Column containing state FIPS codes.")
    target_col:  str                  = Field(..., description="Column containing the target variable.")
    year_col:    Optional[str]        = Field(None, description="Column containing year values. Omit if using single_year.")
    single_year: Optional[int]        = Field(None, description="Single year to assign when year_col is not provided.")

    @model_validator(mode="after")
    def check_year_source(self):
        if self.year_col is None and self.single_year is None:
            raise ValueError("Either year_col or single_year must be provided in user_data.")
        return self


class RunRequest(BaseModel):
    # Path A
    loader:    Optional[str]       = Field(None, description="cancer | opioid | ufo")
    years:     Optional[List[int]] = Field(None, description="Subset of ACS years to run. All available if omitted.")
    site:      str                 = Field("All Invasive Cancer Sites Combined")
    metric:    str                 = Field("")
    window:    int                 = Field(1, ge=1, le=5)
    # Path B
    user_data: Optional[UserDataPayload] = Field(None)
    # Shared
    threshold_mode:      str   = Field("mean")
    r_floor_fdr:         float = Field(0.05, ge=0.0, le=1.0)
    r_floor_convergence: float = Field(0.10, ge=0.0, le=1.0)
    stale_rounds:        int   = Field(3,   ge=1)
    top_n:               int   = Field(5,   ge=1)
    rounds:              int   = Field(999, ge=1)

    @model_validator(mode="after")
    def check_path(self):
        if self.loader is None and self.user_data is None:
            raise ValueError("Either 'loader' or 'user_data' must be provided.")
        if self.loader is not None and self.user_data is not None:
            raise ValueError("Provide either 'loader' or 'user_data', not both.")
        return self


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_VALID_LOADERS  = {"cancer", "opioid", "ufo"}
_DEFAULT_METRIC = {
    "cancer": "count",
    "opioid": "age_adjusted_rate",
    "ufo":    "raw_count",
}


def _derive_output_path(req: RunRequest) -> Path:
    if req.user_data is not None:
        slug       = req.user_data.target_col.lower().replace(" ", "_")[:40]
        loader_str = "user"
    else:
        loader_str = req.loader
        if req.loader == "cancer":
            slug = req.site.lower().replace(" ", "_").replace(",", "")[:40]
        elif req.loader == "opioid":
            slug = req.metric or _DEFAULT_METRIC["opioid"]
        else:
            slug = "ufo"

    window_str = f"_w{req.window}" if req.window > 1 else ""
    mode_str   = f"_{req.threshold_mode}" if req.threshold_mode != "mean" else ""
    years_str  = f"_y{'_'.join(str(y) for y in sorted(req.years))}" if req.years else ""
    filename   = f"{loader_str}_{slug}{window_str}{mode_str}{years_str}_all.json"
    out_path   = Path("results") / filename
    out_path.parent.mkdir(parents=True, exist_ok=True)
    return out_path

# ---------------------------------------------------------------------------
# DP metadata / label enrichment
# ---------------------------------------------------------------------------

DP_GROUPS = ("DP02", "DP03", "DP04", "DP05")


def _is_year_key(key: Any) -> bool:
    return str(key).isdigit()


def _is_dp_feature(feature: Any) -> bool:
    if not isinstance(feature, str):
        return False

    if "_" not in feature:
        return False

    group = feature.split("_", 1)[0]
    return group in DP_GROUPS


def _clean_dp_label(label: str) -> str:
    """
    Convert Census-style labels into shorter chart labels.

    Example:
        "Estimate!!SEX AND AGE!!Total population!!Female"
        -> "Female"

    The original label is preserved separately as raw_label.
    """
    label = (label or "").strip()

    if "!!" in label:
        parts = [p.strip() for p in label.split("!!") if p.strip()]
        if parts:
            return parts[-1]

    label = label.replace("Estimate: ", "").replace("Percent: ", "")
    return label.strip()


def _label_parts(raw_label: str) -> List[str]:
    """Split an ACS label into hierarchy parts, dropping the Estimate/Percent prefix."""
    parts = [p.strip() for p in (raw_label or "").split("!!") if p.strip()]
    if parts and parts[0].lower() in ("estimate", "percent"):
        parts = parts[1:]
    return parts


def _section_name(section: str) -> str:
    """'INCOME AND BENEFITS (IN 2016 INFLATION-ADJUSTED DOLLARS)' -> 'Income and benefits'."""
    import re

    section = re.sub(r"\s*\([^)]*\)", "", section).strip()
    return section.capitalize() if section.isupper() else section


def _display_label(parts: List[str], depth: int) -> str:
    """Section name plus the last `depth` hierarchy levels below it."""
    if not parts:
        return ""
    if len(parts) == 1:
        return _section_name(parts[0])
    tail = parts[1:][-depth:]
    return f"{_section_name(parts[0])}: " + " › ".join(tail)


# Long, frequently repeated ACS phrases -> chart-friendly short forms.
# Longest phrases first so a longer match wins over its prefix.
_LABEL_ABBREVIATIONS = (
    ("Selected monthly owner costs as a percentage of household income", "Owner costs as % of income"),
    ("Gross rent as a percentage of household income", "Rent as % of income"),
    ("Race alone or in combination with one or more other races", "Race alone or in combination"),
    ("Selected monthly owner costs", "Owner costs"),
    ("Hispanic or latino and race", "Hispanic/Latino & race"),
    ("Income and benefits", "Income"),
)


def _abbreviate(label: str) -> str:
    for long_form, short_form in _LABEL_ABBREVIATIONS:
        label = label.replace(long_form, short_form)
    return label


def _build_display_labels(raw_by_feature: Dict[str, str]) -> Dict[str, str]:
    """
    Build chart labels that are unique within one report year.

    The ACS repeats leaf names across sections (e.g. "Total housing units"
    appears under HOUSING OCCUPANCY, UNITS IN STRUCTURE, YEAR STRUCTURE BUILT...),
    so leaf-only labels collide. Start with "Section: leaf", add parent levels
    only where labels still collide, and fall back to the variable code.
    """
    parts_by = {f: _label_parts(raw) for f, raw in raw_by_feature.items()}
    depth = {f: 1 for f in parts_by}

    for _ in range(6):
        labels = {f: _display_label(parts_by[f], depth[f]) for f in parts_by}
        groups: Dict[str, List[str]] = {}
        for f, lab in labels.items():
            groups.setdefault(lab, []).append(f)

        changed = False
        for members in groups.values():
            if len(members) < 2:
                continue
            for f in members:
                if depth[f] < len(parts_by[f]) - 1:
                    depth[f] += 1
                    changed = True
        if not changed:
            break

    labels = {f: _abbreviate(_display_label(parts_by[f], depth[f])) or f for f in parts_by}
    counts: Dict[str, int] = {}
    for lab in labels.values():
        counts[lab] = counts.get(lab, 0) + 1

    return {
        f: (f"{lab} ({f.rstrip('E')})" if counts[lab] > 1 else lab)
        for f, lab in labels.items()
    }


def _collect_features_for_year(year_data: Dict[str, Any]) -> set[str]:
    """
    Find every DP feature code used anywhere inside one year block.
    """
    features: set[str] = set()

    rounds = year_data.get("rounds", [])
    if isinstance(rounds, list):
        for round_data in rounds:
            if not isinstance(round_data, dict):
                continue

            for list_key in ("top_columns", "all_columns"):
                cols = round_data.get(list_key, [])
                if not isinstance(cols, list):
                    continue

                for col in cols:
                    if not isinstance(col, dict):
                        continue

                    feature = col.get("feature")
                    if _is_dp_feature(feature):
                        features.add(feature)

            for list_key in ("top_subfamilies",):
                subfamilies = round_data.get(list_key, [])
                if not isinstance(subfamilies, list):
                    continue

                # No feature codes usually live here, but this keeps the function
                # future-proof if subfamily entries later include representative features.
                for item in subfamilies:
                    if not isinstance(item, dict):
                        continue

                    feature = item.get("feature")
                    if _is_dp_feature(feature):
                        features.add(feature)

    return features


def _load_dp_metadata_for_year(acs_year: int) -> Dict[str, Any]:
    """
    Load year-correct ACS DP metadata.
    """

    # Preferred production path.
    try:
        from data_server.acs_loader import get_all_metadata
        from phase_1.dp_labels import metadata_to_dp_mapping

        metadata = get_all_metadata(acs_year)
        return metadata_to_dp_mapping(metadata)

    except Exception as e:
        print(f"WARNING: could not load live DP metadata for {acs_year}: {e}")

    # Fallback: no metadata.
    return {}


def _build_report_metadata_for_year(
    acs_year: int,
    year_data: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Build a compact metadata block containing only features that appear
    in this result year.
    """
    features = _collect_features_for_year(year_data)
    if not features:
        return {}

    full_mapping = _load_dp_metadata_for_year(acs_year)
    if not full_mapping:
        return {}

    compact: Dict[str, Any] = {}

    raw_by_feature = {
        f: str(full_mapping[f].get("label", "")).strip()
        for f in features
        if isinstance(full_mapping.get(f), dict)
    }
    display_labels = _build_display_labels(raw_by_feature)

    for feature in sorted(features):
        meta = full_mapping.get(feature)
        if not isinstance(meta, dict):
            continue

        raw_label = raw_by_feature[feature]

        compact[feature] = {
            "label": display_labels.get(feature) or _clean_dp_label(raw_label) or feature,
            "raw_label": raw_label,
            "concept": str(meta.get("concept", "")).strip(),
            "group": str(meta.get("group", feature.split("_", 1)[0])).strip(),
        }

    return compact


def _patch_column_labels(
    year_data: Dict[str, Any],
    year_metadata: Dict[str, Any],
) -> None:
    """
    Mutate year_data in-place so each column object carries label/group/concept
    when metadata is available.
    """
    rounds = year_data.get("rounds", [])
    if not isinstance(rounds, list):
        return

    for round_data in rounds:
        if not isinstance(round_data, dict):
            continue

        for list_key in ("top_columns", "all_columns"):
            cols = round_data.get(list_key, [])
            if not isinstance(cols, list):
                continue

            for col in cols:
                if not isinstance(col, dict):
                    continue

                feature = col.get("feature")
                if not _is_dp_feature(feature):
                    continue

                meta = year_metadata.get(feature)
                if not isinstance(meta, dict):
                    continue

                # Preserve existing label if it is already more specific,
                # but generally normalize to clean chart label.
                col["label"] = meta.get("label", col.get("label", feature))
                col["raw_label"] = meta.get("raw_label", col.get("raw_label", ""))
                col["concept"] = meta.get("concept", col.get("concept", ""))
                col["group"] = meta.get("group", col.get("group", feature.split("_", 1)[0]))


def _enrich_result_with_dp_metadata(result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Add year-specific DP metadata to the returned screening report.

    Adds:
        result["report"]["dp_metadata_by_year"][year][feature] = metadata

    Also patches:
        result[year]["rounds"][...]["all_columns"][...]["label"]
        result[year]["rounds"][...]["top_columns"][...]["label"]
    """
    if not isinstance(result, dict):
        return result

    report_block = result.setdefault("report", {})
    if not isinstance(report_block, dict):
        report_block = {}
        result["report"] = report_block

    dp_metadata_by_year: Dict[str, Any] = {}

    for key, year_data in result.items():
        if key == "report":
            continue

        if not _is_year_key(key):
            continue

        if not isinstance(year_data, dict):
            continue

        config = year_data.get("config", {})
        if isinstance(config, dict) and config.get("acs_year") is not None:
            acs_year = int(config["acs_year"])
        else:
            acs_year = int(key)

        year_metadata = _build_report_metadata_for_year(acs_year, year_data)

        if year_metadata:
            dp_metadata_by_year[str(acs_year)] = year_metadata
            year_data["dp_metadata"] = year_metadata
            _patch_column_labels(year_data, year_metadata)

    report_block["dp_metadata_by_year"] = dp_metadata_by_year

    return result

# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/template")
def template():
    """Full loader param shapes for building the Godot UI."""
    if not _TEMPLATE:
        raise HTTPException(
            status_code=503,
            detail="Template not yet built — server still starting."
        )
    return _TEMPLATE


@app.post("/run")
def run_screening(req: RunRequest):
    """
    Run iterative screening and return the full multi-year report.
    Holds the connection open until complete.
    Also writes result to disk under results/.
    """
    if req.threshold_mode not in ("mean", "q3"):
        raise HTTPException(
            status_code=422,
            detail=f"threshold_mode must be 'mean' or 'q3', got {req.threshold_mode!r}."
        )

    try:
        # Path B — user-uploaded data
        if req.user_data is not None:
            from phase_1.user_data_loader import UserDataLoader

            ud = req.user_data
            loader_instance = UserDataLoader(
                rows=ud.rows,
                fips_col=ud.fips_col,
                target_col=ud.target_col,
                year_col=ud.year_col,
                single_year=ud.single_year,
            )
            result = run(
                loader_name="",
                cancer_site="",
                metric="",
                n_rounds=req.rounds,
                top_n=req.top_n,
                r_floor_fdr=req.r_floor_fdr,
                r_floor_convergence=req.r_floor_convergence,
                threshold_mode=req.threshold_mode,
                window=1,
                stale_rounds=req.stale_rounds,
                years=req.years,
                loader_instance=loader_instance,
            )

        # Path A — built-in loader
        else:
            if req.loader not in _VALID_LOADERS:
                raise HTTPException(
                    status_code=422,
                    detail=f"Unknown loader {req.loader!r}. Valid: {sorted(_VALID_LOADERS)}."
                )
            metric = req.metric or _DEFAULT_METRIC[req.loader]
            result = run(
                loader_name=req.loader,
                cancer_site=req.site,
                metric=metric,
                n_rounds=req.rounds,
                top_n=req.top_n,
                r_floor_fdr=req.r_floor_fdr,
                r_floor_convergence=req.r_floor_convergence,
                threshold_mode=req.threshold_mode,
                window=req.window,
                stale_rounds=req.stale_rounds,
                years=req.years,
            )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    # Enrich DP features with year-correct metadata / labels before returning
    # to Godot and before writing the result JSON to disk.
    result = _enrich_result_with_dp_metadata(result)

    # Write to disk — non-fatal
    out_path = _derive_output_path(req)
    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"WARNING: could not write result to {out_path}: {e}")

    return result


# ---------------------------------------------------------------------------
# Dev entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("screening_api:app", host="0.0.0.0", port=11650, reload=False)