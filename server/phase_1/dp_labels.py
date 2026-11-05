# dp_labels.py
#
# Rewired for Phase 1:
#   - metadata_to_dp_mapping() converts Phase 0 get_all_metadata() output
#     into the same dict shape that human_label() / make_readable_view() expect.
#   - load_dp_mapping() is kept for the transition period (and for tests that
#     still have dp_mapping.json on disk) but should not be called in the
#     production pipeline once Phase 1 is fully wired.
#
# Phase 0 get_all_metadata() returns:
#   { "DP05_0001E": {"label": "...", "concept": "...", "group": "DP05"}, ... }
#
# dp_mapping.json (old static file) stores the same shape, so the conversion
# is a no-op — we just validate and return a copy.  The function exists so
# callers have an explicit, named seam between Phase 0 output and dp_labels.

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any, Optional

import re

DP_GROUPS = ("DP02", "DP03", "DP04", "DP05")
_dp_pattern = re.compile(rf"^({'|'.join(DP_GROUPS)})_(\d{{4}})([A-Z]+)$")


# ---------------------------------------------------------------------------
# Variable-code helpers  (pure regex — no file I/O)
# ---------------------------------------------------------------------------

def split_dp_var(col: str):
    """
    Parse a DP variable code into (group, number, suffix).

    Returns a 3-tuple of strings, or None if the column doesn't match.

    Examples:
        split_dp_var("DP05_0001E")  -> ("DP05", "0001", "E")
        split_dp_var("DP03_0062PE") -> ("DP03", "0062", "PE")
        split_dp_var("NAME")        -> None
    """
    m = _dp_pattern.match(col)
    return m.groups() if m else None


def is_dp_estimate(col: str) -> bool:
    """True for E-suffix variables only (raw count estimates, not percents)."""
    parts = split_dp_var(col)
    if parts is None:
        return False
    _, _, suffix = parts
    return suffix == "E"


def is_dp_percent(col: str) -> bool:
    """True for PE-suffix variables (percent estimates)."""
    parts = split_dp_var(col)
    if parts is None:
        return False
    _, _, suffix = parts
    return suffix == "PE"


def is_dp_analysis_var(col: str) -> bool:
    """
    True for variables that should be kept in the feature matrix.
    That means E (estimate) or PE (percent estimate) — not margins or annotations.
    """
    parts = split_dp_var(col)
    if parts is None:
        return False
    _, _, suffix = parts
    return suffix in ("E", "PE")


def is_dp_margin(col: str) -> bool:
    """True for margin-of-error variables (M or PM suffix)."""
    parts = split_dp_var(col)
    if parts is None:
        return False
    _, _, suffix = parts
    return suffix in ("M", "PM")


# ---------------------------------------------------------------------------
# Metadata loading
# ---------------------------------------------------------------------------

def metadata_to_dp_mapping(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert Phase 0 get_all_metadata() output to the dp_mapping dict shape.

    Phase 0 returns:
        {
            "DP05_0001E": {"label": "Estimate!!SEX AND AGE!!Total population",
                           "concept": "ACS DEMOGRAPHIC AND HOUSING ESTIMATES",
                           "group": "DP05"},
            ...
        }

    dp_mapping.json (legacy) uses the same shape, so this function is
    essentially a validated copy.  It exists as an explicit named seam so that:
      - callers don't need to know the internal shape
      - we can adapt here if Phase 0's output format ever changes
      - tests can mock it cleanly

    Raises ValueError if metadata is None or empty.
    """
    if not metadata:
        raise ValueError(
            "metadata_to_dp_mapping received an empty or None metadata dict. "
            "Ensure get_all_metadata() returned data for the requested year."
        )
    # Shallow copy so we don't mutate the caller's dict
    return dict(metadata)


def load_dp_mapping(path: str | Path = "dp_mapping.json") -> Dict[str, Any]:
    """
    Load dp_mapping from a static JSON file on disk.

    TRANSITION NOTE: This function is kept for backward compatibility while
    the pipeline is being rewired to use Phase 0's live metadata.  Once
    Dataset_Builder and Dataset_Provider are fully wired to pass a metadata
    dict from get_all_metadata(), this function should not be called in the
    production pipeline.

    The flat file dp_mapping.json is always 2023-only and will produce
    silently wrong labels for any other vintage year.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(
            f"DP mapping file not found at {path}. "
            "In production, pass a metadata dict from Phase 0's "
            "get_all_metadata() instead of reading from disk. "
            "Run build_dp_mapping.py to regenerate the file if needed for dev."
        )
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Label helpers  (accept the dp_mapping dict, no file I/O)
# ---------------------------------------------------------------------------

def human_label(
    col_name: str,
    mapping: Dict[str, Any],
    *,
    include_concept: bool = False,
) -> str:
    """
    Turn a DP variable code into a human-readable label string.

    mapping — either the output of metadata_to_dp_mapping() (from Phase 0)
              or the legacy load_dp_mapping() dict — both have the same shape.

    If the column is not in the mapping, the original code is returned as-is.
    """
    meta: Optional[dict] = mapping.get(col_name)
    if not meta:
        return col_name

    label = meta.get("label", "").strip()
    concept = meta.get("concept", "").strip()

    if include_concept and concept:
        return f"{label} [{concept}]"
    return label or col_name


def make_readable_view(X, dp_map: Dict[str, Any]):
    """
    Return a copy of DataFrame X with columns renamed to human-readable labels.

    Useful for display/debugging; do not use the renamed DataFrame for any
    computation that expects DP variable code column names.
    """
    new_cols = {col: human_label(col, dp_map) for col in X.columns}
    return X.rename(columns=new_cols)