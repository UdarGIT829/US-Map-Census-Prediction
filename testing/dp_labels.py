# dp_labels.py

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any, Optional

import re

DP_GROUPS = ("DP02", "DP03", "DP04", "DP05")
_dp_pattern = re.compile(rf"^({'|'.join(DP_GROUPS)})_(\d{{4}})([A-Z]+)$")

def split_dp_var(col: str):
    m = _dp_pattern.match(col)
    return m.groups() if m else None

def is_dp_estimate(col: str) -> bool:
    parts = split_dp_var(col)
    if parts is None:
        return False
    _, _, suffix = parts
    return suffix == "E"


def is_dp_margin(col: str) -> bool:
    parts = split_dp_var(col)
    if parts is None:
        return False
    _, _, suffix = parts
    return suffix == "M" or suffix == "PM" 


def load_dp_mapping(path: str | Path = "dp_mapping.json") -> Dict[str, Any]:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(
            f"DP mapping file not found at {path}. "
            "Run build_dp_mapping.py first."
        )
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def human_label(
    col_name: str,
    mapping: Dict[str, Any],
    *,
    include_concept: bool = False
) -> str:
    """
    Turn 'DP02_0001E' into a human-readable label string.

    If the column is not in the DP mapping, just return the original name.
    """
    meta: Optional[dict] = mapping.get(col_name)
    if not meta:
        return col_name

    label = meta.get("label", "").strip()
    concept = meta.get("concept", "").strip()

    if include_concept and concept:
        return f"{label} [{concept}]"
    return label or col_name

def make_readable_view(X, dp_map):
    new_cols = {
        col: human_label(col, dp_map)
        for col in X.columns
    }
    return X.rename(columns=new_cols)

