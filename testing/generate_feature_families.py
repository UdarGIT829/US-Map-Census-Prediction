#!/usr/bin/env python3
"""
Generate feature_families.json from dp_mapping.json with 3-level hierarchy:

TopFamily (by DP table)
  └─ Subfamily (first semantic label, e.g. "INCOME AND BENEFITS ...")
       ├─ columns: [all columns in this subfamily]
       ├─ labels:  [human-readable labels in same order as columns]
       └─ inner: {  # optional
            InnerSubfamily: {
              "columns": [...],
              "labels":  [...],
            },
            ...
          }
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List

from dp_labels import split_dp_var, is_dp_estimate


# Map DP table IDs to human-ish top-level families
TABLE_ALIASES = {
    "DP02": "Social Characteristics (DP02)",
    "DP03": "Economic Characteristics (DP03)",
    "DP04": "Housing Characteristics (DP04)",
    "DP05": "Demographic Characteristics (DP05)",
}


def _extract_label(entry: Any) -> str | None:
    """
    Try to get a human-readable label string from a dp_mapping entry.

    Supports:
      - entry is a plain string
      - entry is a dict with a 'label' field
    """
    if entry is None:
        return None
    if isinstance(entry, str):
        return entry
    if isinstance(entry, dict):
        label = entry.get("label")
        if isinstance(label, str):
            return label
    return None


def _semantic_chunks(label: str) -> List[str]:
    """
    Return 'semantic' chunks from an ACS profile label, skipping boilerplate like
    'Estimate', 'Percent', etc.

    Example label:
      "Estimate!!INCOME AND BENEFITS (IN 2020 INFLATION-ADJUSTED DOLLARS)!!Total households!!Less than $10,000"

    Returns:
      ["INCOME AND BENEFITS (IN 2020 INFLATION-ADJUSTED DOLLARS)",
       "Total households",
       "Less than $10,000"]
    """
    raw_parts = [p.strip() for p in label.split("!!") if p.strip()]
    if not raw_parts:
        return []

    SKIP_PREFIXES = (
        "Estimate",
        "Percent",
        "Margin of Error",
        "Percent Margin of Error",
    )

    semantic: list[str] = []
    for p in raw_parts:
        base = p.split(":")[0].strip()
        if any(base.startswith(s) for s in SKIP_PREFIXES):
            continue
        semantic.append(p)

    return semantic


def _label_root(label: str) -> str:
    """
    Subfamily root = first semantic chunk.

    If we somehow strip everything, we fall back to the first raw chunk.
    """
    semantic = _semantic_chunks(label)
    if semantic:
        return semantic[0]

    # Fallback: just take the first raw non-empty segment
    raw_parts = [p.strip() for p in label.split("!!") if p.strip()]
    return raw_parts[0] if raw_parts else "Unlabeled"


def build_feature_family_tree(
    dp_mapping_path: str = "dp_mapping.json",
    inner_threshold: int = 25,
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Build an in-memory tree:

    {
      "<TopFamily>": {
        "<SubfamilyRoot>": {
          "columns": [ "DP02_0001E", ... ],     # all cols in this subfamily
          "labels":  [ "<full ACS label>", ... ],# human-readable labels
          "inner": {                            # optional
            "<InnerSubfamily>": {
              "columns": [ "DP02_0001E", ... ],
              "labels":  [ "<full ACS label>", ... ]
            },
            ...
          }
        },
        ...
      },
      ...
    }

    inner_threshold: minimum number of columns a subfamily must have before we
                     keep the 'inner' breakdown. If a subfamily has <= threshold
                     columns, we drop its "inner" key to keep the structure simple.
    """
    dp_mapping_file = Path(dp_mapping_path)
    if not dp_mapping_file.exists():
        raise FileNotFoundError(f"dp_mapping.json not found at {dp_mapping_file}")

    with dp_mapping_file.open("r", encoding="utf-8") as f:
        dp_map = json.load(f)

    # Data structure:
    # top_family -> subfamily -> {
    #     "columns": [...],
    #     "labels":  [...],
    #     "inner": { inner_name -> { "columns": [...], "labels": [...] } }
    # }
    tree: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(
        lambda: defaultdict(
            lambda: {
                "columns": [],
                "labels": [],
                "inner": defaultdict(
                    lambda: {"columns": [], "labels": []}
                ),
            }
        )
    )

    for var_name, entry in dp_map.items():
        # Only DP estimate variables (e.g. DP02_xxxxE)
        if not is_dp_estimate(var_name):
            continue

        parts = split_dp_var(var_name)
        if parts is None:
            continue

        table_id, _, suffix = parts
        if suffix != "E":
            continue

        top_family = TABLE_ALIASES.get(table_id, f"Other ({table_id})")

        raw_label = _extract_label(entry)
        if not raw_label:
            subfamily_root = "Unlabeled"
            semantic_chunks: list[str] = []
        else:
            semantic_chunks = _semantic_chunks(raw_label)
            subfamily_root = semantic_chunks[0] if semantic_chunks else "Unlabeled"

        # Optional inner subfamily: second semantic chunk, if present
        inner_name = None
        if len(semantic_chunks) >= 2:
            inner_name = semantic_chunks[1]

        # Fallback for unlabeled vars: just use var_name as the "label"
        label_for_storage = raw_label if raw_label else var_name

        # Always attach to the subfamily's main list
        subfamily = tree[top_family][subfamily_root]
        subfamily["columns"].append(var_name)
        subfamily["labels"].append(label_for_storage)

        # Also attach to inner subfamily if we have one
        if inner_name is not None:
            inner = subfamily["inner"][inner_name]
            inner["columns"].append(var_name)
            inner["labels"].append(label_for_storage)

    # Post-process: apply inner_threshold & convert defaultdicts to plain dicts
    top_level_dict: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for top_family, subfamilies in tree.items():
        sf_dict: Dict[str, Dict[str, Any]] = {}
        for subfamily_name, payload in subfamilies.items():
            cols = payload["columns"]
            labels = payload["labels"]
            inner_map = payload["inner"]

            # Convert inner_map from defaultdict to dict
            inner_dict = {k: v for k, v in inner_map.items()}

            # If subfamily is small, drop 'inner' to avoid over-nesting
            if len(cols) <= inner_threshold:
                sf_dict[subfamily_name] = {
                    "columns": cols,
                    "labels": labels,
                }
            else:
                sf_dict[subfamily_name] = {
                    "columns": cols,
                    "labels": labels,
                    "inner": inner_dict,
                }

        top_level_dict[top_family] = sf_dict

    return top_level_dict


def main():
    tree = build_feature_family_tree("dp_mapping.json", inner_threshold=25)

    out_path = Path("feature_families2.json")
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(tree, f, indent=2, sort_keys=True)

    print(f"Wrote feature family tree to {out_path.resolve()}")


if __name__ == "__main__":
    main()
