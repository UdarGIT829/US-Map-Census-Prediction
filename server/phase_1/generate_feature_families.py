#!/usr/bin/env python3
# generate_feature_families.py
#
# Rewired for Phase 1:
#   - build_feature_family_tree() now accepts an optional `metadata` dict
#     (from Phase 0's get_all_metadata()) instead of always reading from disk.
#   - File-path fallback is kept for the transition period and for dev tools
#     that still have dp_mapping.json on disk.
#   - The tree-building logic itself is unchanged.
#   - Must be called per-year when the vintage changes, because 1424 label
#     changes were confirmed between 2019 and 2023.

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

from phase_1.dp_labels import split_dp_var, is_dp_estimate


# Human-readable top-level family names keyed by DP table ID
TABLE_ALIASES = {
    "DP02": "Social Characteristics (DP02)",
    "DP03": "Economic Characteristics (DP03)",
    "DP04": "Housing Characteristics (DP04)",
    "DP05": "Demographic Characteristics (DP05)",
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _extract_label(entry: Any) -> str | None:
    """
    Extract a human-readable label string from a dp_mapping / metadata entry.

    Supports:
      - entry is a plain string
      - entry is a dict with a 'label' field  (Phase 0 get_all_metadata format)
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
    Return the meaningful semantic chunks of an ACS profile label, skipping
    boilerplate prefixes like "Estimate!!", "Percent!!", etc.

    Example:
      "Estimate!!INCOME AND BENEFITS (IN 2020 INFLATION-ADJUSTED DOLLARS)
       !!Total households!!Less than $10,000"
      → ["INCOME AND BENEFITS (IN 2020 INFLATION-ADJUSTED DOLLARS)",
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


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_feature_family_tree(
    dp_mapping_path: str = "dp_mapping.json",
    metadata: Optional[Dict[str, Any]] = None,
    inner_threshold: int = 25,
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Build an in-memory 3-level hierarchy from ACS DP metadata.

    Structure:
    {
      "<TopFamily>": {
        "<SubfamilyRoot>": {
          "columns": ["DP02_0001E", ...],
          "labels":  ["<full ACS label>", ...],
          "inner": {                          # omitted if len(cols) <= inner_threshold
            "<InnerSubfamily>": {
              "columns": [...],
              "labels":  [...]
            },
            ...
          }
        },
        ...
      },
      ...
    }

    Parameters
    ----------
    dp_mapping_path : path to the legacy dp_mapping.json file.
        Used only as a fallback when `metadata` is None.  In production,
        always pass `metadata` from Phase 0's get_all_metadata() so labels
        are vintage-matched.
    metadata : dict from Phase 0 get_all_metadata(year, groups).
        When provided, dp_mapping_path is ignored entirely.
        Shape: { varcode: {"label": ..., "concept": ..., "group": ...} }
    inner_threshold : subfamilies with this many columns or fewer will have
        their "inner" key omitted, keeping the structure flat for small groups.

    Raises
    ------
    FileNotFoundError if metadata is None and dp_mapping_path does not exist.
    ValueError if both metadata and dp_mapping_path produce an empty mapping.
    """
    # --- Source resolution ---
    if metadata is not None:
        dp_map = metadata
    else:
        dp_mapping_file = Path(dp_mapping_path)
        if not dp_mapping_file.exists():
            raise FileNotFoundError(
                f"dp_mapping.json not found at {dp_mapping_file} and no metadata "
                "dict was provided. Pass metadata from Phase 0's get_all_metadata()."
            )
        with dp_mapping_file.open("r", encoding="utf-8") as f:
            dp_map = json.load(f)

    if not dp_map:
        raise ValueError(
            "The metadata source (metadata dict or dp_mapping.json) is empty. "
            "Ensure get_all_metadata() returned data for the requested year."
        )

    # --- Tree construction ---
    # Structure: top_family -> subfamily -> {columns, labels, inner -> {cols, labels}}
    tree: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(
        lambda: defaultdict(
            lambda: {
                "columns": [],
                "labels": [],
                "inner": defaultdict(lambda: {"columns": [], "labels": []}),
            }
        )
    )

    for var_name, entry in dp_map.items():
        if not is_dp_estimate(var_name):
            continue

        parts = split_dp_var(var_name)
        if parts is None:
            continue

        table_id, _, suffix = parts
        if suffix != "E":
            continue  # is_dp_estimate already guards this, belt-and-suspenders

        top_family = TABLE_ALIASES.get(table_id, f"Other ({table_id})")

        raw_label = _extract_label(entry)
        if not raw_label:
            subfamily_root = "Unlabeled"
            semantic_chunks: list[str] = []
        else:
            semantic_chunks = _semantic_chunks(raw_label)
            subfamily_root = semantic_chunks[0] if semantic_chunks else "Unlabeled"

        # Second semantic chunk → inner subfamily (optional)
        inner_name = semantic_chunks[1] if len(semantic_chunks) >= 2 else None

        label_for_storage = raw_label if raw_label else var_name

        # Attach to subfamily
        subfamily = tree[top_family][subfamily_root]
        subfamily["columns"].append(var_name)
        subfamily["labels"].append(label_for_storage)

        # Attach to inner subfamily if present
        if inner_name is not None:
            inner = subfamily["inner"][inner_name]
            inner["columns"].append(var_name)
            inner["labels"].append(label_for_storage)

    # --- Post-process: apply inner_threshold, convert defaultdicts ---
    top_level_dict: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for top_family, subfamilies in tree.items():
        sf_dict: Dict[str, Dict[str, Any]] = {}
        for subfamily_name, payload in subfamilies.items():
            cols = payload["columns"]
            labels = payload["labels"]
            inner_dict = {k: v for k, v in payload["inner"].items()}

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


def generate_feature_families(
    metadata: Dict[str, Any],
    *,
    inner_threshold: int = 25,
    write_path: Optional[str | Path] = None,
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Pipeline-facing entry point.  Always takes a metadata dict from Phase 0.

    This is what Dataset_Builder and phase1_screening should call.
    metadata must come from get_all_metadata(year, groups) — it is
    vintage-matched to the data being analysed.  Passing None raises
    ValueError because ACS labels change between vintages and there is
    no acceptable static fallback.

    Parameters
    ----------
    metadata : Phase 0 get_all_metadata() output (required, not None).
    inner_threshold : passed through to build_feature_family_tree.
    write_path : if provided, the tree is also written to this JSON file.

    Returns
    -------
    The tree dict (always returned in-memory regardless of write_path).
    """
    if metadata is None:
        raise ValueError(
            "metadata=None passed to generate_feature_families. "
            "metadata must be the output of Phase 0's get_all_metadata(year, groups). "
            "ACS variable labels change between vintages — a static fallback file "
            "would silently place columns in the wrong subfamilies for other years."
        )
    tree = build_feature_family_tree(metadata=metadata, inner_threshold=inner_threshold)

    if write_path is not None:
        out_path = Path(write_path)
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(tree, f, indent=2, sort_keys=True)
        print(f"[generate_feature_families] wrote tree to {out_path.resolve()}")

    return tree


# ---------------------------------------------------------------------------
# CLI entry point — for dev use only, not called by the pipeline
# ---------------------------------------------------------------------------

def main():
    """
    Fallback CLI: generate from dp_mapping.json on disk.
    In production, the pipeline calls generate_feature_families(metadata=...)
    directly — this CLI is only useful during development when you want to
    inspect the tree without a running Phase 0 server.
    """
    print(
        "[WARN] Running generate_feature_families.py from CLI uses the static "
        "dp_mapping.json file, which is always 2023-only. In production, call "
        "generate_feature_families(metadata=get_all_metadata(...)) instead."
    )
    tree = build_feature_family_tree("dp_mapping.json", inner_threshold=25)

    out_path = Path("feature_families.json")
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(tree, f, indent=2, sort_keys=True)

    total_cols = sum(
        len(sf["columns"])
        for fam in tree.values()
        for sf in fam.values()
    )
    print(f"Wrote feature family tree to {out_path.resolve()}")
    print(f"Total families: {len(tree)}, total E-columns indexed: {total_cols}")


if __name__ == "__main__":
    main()