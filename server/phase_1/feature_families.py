from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple, Iterable, Optional, Any


@dataclass(frozen=True)
class FamilyPath:
    """Represents the hierarchical location of a single feature."""
    top_family: str
    subfamily: str
    inner_subfamily: Optional[str] = None


class FeatureFamilies:
    """
    Wrapper around feature_families.json providing:

    - family tree access
    - column → FamilyPath lookup (with optional inner_subfamily)
    - iteration helpers
    """

    def __init__(self, tree: Dict[str, Dict[str, Dict[str, Any]]]):
        self._tree = tree
        self._col_index: Dict[str, FamilyPath] = self._build_col_index(tree)

    @staticmethod
    def _build_col_index(
        tree: Dict[str, Dict[str, Dict[str, Any]]]
    ) -> Dict[str, FamilyPath]:
        """
        Build a reverse index:

            DP02_0001E -> FamilyPath(top_family=..., subfamily=..., inner_subfamily=...)

        Strategy:
          1. First index inner subfamilies (if present).
          2. Then index plain subfamily columns for any that didn't get an inner mapping.
        """
        index: Dict[str, FamilyPath] = {}

        # Pass 1: inner subfamilies (most specific)
        for top_family, subfamilies in tree.items():
            for subfamily_name, payload in subfamilies.items():
                inner = payload.get("inner") or {}
                for inner_name, inner_payload in inner.items():
                    cols = inner_payload.get("columns", [])
                    for col in cols:
                        index[col] = FamilyPath(
                            top_family=top_family,
                            subfamily=subfamily_name,
                            inner_subfamily=inner_name,
                        )

        # Pass 2: direct subfamily columns (only if not already mapped)
        for top_family, subfamilies in tree.items():
            for subfamily_name, payload in subfamilies.items():
                cols = payload.get("columns", [])
                for col in cols:
                    if col not in index:
                        index[col] = FamilyPath(
                            top_family=top_family,
                            subfamily=subfamily_name,
                            inner_subfamily=None,
                        )

        return index

    @classmethod
    def from_json(cls, path: str | Path = "feature_families.json") -> "FeatureFamilies":
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"feature_families.json not found at {p}")

        with p.open("r", encoding="utf-8") as f:
            tree = json.load(f)

        if not isinstance(tree, dict):
            raise ValueError("feature_families.json must contain a JSON object at the root")

        return cls(tree=tree)

    # --- Public API ---

    @property
    def tree(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """Raw hierarchical structure."""
        return self._tree

    @property
    def col_index(self) -> Dict[str, FamilyPath]:
        """Map from DP variable name to its FamilyPath."""
        return self._col_index

    def families(self) -> Iterable[str]:
        """All top-level family names."""
        return self._tree.keys()

    def subfamilies(self, top_family: str) -> Iterable[str]:
        """Subfamilies under a given top-level family."""
        return self._tree.get(top_family, {}).keys()

    def inner_subfamilies(self, top_family: str, subfamily: str) -> Iterable[str]:
        """Inner subfamilies under a given (top_family, subfamily), if any."""
        payload = self._tree.get(top_family, {}).get(subfamily, {})
        inner = payload.get("inner") or {}
        return inner.keys()

    def columns_in_subfamily(self, top_family: str, subfamily: str) -> List[str]:
        """All columns for a given (top_family, subfamily), regardless of inner grouping."""
        payload = self._tree.get(top_family, {}).get(subfamily, {})
        return payload.get("columns", []) or []

    def columns_in_inner_subfamily(
        self,
        top_family: str,
        subfamily: str,
        inner_subfamily: str,
    ) -> List[str]:
        """Columns for a specific inner subfamily, if defined."""
        payload = self._tree.get(top_family, {}).get(subfamily, {})
        inner = payload.get("inner") or {}
        return inner.get(inner_subfamily, {}).get("columns", []) or []

    def path_for_column(self, col: str) -> Optional[FamilyPath]:
        """Look up the FamilyPath for a given DP feature column."""
        return self._col_index.get(col)

    def columns_for_family(self, top_family: str) -> List[str]:
        """All columns belonging to a given top-level family (all its subfamilies)."""
        cols: List[str] = []
        for subfamily in self.subfamilies(top_family):
            cols.extend(self.columns_in_subfamily(top_family, subfamily))
        return cols

    def filter_known_columns(self, cols: Iterable[str]) -> List[str]:
        """
        Given an arbitrary list of columns (e.g., dataset['X'].columns),
        return only those that appear in the family tree.
        """
        return [c for c in cols if c in self._col_index]
