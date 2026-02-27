#!/usr/bin/env python3
"""
build_dp_mapping.py

Fetch ACS5 Profile (DP02–DP05) variable metadata from Census API
and write a JSON mapping:

{
  "DP02_0001E": {
    "label": "...",
    "concept": "...",
    "group": "DP02"
  },
  ...
}
"""

import json
import requests

GROUPS = ["DP02", "DP03", "DP04", "DP05"]
ACS_YEAR = "2023"  # change if you need a different vintage
BASE_URL = "https://api.census.gov/data/{year}/acs/acs5/profile/groups/{group}.json"

OUT_PATH = "dp_mapping.json"


def main():
    mapping = {}

    for group in GROUPS:
        url = BASE_URL.format(year=ACS_YEAR, group=group)
        print(f"Fetching {group} from {url} ...")
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        vars_dict = data.get("variables", {})
        for name, meta in vars_dict.items():
            # We only care about DPxx_ variables here, not the "for" and "in" etc.
            if not name.startswith(tuple(GROUPS)):
                continue

            label = meta.get("label", "").strip()
            concept = meta.get("concept", "").strip()

            mapping[name] = {
                "label": label,
                "concept": concept,
                "group": group,
            }

    print(f"Collected {len(mapping)} DP variables. Writing to {OUT_PATH} ...")
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(mapping, f, indent=2, ensure_ascii=False)

    print("Done.")


if __name__ == "__main__":
    main()
