import requests
import csv

# ============================
# CONFIG
# ============================
YEAR = "2022"
DATASET = "acs/acs5/profile"
API_KEY = "YOUR_CENSUS_API_KEY"  # <- consider using an env var in real use
OUTPUT_FILE = "states_dp.csv"

BASE = f"https://api.census.gov/data/{YEAR}/{DATASET}"

# Census state FIPS codes (50 states + DC)
STATE_FIPS = {
    "01": "Alabama",
    "02": "Alaska",
    "04": "Arizona",
    "05": "Arkansas",
    "06": "California",
    "08": "Colorado",
    "09": "Connecticut",
    "10": "Delaware",
    "11": "District of Columbia",
    "12": "Florida",
    "13": "Georgia",
    "15": "Hawaii",
    "16": "Idaho",
    "17": "Illinois",
    "18": "Indiana",
    "19": "Iowa",
    "20": "Kansas",
    "21": "Kentucky",
    "22": "Louisiana",
    "23": "Maine",
    "24": "Maryland",
    "25": "Massachusetts",
    "26": "Michigan",
    "27": "Minnesota",
    "28": "Mississippi",
    "29": "Missouri",
    "30": "Montana",
    "31": "Nebraska",
    "32": "Nevada",
    "33": "New Hampshire",
    "34": "New Jersey",
    "35": "New Mexico",
    "36": "New York",
    "37": "North Carolina",
    "38": "North Dakota",
    "39": "Ohio",
    "40": "Oklahoma",
    "41": "Oregon",
    "42": "Pennsylvania",
    "44": "Rhode Island",
    "45": "South Carolina",
    "46": "South Dakota",
    "47": "Tennessee",
    "48": "Texas",
    "49": "Utah",
    "50": "Vermont",
    "51": "Virginia",
    "53": "Washington",
    "54": "West Virginia",
    "55": "Wisconsin",
    "56": "Wyoming",
}

# Max variables per request (stay under Census limit)
VARS_PER_REQUEST = 40


# ============================
# STEP 1: FETCH ALL DP VARIABLES + LABELS
# ============================

def fetch_dp_variables_and_labels():
    url = f"{BASE}/variables.json"
    resp = requests.get(url)
    resp.raise_for_status()
    variables = resp.json()["variables"]

    dp_vars = []
    dp_labels = {}

    for name, meta in variables.items():
        if name.startswith("DP") and name.endswith("E"):
            dp_vars.append(name)
            # Use label; fall back to empty string if missing
            label = meta.get("label", "")
            # Clean up any newlines so the CSV stays tidy
            label = label.replace("\n", " ")
            dp_labels[name] = label

    dp_vars = sorted(dp_vars)
    # ensure labels are aligned with sorted vars
    dp_labels = {v: dp_labels.get(v, "") for v in dp_vars}

    print(f"Discovered {len(dp_vars)} DP estimate variables.")
    return dp_vars, dp_labels


# ============================
# STEP 2: HELPER TO CHUNK A LIST
# ============================

def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]


# ============================
# STEP 3: FETCH DATA FOR ONE STATE
# ============================

def fetch_state_row(state_fips, state_name, dp_vars):
    """
    Returns a dict: { "STATE": state_name, "DPxx_...E": value, ... }
    """
    row = {"STATE": state_name}

    for var_chunk in chunked(dp_vars, VARS_PER_REQUEST):
        params = {
            "get": "NAME," + ",".join(var_chunk),
            "for": f"state:{state_fips}",
        }
        if API_KEY:
            params["key"] = API_KEY

        resp = requests.get(BASE, params=params)
        resp.raise_for_status()
        data = resp.json()

        header = data[0]
        values = data[1]

        chunk_dict = dict(zip(header, values))

        for v in var_chunk:
            row[v] = chunk_dict.get(v, "")

    return row


# ============================
# STEP 4: MAIN PIPELINE (STREAMING TO CSV)
# ============================

def main():
    dp_vars, dp_labels = fetch_dp_variables_and_labels()

    # Header row 2: variable codes (what you already had)
    header_codes = ["STATE"] + dp_vars

    # Header row 1: human-readable descriptions
    header_descriptions = ["State Name"] + [dp_labels[v] for v in dp_vars]

    total_states = len(STATE_FIPS)
    print(f"Fetching data for {total_states} states (one at a time)...")

    with open(OUTPUT_FILE, "w", newline="") as f:
        # First, use a plain csv.writer to write the two header rows
        raw_writer = csv.writer(f)
        raw_writer.writerow(header_descriptions)  # description row
        raw_writer.writerow(header_codes)         # code row

        # Then use DictWriter for the data rows (no writeheader() call)
        writer = csv.DictWriter(f, fieldnames=header_codes)

        for idx, (fips, name) in enumerate(STATE_FIPS.items(), start=1):
            print(f"[{idx}/{total_states}] {name} (state:{fips})")
            try:
                row = fetch_state_row(fips, name, dp_vars)
            except Exception as e:
                print(f"  !! Error for {name}: {e}")
                # still write a partial/empty row so structure is consistent
                row = {"STATE": name}
            writer.writerow(row)
            f.flush()  # ensure it's on disk after each state

    print(f"\nDone (or interrupted). Data so far is in {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
