"""
PACCAR Solutions - T521 Not Communicating Device Remote Diagnostics Checker

This script:
1. Fetches all vehicles with Recommendation Status = "Not Communicating"
2. Filters for T521 devices (DSN between 20,000,000 and 30,000,000)
3. For each T521 device, retrieves Shadow State and extracts:
   - reported.remoteDiagnostics.enabled
   - desired.remoteDiagnostics.enabled

SETUP:
- Copy your auth token from the browser:
  1. Open PACCAR Solutions in Chrome
  2. Open DevTools (F12) > Application > Local Storage > https://paccarsolutions.com
  3. Find 'pnet.portal.encodedToken' and copy its value (without surrounding quotes)
  4. Set it as the AUTH_TOKEN constant below, or pass it via environment variable
"""

import os
import pandas as pd

# ─────────────────────────────────────────────
# HEADERS
# ─────────────────────────────────────────────

def get_headers(content_type="application/json"):
    return {
        "X-Auth-Token": AUTH_TOKEN,
        "Accept": "application/json, text/plain, */*",
        "Content-Type": content_type,
    }

def prompt_for_new_token():
    """Prompt the user for a new auth token. Returns True if updated, False if skipped."""
    global AUTH_TOKEN
    print("\n  *** 401 Unauthorized — your auth token has expired. ***")
    print("  To continue, paste a fresh token from browser DevTools:")
    print("    Chrome > F12 > Application > Local Storage > pnet.portal.encodedToken")
    new_token = input("\n  Paste new token (or press Enter to abort): ").strip()
    if new_token:
        AUTH_TOKEN = new_token
        print("  Token updated. Retrying request...\n")
        return True
    return False

def request_with_retry(method, url, retries=MAX_RETRIES, **kwargs):
    """Make an HTTP request with retry + exponential backoff for 5xx errors."""
    for attempt in range(retries + 1):
        # Rebuild headers with current AUTH_TOKEN in case it was refreshed
        if "headers" in kwargs:
            kwargs["headers"]["X-Auth-Token"] = AUTH_TOKEN
        try:
            response = method(url, **kwargs)
            response.raise_for_status()
            return response
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            if status == 401:
                if prompt_for_new_token():
                    if "headers" in kwargs:
                        kwargs["headers"]["X-Auth-Token"] = AUTH_TOKEN
                    continue  # Retry with new token
                raise
            if status >= 500 and attempt < retries:
                wait = RETRY_BACKOFF ** (attempt + 1)
                print(f"\n    Server error ({status}), retrying in {wait}s "
                      f"(attempt {attempt + 1}/{retries})...")
                time.sleep(wait)
                continue
            raise
        except requests.ConnectionError as e:
            if attempt < retries:
                wait = RETRY_BACKOFF ** (attempt + 1)
                print(f"\n    Connection error, retrying in {wait}s "
                      f"(attempt {attempt + 1}/{retries})...")
                time.sleep(wait)
                continue
            raise

# ─────────────────────────────────────────────
# STEP 1: Fetch all "Not Communicating" vehicles
# ─────────────────────────────────────────────

def fetch_not_communicating_vehicles():
    """
    Fetches all vehicles with Recommendation Status = NOT_COMMUNICATING.
    Returns a list of vehicle dicts.
    """
    url = f"{BASE_URL}/v2vehicles"
    all_vehicles = []
    page = 0
    total_hits = None

    print("Fetching Not Communicating vehicles...")

    while True:
        body = {
            "page": str(page),
            "pageSize": str(PAGE_SIZE),
            "version": 2,
            "recommendation.faultGuidance.RecommendedAction": "NOT_COMMUNICATING",
            "basicInfo.tags": [],
            "sort": ["vin"],
            "exclude": [
                "basicInfo.ownershipRecords",
                "dealerKeys",
                "licenseInfo.*",
                "stateInfo.*",
            ],
            "forceEntityClass": True,
            "licenseInfo.disabledOemLicense": False,
        }

        try:
            response = request_with_retry(requests.post, url, json=body, headers=get_headers())
            data = response.json()
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            if status == 401:
                raise  # Auth errors should still abort
            print(f"\n  WARNING: Failed to fetch page {page} after retries ({e}).")
            print(f"  Continuing with {len(all_vehicles)} vehicles fetched so far.")
            break
        except requests.ConnectionError as e:
            print(f"\n  WARNING: Connection failed on page {page} after retries ({e}).")
            print(f"  Continuing with {len(all_vehicles)} vehicles fetched so far.")
            break

        vehicles = data.get("data", [])
        resp_desc = data.get("responseDescription", {})

        if total_hits is None:
            total_hits = resp_desc.get("totalHits", 0)
            print(f"  Total Not Communicating vehicles found: {total_hits}")

        all_vehicles.extend(vehicles)
        print(f"  Page {page}: fetched {len(vehicles)} vehicles "
              f"(cumulative: {len(all_vehicles)}/{total_hits})")

        # Check if we've retrieved all pages
        if len(all_vehicles) >= total_hits or len(vehicles) == 0:
            break

        page += 1

    print(f"\nTotal vehicles retrieved: {len(all_vehicles)}")
    return all_vehicles

# ─────────────────────────────────────────────
# STEP 2: Filter for T521 devices
# ─────────────────────────────────────────────

def extract_dsn(vehicle):
    """Extract the DSN from a vehicle dict. Returns None if not present."""
    try:
        return vehicle["deviceInfo"]["pmgInfo"]["pmgDsn"]
    except (KeyError, TypeError):
        return None

def is_t521(dsn):
    """Returns True if the DSN is in the T521 range."""
    return dsn is not None and T521_DSN_MIN < dsn < T521_DSN_MAX

def filter_t521_vehicles(vehicles):
    """Filter vehicle list to T521 devices only."""
    t521 = []
    no_dsn_count = 0
    non_t521_count = 0

    for v in vehicles:
        dsn = extract_dsn(v)
        if dsn is None:
            no_dsn_count += 1
        elif is_t521(dsn):
            t521.append({
                "dsn": dsn,
                "vin": v.get("vin") or v.get("basicInfo", {}).get("vin", ""),
                "vehicle": v,
            })
        else:
            non_t521_count += 1

    print(f"\nT521 filter results:")
    print(f"  T521 devices (DSN {T521_DSN_MIN:,}–{T521_DSN_MAX:,}): {len(t521)}")
    print(f"  Non-T521 devices: {non_t521_count}")
    print(f"  Vehicles with no DSN: {no_dsn_count}")

    return t521

# ─────────────────────────────────────────────
# STEP 3: Fetch Shadow State for each T521 device
# ─────────────────────────────────────────────

def fetch_shadow_state(dsn):
    """
    Fetches the shadow state (device-config) for a given DSN.
    Returns the parsed JSON or None on error.
    """
    url = f"{BASE_URL}/device-config/device-config/{dsn}"
    try:
        response = request_with_retry(requests.get, url, headers=get_headers())
        return response.json()
    except requests.HTTPError as e:
        print(f"    HTTP error for DSN {dsn}: {e}")
        return None
    except Exception as e:
        print(f"    Error fetching shadow state for DSN {dsn}: {e}")
        return None

def extract_remote_diagnostics(shadow_state):
    """
    Extracts the reported and desired remoteDiagnostics.enabled values.
    Returns (reported_value, desired_value) — each is True/False/None.
    """
    if shadow_state is None:
        return None, None

    reported = shadow_state.get("reported", {}) or {}
    desired = shadow_state.get("desired", {}) or {}

    reported_enabled = (reported
                        .get("remoteDiagnostics", {}) or {}
                        ).get("enabled")

    desired_enabled = (desired
                       .get("remoteDiagnostics", {}) or {}
                       ).get("enabled")

    return reported_enabled, desired_enabled

# ─────────────────────────────────────────────
# STEP 0: Load active TIG devices from OEM Historical Usage
# ─────────────────────────────────────────────

def load_active_tig_devices():
    """
    Load OEM Historical Usage.xlsx, filter to units with Cycle-to-date Data Usage > 0,
    join with devices.csv on ICCID, and filter to Device Type == TIG.
    Returns a DataFrame with columns: ICCID, DSN, Device Type, Cycle-to-date Data Usage
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    usage_file = os.path.join(script_dir, "OEM Historical Usage.xlsx")
    devices_file = os.path.join(script_dir, "devices.csv")

    # Load usage report - read ICCID as string to preserve all 20 digits
    usage_df = pd.read_excel(usage_file, dtype={"ICCID": str})
    usage_df["ICCID"] = usage_df["ICCID"].str.strip()

    # Filter to units with data usage > 0
    active_df = usage_df[usage_df["Cycle-to-date Data Usage"] > 0].copy()

    # Load device map - skipinitialspace handles the ", " CSV formatting
    devices_df = pd.read_csv(devices_file, skipinitialspace=True, dtype={"ICCID": str, "DSN": str})
    devices_df["ICCID"] = devices_df["ICCID"].str.strip()
    devices_df["Device Type"] = devices_df["Device Type"].str.strip()

    # Join on ICCID
    merged_df = active_df.merge(devices_df[["ICCID", "DSN", "Device Type"]], on="ICCID", how="inner")

    # Filter to TIG only
    tig_df = merged_df[merged_df["Device Type"] == "TIG"].copy()

    return tig_df

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    if AUTH_TOKEN == "PASTE_YOUR_TOKEN_HERE":
        print("ERROR: Please set your auth token.")
        print("  Option 1: Set AUTH_TOKEN in the script")
        print("  Option 2: export PACCAR_AUTH_TOKEN='your_token_here'")
        print("\nHow to get your token:")
        print("  1. Open PACCAR Solutions in Chrome")
        print("  2. Open DevTools (F12) > Application > Local Storage > https://paccarsolutions.com")
        print("  3. Find key 'pnet.portal.encodedToken' and copy its value")
        return

    results = []

    # Step 1: Load active TIG devices from OEM Historical Usage
    print("=" * 60)
    print("STEP 1: Load active TIG devices from OEM Historical Usage")
    print("=" * 60)
    try:
        tig_df = load_active_tig_devices()
        print(f"\nTIG devices with Cycle-to-date Data Usage > 0: {len(tig_df)}")
        input("\nPress Enter to continue...")
    except Exception as e:
        print(f"\nERROR loading OEM Historical Usage data: {e}")
        import traceback
        traceback.print_exc()
        return

    # Step 2: Fetch Not Communicating vehicles
    try:
        all_vehicles = fetch_not_communicating_vehicles()
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 401:
            print("\nERROR 401: Auth token is invalid or expired. Please refresh your token.")
        else:
            print(f"\nERROR fetching vehicles: {e}")
        return

    if not all_vehicles:
        print("\nNo vehicles were retrieved. Check your token and network connection.")
        return

    # Step 3: Filter for T521
    t521_vehicles = filter_t521_vehicles(all_vehicles)

    if not t521_vehicles:
        print("\nNo T521 devices found in the Not Communicating list.")
        return

    # Step 4: Fetch shadow state for each T521 device
    print(f"\nFetching Shadow State for {len(t521_vehicles)} T521 devices...")

    for i, item in enumerate(t521_vehicles, 1):
        dsn = item["dsn"]
        vin = item["vin"]

        print(f"  [{i}/{len(t521_vehicles)}] DSN {dsn} (VIN: {vin})", end=" ... ")

        shadow = fetch_shadow_state(dsn)
        reported_enabled, desired_enabled = extract_remote_diagnostics(shadow)

        result = {
            "dsn": dsn,
            "vin": vin,
            "device_details_url": f"https://paccarsolutions.com/#/nav/device/details/{dsn}",
            "reported_remoteDiagnostics_enabled": reported_enabled,
            "desired_remoteDiagnostics_enabled": desired_enabled,
            "shadow_state_fetched": shadow is not None,
        }
        results.append(result)

        print(f"reported={reported_enabled}, desired={desired_enabled}")

        # Respectful delay between requests
        if i < len(t521_vehicles):
            time.sleep(REQUEST_DELAY)

    # ─── Output Results ───────────────────────────────────────

    print(f"\n{'='*60}")
    print(f"RESULTS SUMMARY")
    print(f"{'='*60}")
    print(f"T521 Not Communicating devices processed: {len(results)}")

    # Summary counts
    with_data = [r for r in results if r["shadow_state_fetched"]]
    no_data   = [r for r in results if not r["shadow_state_fetched"]]
    print(f"  Shadow state fetched successfully: {len(with_data)}")
    print(f"  Shadow state unavailable:          {len(no_data)}")

    if with_data:
        rep_true  = sum(1 for r in with_data if r["reported_remoteDiagnostics_enabled"] is True)
        rep_false = sum(1 for r in with_data if r["reported_remoteDiagnostics_enabled"] is False)
        rep_none  = sum(1 for r in with_data if r["reported_remoteDiagnostics_enabled"] is None)
        des_true  = sum(1 for r in with_data if r["desired_remoteDiagnostics_enabled"] is True)
        des_false = sum(1 for r in with_data if r["desired_remoteDiagnostics_enabled"] is False)
        des_none  = sum(1 for r in with_data if r["desired_remoteDiagnostics_enabled"] is None)

        print(f"\n  Reported remoteDiagnostics.enabled:")
        print(f"    True:         {rep_true}")
        print(f"    False:        {rep_false}")
        print(f"    Not set/None: {rep_none}")
        print(f"\n  Desired remoteDiagnostics.enabled:")
        print(f"    True:         {des_true}")
        print(f"    False:        {des_false}")
        print(f"    Not set/None: {des_none}")

    # Write CSV
    if results:
        with open(OUTPUT_CSV, "w", newline="") as f:
            fieldnames = [
                "dsn", "vin", "device_details_url",
                "reported_remoteDiagnostics_enabled",
                "desired_remoteDiagnostics_enabled",
                "shadow_state_fetched",
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        print(f"\nResults written to: {OUTPUT_CSV}")

    # Also write JSON for full detail
    json_output = OUTPUT_CSV.replace(".csv", ".json")
    with open(json_output, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"Full JSON written to:  {json_output}")

    return results


if __name__ == "__main__":
    main()