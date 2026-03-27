"""
SHADOW AUDIT - Not Communicating Devices with Active Data Analysis

This script identifies vehicles marked as "Not Communicating" in PACCAR that
actually have active cellular data usage, revealing potential monitoring issues.

The script performs these steps:
1. Load OEM Historical Usage data and filter to devices with data usage > 0
2. Join with devices.csv on ICCID and filter to TIG device type
3. Load vehicles.csv and filter to "Not Communicating" status
4. Left join Not Communicating vehicles to TIG devices on DSN
5. Count and display matches

SETUP:
- Ensure these files exist in the shadow-audit/ directory:
  - OEM Historical Usage.xlsx
  - devices.csv
  - vehicles.csv (Not Communicating vehicles from PACCAR)
- Install pandas: pip install pandas
- Install openpyxl: pip install openpyxl

USAGE:
```bash
python analyze.py
```

OUTPUT:
- TIG devices with Cycle-to-date Data Usage > 0 (count)
- Not Communicating vehicles with active cellular data (count)
The second metric reveals devices sending data but not communicating status to PACCAR.
"""

import csv
import os
import time
import traceback
import warnings
from datetime import datetime, timezone
import requests
import pandas as pd
from tqdm import tqdm

# Suppress openpyxl stylesheet warning
warnings.filterwarnings('ignore', message='Workbook contains no default style')
# Suppress pandas mixed dtype warning
warnings.filterwarnings('ignore', message='Columns.*have mixed types')


# ─────────────────────────────────────────────
# PACCAR Auth Token Management
# ─────────────────────────────────────────────

def save_paccar_token(token: str) -> None:
    """Save PACCAR bearer token to local cache file."""
    try:
        with open(".paccar_token", "w") as f:
            f.write(token.strip())
        os.chmod(".paccar_token", 0o600)
        print("  Token saved to .paccar_token")
    except (OSError, IOError) as e:
        print(f"  Warning: Could not save PACCAR token: {e}")


def load_paccar_token() -> str:
    """Load saved PACCAR bearer token from cache file."""
    try:
        if os.path.exists(".paccar_token"):
            with open(".paccar_token", "r") as f:
                token = f.read().strip()
                if token:
                    return token
    except (OSError, IOError) as e:
        print(f"  Warning: Could not load PACCAR token: {e}")
    return None


def prompt_for_paccar_token() -> str:
    """Prompt user to paste PACCAR auth token from browser."""
    print("\n  PACCAR auth token required.")
    print("  Get token from: Chrome > DevTools (F12) > Application > Local Storage > https://paccarsolutions.com")
    print("  Find key: pnet.portal.encodedToken")
    token = input("\n  Paste token (or press Enter to abort): ").strip()
    if token:
        save_paccar_token(token)
        return token
    return None


def refresh_paccar_token(current_token: str) -> str:
    """Attempt to refresh expired PACCAR bearer token."""
    try:
        url = "https://security-gateway-rp.platform.fleethealth.io/refreshToken"
        headers = {
            "Authorization": f"Bearer {current_token}",
            "X-Auth-Token": current_token,
            "X-OEM": "paccar",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }
        payload = {"encodedToken": current_token}

        response = requests.post(url, json=payload, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            new_token = data.get("encodedToken") or data.get("token")
            if new_token:
                save_paccar_token(new_token)
                print("  Token refreshed successfully")
                return new_token
    except Exception as e:
        print(f"  Warning: Token refresh failed: {e}")

    return None


# ─────────────────────────────────────────────
# PACCAR API Helper Functions
# ─────────────────────────────────────────────

def get_headers(token: str) -> dict:
    """Build API headers with auth token."""
    return {
        "X-Auth-Token": token,
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
    }


def fetch_shadow_state(dsn: str, token: str, max_retries: int = 5) -> dict:
    """
    Fetch shadow state for a device from PACCAR API.
    Returns JSON response or None on failure.
    """
    url = f"https://security-gateway-rp.platform.fleethealth.io/device-config/device-config/{dsn}"

    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=get_headers(token), timeout=10)

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                # Token expired, attempt refresh
                new_token = refresh_paccar_token(token)
                if new_token:
                    # Retry with new token
                    return fetch_shadow_state(dsn, new_token, max_retries=1)
                return None
            elif response.status_code == 404:
                # Device not found
                return None
            elif response.status_code >= 500:
                # Server error, retry with backoff
                if attempt < max_retries - 1:
                    wait_time = 2 ** (attempt + 1)
                    time.sleep(wait_time)
                    continue
            else:
                return None

        except requests.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** (attempt + 1)
                time.sleep(wait_time)
                continue
            return None

    return None


def extract_remote_diagnostics(shadow_state: dict) -> tuple:
    """
    Extract remote diagnostics enabled status from shadow state.
    Returns (reported_enabled, desired_enabled) - each is True/False/None
    """
    if not shadow_state:
        return None, None

    try:
        reported = shadow_state.get("reported", {}) or {}
        desired = shadow_state.get("desired", {}) or {}

        reported_enabled = reported.get("remoteDiagnostics", {}).get("enabled")
        desired_enabled = desired.get("remoteDiagnostics", {}).get("enabled")

        return reported_enabled, desired_enabled
    except (KeyError, TypeError, AttributeError):
        return None, None


def export_shadow_state_results(results: list, timestamp: str) -> str | None:
    """
    Export shadow state fetch results to CSV.
    Returns the output filename (str) or None if write fails.
    """
    output_file = f"shadow-audit-results-{timestamp}.csv"

    try:
        with open(output_file, "w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["DSN", "Reported Enabled", "Desired Enabled", "Fetch Timestamp"]
            )
            writer.writeheader()

            for result in results:
                writer.writerow({
                    "DSN": result["dsn"],
                    "Reported Enabled": result["reported_enabled"],
                    "Desired Enabled": result["desired_enabled"],
                    "Fetch Timestamp": result["timestamp"]
                })

        return output_file
    except (OSError, IOError) as e:
        print(f"  Warning: Could not write CSV file: {e}")
        return None


def fetch_shadow_state_for_devices(matched_df: pd.DataFrame, token: str) -> list:
    """
    Fetch shadow state for matched Not Communicating devices.
    Returns list of results with DSN, remote diagnostics settings, and timestamp.
    """
    results = []
    successful = 0
    failed = 0

    for dsn in tqdm(matched_df["DSN"], desc="Fetching shadow state"):
        shadow_state = fetch_shadow_state(dsn, token)
        reported_enabled, desired_enabled = extract_remote_diagnostics(shadow_state)

        result = {
            "dsn": dsn,
            "reported_enabled": reported_enabled,
            "desired_enabled": desired_enabled,
            "timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        }
        results.append(result)

        if shadow_state is not None:
            successful += 1
        else:
            failed += 1

    print(f"\n  Successfully fetched shadow state for {successful}/{len(matched_df)} devices")
    if failed > 0:
        print(f"  Failed to fetch {failed} devices (skipped)")

    return results


# Constants
SEPARATOR_WIDTH = 60

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

    # Load usage report - read only required columns (ICCID and data usage)
    print("  Loading OEM Historical Usage data...", end="", flush=True)
    usage_df = pd.read_excel(
        usage_file,
        usecols=["ICCID", "Cycle-to-date Data Usage"],
        dtype={"ICCID": str}
    )
    usage_df["ICCID"] = usage_df["ICCID"].str.strip()
    print(" [OK]")

    # Filter to units with data usage > 0
    print("  Filtering to active devices...", end="", flush=True)
    active_df = usage_df[usage_df["Cycle-to-date Data Usage"] > 0].copy()
    print(" [OK]")

    # Load device map - skipinitialspace handles the ", " CSV formatting
    print("  Loading device registry...", end="", flush=True)
    devices_df = pd.read_csv(devices_file, skipinitialspace=True, dtype={"ICCID": str, "DSN": str})
    devices_df["ICCID"] = devices_df["ICCID"].str.strip()
    devices_df["Device Type"] = devices_df["Device Type"].str.strip()
    print(" [OK]")

    # Join on ICCID
    print("  Merging on ICCID...", end="", flush=True)
    merged_df = active_df.merge(devices_df[["ICCID", "DSN", "Device Type"]], on="ICCID", how="inner")
    print(" [OK]")

    # Filter to TIG only
    print("  Filtering to TIG devices...", end="", flush=True)
    tig_df = merged_df[merged_df["Device Type"] == "TIG"].copy()
    print(" [OK]")

    return tig_df

def load_not_communicating_vehicles():
    """
    Load vehicles.csv and filter to Recommendation == 'Not Communicating' (case-insensitive).
    Returns a DataFrame with columns: DSN, Vin, Recommendation
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    vehicles_file = os.path.join(script_dir, "vehicles.csv")

    # Load vehicles - DSN is around column 34, Recommendation is column 8
    print("  Loading vehicle status data...", end="", flush=True)
    vehicles_df = pd.read_csv(vehicles_file, dtype={"DSN": str})
    vehicles_df["DSN"] = vehicles_df["DSN"].str.strip()
    print(" [OK]")

    # Filter to Not Communicating status (case-insensitive)
    print("  Filtering to Not Communicating vehicles...", end="", flush=True)
    not_comm_df = vehicles_df[vehicles_df["Recommendation"].str.upper() == "NOT COMMUNICATING"].copy()
    print(" [OK]")

    return not_comm_df

def count_not_communicating_with_data(tig_df, not_comm_df):
    """
    Left join Not Communicating vehicles to TIG devices on DSN.
    Count how many Not Communicating vehicles have active data usage.
    Returns the count.
    """
    if not_comm_df.empty:
        return 0

    # Left join: Not Communicating vehicles joined to TIG data
    merged = not_comm_df.merge(
        tig_df[["DSN"]],
        on="DSN",
        how="left",
        indicator=True
    )

    # Count matches (both sides, meaning in both datasets)
    count = (merged["_merge"] == "both").sum()

    return count

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    """
    Load OEM data and vehicles, identify TIG devices with active data, fetch remote diagnostics settings.
    """
    print("=" * 60)
    print("SHADOW AUDIT: Not Communicating Vehicles with Active Data")
    print("=" * 60)

    try:
        # Load and filter TIG devices with active data
        print("\nLoading TIG devices with active data usage...")
        tig_df = load_active_tig_devices()
        tig_count = len(tig_df)

        print(f"\nTIG devices with Cycle-to-date Data Usage > 0: {tig_count:,}")

        # Load Not Communicating vehicles and find overlap
        print("\nAnalyzing Not Communicating vehicles...")
        not_comm_df = load_not_communicating_vehicles()
        print("  Joining datasets...", end="", flush=True)
        not_comm_with_data = count_not_communicating_with_data(tig_df, not_comm_df)
        print(" [OK]")

        print(f"Not Communicating vehicles with active cellular data: {not_comm_with_data:,}")

        # Show first 10 DSNs of Not Communicating vehicles with active data
        if not_comm_with_data > 0:
            matched = not_comm_df.merge(tig_df[["DSN"]], on="DSN", how="inner")
            print("\nFirst 10 DSNs:")
            for i, dsn in enumerate(matched["DSN"].head(10), 1):
                print(f"  {i}. {dsn}")
            if not_comm_with_data > 10:
                print(f"  ... and {not_comm_with_data - 10} more")

            # Fetch shadow state for matched devices
            print("\nFetching remote diagnostics settings from PACCAR...")

            # Load or prompt for auth token
            token = load_paccar_token()
            if not token:
                token = prompt_for_paccar_token()
                if not token:
                    print("  Aborted: No auth token provided")
                    return

            # Fetch shadow state for all matched devices
            results = fetch_shadow_state_for_devices(matched, token)

            # Export to CSV
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = export_shadow_state_results(results, timestamp)
            if output_file:
                print(f"\n  Results exported to: {output_file}")

        input("\nPress Enter to continue...")

    except FileNotFoundError as e:
        print(f"\nERROR: Missing file - {e}")
        print("  Ensure 'OEM Historical Usage.xlsx', 'devices.csv', and 'vehicles.csv'")
        print("  exist in shadow-audit/")
        return
    except Exception as e:
        print(f"\nERROR: {e}")
        traceback.print_exc()
        return

    return None


if __name__ == "__main__":
    main()