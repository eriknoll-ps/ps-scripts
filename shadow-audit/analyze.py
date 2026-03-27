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

import os
import traceback
import warnings
import requests
import pandas as pd

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

    # Load usage report - read ICCID as string to preserve all 20 digits
    print("  Loading OEM Historical Usage data...", end="", flush=True)
    usage_df = pd.read_excel(usage_file, dtype={"ICCID": str})
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
    Load OEM data and vehicles, identify TIG devices with active data, find Not Communicating vehicles with data usage.
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

        input("\nPress Enter to continue...")

    except FileNotFoundError as e:
        print(f"\nERROR: Missing file - {e}")
        print("  Ensure 'OEM Historical Usage.xlsx', 'devices.csv', and 'vehicles.csv'")
        print("  exist in shadow-audit/")
        return
    except Exception as e:
        print(f"\nERROR loading data: {e}")
        traceback.print_exc()
        return


if __name__ == "__main__":
    main()