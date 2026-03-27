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
import traceback
import pandas as pd

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

def load_not_communicating_vehicles():
    """
    Load vehicles.csv and filter to Recommendation == 'Not Communicating' (case-insensitive).
    Returns a DataFrame with columns: DSN, Vin, Recommendation
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    vehicles_file = os.path.join(script_dir, "vehicles.csv")

    # Load vehicles - DSN is around column 34, Recommendation is column 8
    vehicles_df = pd.read_csv(vehicles_file, dtype={"DSN": str})
    vehicles_df["DSN"] = vehicles_df["DSN"].str.strip()

    # Filter to Not Communicating status (case-insensitive)
    not_comm_df = vehicles_df[vehicles_df["Recommendation"].str.upper() == "NOT COMMUNICATING"].copy()

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
        tig_df = load_active_tig_devices()
        tig_count = len(tig_df)

        print(f"\nTIG devices with Cycle-to-date Data Usage > 0: {tig_count:,}")

        # Load Not Communicating vehicles and find overlap
        not_comm_df = load_not_communicating_vehicles()
        not_comm_with_data = count_not_communicating_with_data(tig_df, not_comm_df)

        print(f"Not Communicating vehicles with active cellular data: {not_comm_with_data:,}")
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