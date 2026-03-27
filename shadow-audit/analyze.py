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
    """
    Load OEM Historical Usage, filter to active TIG devices, report count.
    """
    print("=" * 60)
    print("SHADOW AUDIT: TIG Devices with Active Data Usage")
    print("=" * 60)

    try:
        tig_df = load_active_tig_devices()
        count = len(tig_df)

        print(f"\nTIG devices with Cycle-to-date Data Usage > 0: {count}")
        input("\nPress Enter to continue...")

    except FileNotFoundError as e:
        print(f"\nERROR: Missing file - {e}")
        print("  Ensure 'OEM Historical Usage.xlsx' and 'devices.csv' exist in shadow-audit/")
        return
    except Exception as e:
        print(f"\nERROR loading data: {e}")
        import traceback
        traceback.print_exc()
        return


if __name__ == "__main__":
    main()