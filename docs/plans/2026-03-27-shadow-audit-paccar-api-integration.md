# Shadow-Audit PACCAR API Integration Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add PACCAR Shadow API integration to fetch remote diagnostics settings for 693 Not Communicating devices and export to CSV.

**Architecture:** Add auth token management (save/load/refresh), PACCAR API functions, shadow state fetching loop with progress bar, and CSV export. Token handling copied from wireless-audit pattern. Uses tqdm for progress visibility.

**Tech Stack:** Python 3, pandas, requests, tqdm

---

## Task 1: Add auth token management functions

**Files:**
- Modify: `shadow-audit/analyze.py` (add after imports, before constants)

**Step 1: Add token storage functions**

Add these functions after the constants section (around line 42):

```python
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
```

**Step 2: Verify syntax**

Run: `python -m py_compile shadow-audit/analyze.py`
Expected: No output (success)

**Step 3: Commit**

```bash
git add shadow-audit/analyze.py
git commit -m "feat: add PACCAR auth token management functions"
```

---

## Task 2: Add API helper functions

**Files:**
- Modify: `shadow-audit/analyze.py` (add after token functions)

**Step 1: Add API headers and fetch functions**

Add these functions after token functions:

```python
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
```

**Step 2: Verify syntax**

Run: `python -m py_compile shadow-audit/analyze.py`
Expected: No output

**Step 3: Add time import**

At the top of the file with other imports, add:

```python
import time
```

(Check if already imported)

**Step 4: Add requests import**

At the top with other imports:

```python
import requests
```

(Check if already imported)

**Step 5: Commit**

```bash
git add shadow-audit/analyze.py
git commit -m "feat: add PACCAR API helper functions"
```

---

## Task 3: Add CSV export function

**Files:**
- Modify: `shadow-audit/analyze.py` (add after API functions)

**Step 1: Add CSV export function**

Add this function:

```python
def export_shadow_state_results(results: list, timestamp: str) -> str:
    """
    Export shadow state fetch results to CSV.
    Returns the output filename.
    """
    import csv

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
    except IOError as e:
        print(f"  ERROR: Could not write CSV file: {e}")
        return None
```

**Step 2: Verify syntax**

Run: `python -m py_compile shadow-audit/analyze.py`
Expected: No output

**Step 3: Commit**

```bash
git add shadow-audit/analyze.py
git commit -m "feat: add CSV export function for shadow state results"
```

---

## Task 4: Add shadow state fetching loop

**Files:**
- Modify: `shadow-audit/analyze.py` (add after CSV export function)

**Step 1: Add shadow state fetching function**

Add this function:

```python
def fetch_shadow_state_for_devices(matched_df: pd.DataFrame, token: str) -> list:
    """
    Fetch shadow state for matched Not Communicating devices.
    Returns list of results with DSN, remote diagnostics settings, and timestamp.
    """
    from datetime import datetime

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
            "timestamp": datetime.utcnow().isoformat() + "Z"
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
```

**Step 2: Add tqdm import**

Check if tqdm is imported at the top. If not, add:

```python
from tqdm import tqdm
```

**Step 3: Verify syntax**

Run: `python -m py_compile shadow-audit/analyze.py`
Expected: No output

**Step 4: Commit**

```bash
git add shadow-audit/analyze.py
git commit -m "feat: add shadow state fetching loop with progress bar"
```

---

## Task 5: Integrate into main() function

**Files:**
- Modify: `shadow-audit/analyze.py` (modify main function around line 180)

**Step 1: Update main() to include shadow API calls**

Find the main() function and update it. Add the shadow API integration after displaying the Not Communicating count:

```python
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

        # Show first 10 DSNs
        if not_comm_with_data > 0:
            matched = not_comm_df.merge(tig_df[["DSN"]], on="DSN", how="inner")
            print("\nFirst 10 DSNs:")
            for i, dsn in enumerate(matched["DSN"].head(10), 1):
                print(f"  {i}. {dsn}")
            if not_comm_with_data > 10:
                print(f"  ... and {not_comm_with_data - 10} more")

            # [NEW] Fetch shadow state for matched devices
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
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
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
        import traceback
        traceback.print_exc()
        return

    return None
```

**Step 2: Verify imports**

Make sure `datetime` is imported. Add at top if needed:

```python
import datetime
```

**Step 3: Verify syntax**

Run: `python -m py_compile shadow-audit/analyze.py`
Expected: No output

**Step 4: Commit**

```bash
git add shadow-audit/analyze.py
git commit -m "feat: integrate PACCAR shadow API into main workflow"
```

---

## Task 6: Test the complete integration

**Files:**
- Test: `shadow-audit/analyze.py` (manual test)

**Step 1: Run the script**

```bash
cd shadow-audit
python analyze.py
```

Expected output:
```
============================================================
SHADOW AUDIT: Not Communicating Vehicles with Active Data
============================================================

Loading TIG devices with active data usage...
  Loading OEM Historical Usage data... [OK]
  ...
Not Communicating vehicles with active cellular data: 693

First 10 DSNs:
  1. 20095139
  ...

Fetching remote diagnostics settings from PACCAR...
  [Your token will be cached in .paccar_token]

Fetching shadow state...
[████████████████████] 693/693 [100%, completed in ~X minutes]
  Successfully fetched shadow state for XXX/693 devices
  Results exported to: shadow-audit-results-YYYYMMDD_HHMMSS.csv

Press Enter to continue...
```

**Step 2: Verify CSV file**

Check that the CSV file exists and contains correct data:

```bash
head -20 shadow-audit-results-*.csv
```

Expected: CSV with columns DSN, Reported Enabled, Desired Enabled, Fetch Timestamp

**Step 3: Verify token caching**

Check that `.paccar_token` file exists:

```bash
ls -la .paccar_token
```

Expected: File exists with restricted permissions (600)

**Step 4: Test token refresh (optional)**

If token expires during run, script should refresh and retry automatically.

**Step 5: Commit test confirmation**

```bash
git add shadow-audit/analyze.py
git commit -m "test: verify PACCAR API integration works end-to-end"
```

---

## Summary

**Total changes:**
- Add 6 new functions (~200 lines)
- Update main() with API integration (~50 lines)
- Add 3 imports (requests, time, datetime)
- Create CSV output with shadow state results

**Result:**
- Fetches remote diagnostics settings for 693 devices
- Caches and refreshes auth token automatically
- Shows progress during API calls
- Exports results to timestamped CSV

**Testing approach:**
- Manual end-to-end test (Task 6)
- Verify CSV output format
- Verify token caching
- Verify error handling on network issues
