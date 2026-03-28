# Shadow-Audit Remote Diagnostics Enable Feature - Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add optional step to enable remote diagnostics for devices where Reported Enabled = False via PlatformScience API.

**Architecture:** Add credential management (save/load/prompt PlatformScience token), API helper functions (POST to enable endpoint with retry logic), CSV export for results, enable loop with progress bar, and optional main() integration with user prompts and device filtering.

**Tech Stack:** Python 3, requests, tqdm, pandas, csv, datetime

---

## Task 1: Add PlatformScience token management functions

**Files:**
- Modify: `shadow-audit/analyze.py` (add after PACCAR token functions, around line 120)

**Step 1: Add token management functions**

Add these functions after the PACCAR token functions (after line 109):

```python
def save_platformscience_token(token: str) -> None:
    """Save PlatformScience API token to local cache file."""
    try:
        with open(".platformscience_token", "w") as f:
            f.write(token.strip())
        os.chmod(".platformscience_token", 0o600)
        print("  Token saved to .platformscience_token")
    except (OSError, IOError) as e:
        print(f"  Warning: Could not save PlatformScience token: {e}")


def load_platformscience_token() -> str:
    """Load saved PlatformScience API token from cache file."""
    try:
        if os.path.exists(".platformscience_token"):
            with open(".platformscience_token", "r") as f:
                token = f.read().strip()
                if token:
                    return token
    except (OSError, IOError) as e:
        print(f"  Warning: Could not load PlatformScience token: {e}")
    return None


def prompt_for_platformscience_token() -> str:
    """Prompt user to paste PlatformScience API token from browser."""
    print("\n  PlatformScience API token required.")
    print("  Get token from: Browser DevTools (F12) > Network tab")
    print("  Find request to: cf-api.mc2.telematicsplatform.io")
    print("  Copy Authorization header value (Bearer {token})")
    token = input("\n  Paste token (or press Enter to abort): ").strip()
    if token:
        # Remove "Bearer " prefix if user included it
        if token.startswith("Bearer "):
            token = token[7:]
        save_platformscience_token(token)
        return token
    return None
```

**Step 2: Verify syntax**

Run: `python -m py_compile shadow-audit/analyze.py`
Expected: No output (success)

**Step 3: Commit**

```bash
git add shadow-audit/analyze.py
git commit -m "feat: add PlatformScience token management functions"
```

---

## Task 2: Add API helper function to enable remote diagnostics

**Files:**
- Modify: `shadow-audit/analyze.py` (add after PlatformScience token functions, around line 150)

**Step 1: Add enable API function**

Add this function after the token functions:

```python
def enable_remote_diagnostics(dsn: str, token: str, max_retries: int = 5) -> tuple:
    """
    Enable remote diagnostics for a device via PlatformScience API.
    Returns (success: bool, reason: str or None)
    success=True means API returned 200
    reason explains why if success=False
    """
    url = "https://cf-api.mc2.telematicsplatform.io/peoplenet/cf-gateway/v1/v2/application/send"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    payload = {
        "deviceId": dsn,
        "destinationTopic": '{"remoteDiagnostics":{"enabled": true}}',
        "payload": "[]",
        "payloadContentType": "application/json"
    }

    for attempt in range(max_retries):
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=10)

            if response.status_code == 200:
                return True, None
            elif response.status_code == 401:
                # Token expired
                return False, "401 Unauthorized"
            elif response.status_code == 404:
                # Device not found
                return False, "404 Device not found"
            elif response.status_code >= 500:
                # Server error, retry with backoff
                if attempt < max_retries - 1:
                    wait_time = 2 ** (attempt + 1)
                    time.sleep(wait_time)
                    continue
                return False, f"{response.status_code} Server error"
            else:
                return False, f"{response.status_code} API error"

        except requests.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** (attempt + 1)
                time.sleep(wait_time)
                continue
            return False, f"Network error: {str(e)}"

    return False, "Max retries exceeded"
```

**Step 2: Verify syntax**

Run: `python -m py_compile shadow-audit/analyze.py`
Expected: No output

**Step 3: Commit**

```bash
git add shadow-audit/analyze.py
git commit -m "feat: add API helper function to enable remote diagnostics"
```

---

## Task 3: Add CSV export for enable results

**Files:**
- Modify: `shadow-audit/analyze.py` (add after enable function, around line 200)

**Step 1: Add CSV export function**

Add this function:

```python
def export_enable_results(results: list, timestamp: str) -> str | None:
    """
    Export enable operation results to CSV.
    Returns the output filename (str) or None if write fails.
    """
    reports_dir = "reports"
    output_filename = f"shadow-audit-enable-{timestamp}.csv"
    output_file = os.path.join(reports_dir, output_filename)

    try:
        with open(output_file, "w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["DSN", "Enable Status", "Reason", "Timestamp"]
            )
            writer.writeheader()

            for result in results:
                writer.writerow({
                    "DSN": result["dsn"],
                    "Enable Status": result["status"],
                    "Reason": result["reason"] or "",
                    "Timestamp": result["timestamp"]
                })

        return output_file
    except (OSError, IOError) as e:
        print(f"  Warning: Could not write CSV file: {e}")
        return None
```

**Step 2: Verify syntax**

Run: `python -m py_compile shadow-audit/analyze.py`
Expected: No output

**Step 3: Commit**

```bash
git add shadow-audit/analyze.py
git commit -m "feat: add CSV export for enable operation results"
```

---

## Task 4: Add function to filter devices with disabled remote diagnostics

**Files:**
- Modify: `shadow-audit/analyze.py` (add after export function, around line 230)

**Step 1: Add filter function**

Add this function:

```python
def get_disabled_devices(results: list) -> list:
    """
    Filter results to devices with reported_enabled == False.
    Returns list of DSNs with disabled remote diagnostics.
    """
    disabled = []
    for result in results:
        if result.get("reported_enabled") is False:
            disabled.append(result)
    return disabled
```

**Step 2: Verify syntax**

Run: `python -m py_compile shadow-audit/analyze.py`
Expected: No output

**Step 3: Commit**

```bash
git add shadow-audit/analyze.py
git commit -m "feat: add function to filter devices with disabled remote diagnostics"
```

---

## Task 5: Add enable loop function with progress tracking

**Files:**
- Modify: `shadow-audit/analyze.py` (add after filter function, around line 245)

**Step 1: Add enable loop function**

Add this function:

```python
def enable_devices_loop(disabled_devices: list, token: str) -> list:
    """
    Enable remote diagnostics for a list of devices.
    Returns list of result dicts with DSN, status, reason, and timestamp.
    """
    results = []
    successful = 0
    failed = 0

    for device in tqdm(disabled_devices, desc="Enabling remote diagnostics"):
        dsn = device["dsn"]
        success, reason = enable_remote_diagnostics(dsn, token)

        result = {
            "dsn": dsn,
            "status": "Success" if success else "Failed",
            "reason": reason,
            "timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        }
        results.append(result)

        if success:
            successful += 1
        else:
            failed += 1

    print(f"\n  Successfully enabled {successful}/{len(disabled_devices)} devices")
    if failed > 0:
        print(f"  Failed to enable {failed} devices (skipped)")

    return results
```

**Step 2: Verify syntax**

Run: `python -m py_compile shadow-audit/analyze.py`
Expected: No output

**Step 3: Commit**

```bash
git add shadow-audit/analyze.py
git commit -m "feat: add enable loop function with progress bar"
```

---

## Task 6: Integrate enable feature into main() function

**Files:**
- Modify: `shadow-audit/analyze.py` (update main() function)

**Step 1: Update main() to include enable workflow**

Find the main() function and update it. After the section that exports shadow state results and before the final `input()`, add this block inside the `if not_comm_with_data > 0:` block:

```python
            # Optional: Enable remote diagnostics for devices with disabled settings
            disabled_devices = get_disabled_devices(results)
            if disabled_devices:
                enable_prompt = input(f"\nFound {len(disabled_devices)} devices with Reported Enabled = False\nEnable remote diagnostics for these devices? (Y/N): ").strip().upper()

                if enable_prompt == "Y":
                    # Show filtered list
                    print("\nDevices to enable:")
                    for i, device in enumerate(disabled_devices[:10], 1):
                        print(f"  {i}. {device['dsn']}")
                    if len(disabled_devices) > 10:
                        print(f"  ... and {len(disabled_devices) - 10} more")

                    confirm = input(f"\nConfirm enable {len(disabled_devices)} devices? (Y/N): ").strip().upper()

                    if confirm == "Y":
                        # Get PlatformScience token
                        ps_token = load_platformscience_token()
                        if not ps_token:
                            ps_token = prompt_for_platformscience_token()
                            if not ps_token:
                                print("  Aborted: No PlatformScience token provided")
                            else:
                                # Enable devices
                                enable_results = enable_devices_loop(disabled_devices, ps_token)

                                # Export enable results
                                enable_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                                enable_output = export_enable_results(enable_results, enable_timestamp)
                                if enable_output:
                                    print(f"\n  Enable results exported to: {enable_output}")
                        else:
                            # Enable devices with loaded token
                            enable_results = enable_devices_loop(disabled_devices, ps_token)

                            # Export enable results
                            enable_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            enable_output = export_enable_results(enable_results, enable_timestamp)
                            if enable_output:
                                print(f"\n  Enable results exported to: {enable_output}")
```

**Step 2: Verify syntax**

Run: `python -m py_compile shadow-audit/analyze.py`
Expected: No output

**Step 3: Verify imports**

Make sure these are imported at the top (they should be from previous tasks):
- `import requests` (line 39)
- `import time` (line 35)
- `from tqdm import tqdm` (line 41)
- `from datetime import datetime, timezone` (line 38)

**Step 4: Update .gitignore**

Add PlatformScience token to .gitignore (if not already there):

```bash
echo ".platformscience_token" >> .gitignore
```

**Step 5: Commit**

```bash
git add shadow-audit/analyze.py .gitignore
git commit -m "feat: integrate enable feature into main workflow with user prompts"
```

---

## Task 7: Test the complete enable feature

**Files:**
- Test: `shadow-audit/analyze.py` (manual test)

**Step 1: Verify script syntax**

Run: `python -m py_compile shadow-audit/analyze.py`
Expected: No output

**Step 2: Manual workflow test**

Run: `cd shadow-audit && python analyze.py`

Expected workflow:
1. Script loads TIG devices and Not Communicating vehicles
2. Fetches shadow state for 693 devices
3. Exports to CSV in reports directory
4. Prompts: "Found X devices with Reported Enabled = False. Enable remote diagnostics for these devices? (Y/N):"
5. If "N", skip to final prompt
6. If "Y", shows list and asks for confirmation
7. If "Y" on confirmation, prompts for PlatformScience token
8. Shows progress bar and enables devices
9. Exports results to `shadow-audit-enable-YYYYMMDD_HHMMSS.csv`
10. Shows summary with enabled/failed counts

**Step 3: Verify token caching**

Run the script twice:
- First run: should prompt for token
- Second run: should load cached token without prompting

Check: `ls -la .platformscience_token` - file should exist with 0o600 permissions

**Step 4: Verify CSV output**

Check that both CSVs exist:
```bash
ls -la shadow-audit/reports/
```

Expected: `shadow-audit-results-*.csv` and `shadow-audit-enable-*.csv` files

**Step 5: Verify CSV content**

Check enable results CSV:
```bash
head -10 shadow-audit/reports/shadow-audit-enable-*.csv
```

Expected headers and sample data:
```
DSN,Enable Status,Reason,Timestamp
20095139,Success,,2026-03-28T14:32:45Z
20305225,Failed,401 Unauthorized,2026-03-28T14:32:47Z
```

**Step 6: Commit test confirmation**

```bash
git add shadow-audit/analyze.py
git commit -m "test: verify enable feature works end-to-end"
```

---

## Summary

**Total changes:**
- Add 6 new functions (~150 lines)
- Update main() with enable workflow (~40 lines)
- Add PlatformScience token to .gitignore
- Create CSV export with enable results

**Result:**
- Optional enable step after shadow state export
- User reviews filtered list before enabling
- Automatic token caching and refresh on 401
- Progress bar during enable loop
- Results exported to timestamped CSV
- Resilient error handling (skips failed devices, continues)

**Testing approach:**
- Manual end-to-end test (Task 7)
- Verify CSV output format
- Verify token caching
- Verify error handling on API failures
