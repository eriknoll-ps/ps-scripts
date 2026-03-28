# Shadow-Audit VinDiscovery Command Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add vinDiscovery command execution to shadow-audit script to request VIN information from all 693 Not Communicating devices.

**Architecture:** Add three functions (single command sender, loop with progress bar, CSV export) and integrate into main() workflow after shadow state fetch. No retry logic—skip and continue on failure. Rate limit: 0.2 sec delay between requests.

**Tech Stack:** Python 3, requests, tqdm, csv, datetime

---

## Task 1: Add send_vin_discovery_command() function

**Files:**
- Modify: `shadow-audit/analyze.py` (add after export_shadow_state_results, around line 500)

**Step 1: Add command sender function**

Add this function after the export functions (around line 500):

```python
def send_vin_discovery_command(dsn: str, token: str, delay: float = 0.2) -> tuple[bool, str]:
    """
    Send vinDiscovery command to a device via PACCAR API.
    Returns (success: bool, result: str)
    result contains response.result on success, error message on failure
    """
    if not dsn or not token:
        return False, "Missing required parameter"

    url = "https://security-gateway-rp.platform.fleethealth.io/transmit/command/synchronous"
    headers = {
        "X-Auth-Token": token,
        "X-OEM": "paccar",
        "Content-Type": "application/json"
    }
    payload = {
        "deviceIds": [dsn],
        "deviceType": "yogi",
        "isTestMessage": False,
        "data": {
            "command_id": None,
            "command": "vinDiscovery",
            "parameters": {}
        }
    }

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)

        if response.status_code == 200:
            data = response.json()
            if data.get("success"):
                result = data.get("response", {}).get("result", "OK")
                return True, result
            else:
                result = data.get("response", {}).get("result", "Command failed")
                return False, result
        else:
            return False, f"{response.status_code} error"

    except requests.RequestException as e:
        return False, f"Network error: {str(e)}"

    finally:
        # Apply rate limit delay
        time.sleep(delay)

    return False, "Unknown error"
```

**Step 2: Verify syntax**

Run: `python -m py_compile shadow-audit/analyze.py`
Expected: No output (success)

**Step 3: Commit**

```bash
git add shadow-audit/analyze.py
git commit -m "feat: add send_vin_discovery_command function for single device command"
```

---

## Task 2: Add send_vin_discovery_loop() function

**Files:**
- Modify: `shadow-audit/analyze.py` (add after send_vin_discovery_command, around line 545)

**Step 1: Add command loop function**

Add this function after send_vin_discovery_command:

```python
def send_vin_discovery_loop(devices: list, token: str) -> list:
    """
    Send vinDiscovery command to all devices.
    Returns list of result dicts with DSN, success, result, and timestamp.
    """
    results = []
    successful = 0
    failed = 0

    for device in tqdm(devices, desc="Sending vinDiscovery commands"):
        dsn = device.get("dsn")
        if not dsn:
            continue

        success, result = send_vin_discovery_command(dsn, token)

        result_dict = {
            "dsn": dsn,
            "success": "Success" if success else "Failed",
            "result": result if not success else "",
            "timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        }
        results.append(result_dict)

        if success:
            successful += 1
        else:
            failed += 1

    print(f"\n  Successfully sent {successful}/{len(devices)} commands")
    if failed > 0:
        print(f"  Failed to send {failed} commands (skipped)")

    return results
```

**Step 2: Verify syntax**

Run: `python -m py_compile shadow-audit/analyze.py`
Expected: No output

**Step 3: Commit**

```bash
git add shadow-audit/analyze.py
git commit -m "feat: add send_vin_discovery_loop function with progress bar and rate limiting"
```

---

## Task 3: Add export_vin_discovery_results() function

**Files:**
- Modify: `shadow-audit/analyze.py` (add after send_vin_discovery_loop, around line 575)

**Step 1: Add export function**

Add this function after send_vin_discovery_loop:

```python
def export_vin_discovery_results(results: list, timestamp: str) -> str | None:
    """
    Export vinDiscovery command results to CSV.
    Returns the output filename (str) or None if write fails.
    """
    reports_dir = "reports"
    output_filename = f"shadow-audit-vin-discovery-{timestamp}.csv"
    output_file = os.path.join(reports_dir, output_filename)

    try:
        os.makedirs(reports_dir, exist_ok=True)

        with open(output_file, "w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["DSN", "Command", "Success", "Response Result", "Timestamp"]
            )
            writer.writeheader()

            for result in results:
                writer.writerow({
                    "DSN": result["dsn"],
                    "Command": "vinDiscovery",
                    "Success": result["success"],
                    "Response Result": result["result"],
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
git commit -m "feat: add export_vin_discovery_results function for CSV export"
```

---

## Task 4: Integrate into main() function

**Files:**
- Modify: `shadow-audit/analyze.py` (update main function around line 680-720)

**Step 1: Add vinDiscovery step to main()**

Find the section in main() that exports shadow state results (look for "Results exported to:"). After that section and BEFORE the enable prompt section, add this code block:

```python
            # [NEW] Send vinDiscovery commands to discover VINs
            print("\nSending vinDiscovery commands to discover device VINs...")
            vin_results = send_vin_discovery_loop(matched, token)

            # Export vinDiscovery results
            vin_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            vin_output = export_vin_discovery_results(vin_results, vin_timestamp)
            if vin_output:
                print(f"  VinDiscovery results exported to: {vin_output}")
```

**Location detail:** This code should be inserted AFTER the shadow state export (after the line `print(f"\\n  Results exported to: {output_file}")`) and BEFORE the optional enable prompt section (BEFORE the line `if disabled_devices:`).

**Step 2: Verify syntax**

Run: `python -m py_compile shadow-audit/analyze.py`
Expected: No output

**Step 3: Commit**

```bash
git add shadow-audit/analyze.py
git commit -m "feat: integrate vinDiscovery command execution into main workflow"
```

---

## Task 5: Test the complete vinDiscovery feature

**Files:**
- Test: `shadow-audit/analyze.py` (manual integration test)

**Step 1: Run the script with test data**

Run: `cd shadow-audit && python analyze.py`

Expected output should include:
1. Load TIG devices count
2. Not Communicating vehicles count (693)
3. Shadow state fetch progress bar
4. Shadow state export confirmation
5. **[NEW] VinDiscovery commands progress bar**
6. **[NEW] Summary: "Successfully sent X/693 commands"**
7. **[NEW] VinDiscovery results exported to: shadow-audit-vin-discovery-YYYYMMDD_HHMMSS.csv**
8. Enable prompt (or exit if user chooses not to enable)

**Step 2: Verify CSV output**

Check that the vinDiscovery CSV was created:
```bash
ls -lh shadow-audit/reports/shadow-audit-vin-discovery-*.csv
```

Expected: File exists with recent timestamp

**Step 3: Verify CSV content**

Check the header and first few rows:
```bash
head -5 shadow-audit/reports/shadow-audit-vin-discovery-*.csv
```

Expected output:
```
DSN,Command,Success,Response Result,Timestamp
20013435,vinDiscovery,Success,OK,2026-03-28T14:32:45Z
20013436,vinDiscovery,Failed,Network error: [Errno 11001] getaddrinfo failed,2026-03-28T14:32:46Z
```

**Step 4: Verify rate limiting**

Note the execution time. With 693 devices and 0.2 second delay:
- Expected time: ~2-3 minutes (693 * 0.2 seconds ≈ 139 seconds)
- Actual time will vary based on network speed

The progress bar should show steady progress, not stuck.

**Step 5: Verify error handling**

If any requests fail (network error, invalid response), they should:
- Be skipped and continue
- Show in failed count: "Failed to send X commands (skipped)"
- Have error details in CSV "Response Result" column

**Step 6: Commit test confirmation**

```bash
git add shadow-audit/analyze.py
git commit -m "test: verify vinDiscovery feature works end-to-end"
```

---

## Summary

**Total changes:**
- Add 3 new functions (~100 lines)
- Update main() with vinDiscovery integration (~10 lines)
- No new imports required (all exist: requests, tqdm, csv, datetime, time)

**Result:**
- VinDiscovery commands sent to all 693 Not Communicating devices
- Rate limiting applied (0.2 sec delay between requests)
- Results exported to timestamped CSV
- Integrated into workflow after shadow state fetch
- No retry logic—skip and continue on any failure

**Testing approach:**
- Manual end-to-end test (Task 5)
- Verify CSV output format and content
- Verify rate limiting effectiveness
- Verify error handling on network failures
