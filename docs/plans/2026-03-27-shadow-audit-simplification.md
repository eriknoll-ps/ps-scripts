# Shadow-Audit Simplification Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Simplify shadow-audit/analyze.py to load OEM Historical Usage, filter to active TIG devices, and report the count—removing all PACCAR API integration.

**Architecture:** Strip away the multi-step PACCAR API workflow (fetch vehicles, filter T521, fetch shadow state). Keep only the `load_active_tig_devices()` function which already implements the complete filtering logic (OEM → data usage > 0 → join devices.csv on ICCID → TIG filter).

**Tech Stack:** Python 3, pandas (existing), no external API calls

---

## Task 1: Remove unused imports and constants

**Files:**
- Modify: `shadow-audit/analyze.py:1-46`

**Step 1: Identify imports to remove**

Review the top of analyze.py. These imports/constants are for PACCAR API:
- `requests` (line 19)
- `time` (line 21)
- `datetime` (line 24)
- `csv` (line 23)
- `json` (line 20)
- BASE_URL, T521_DSN_MIN, T521_DSN_MAX, PAGE_SIZE, REQUEST_DELAY, MAX_RETRIES, RETRY_BACKOFF, OUTPUT_CSV (lines 36-45)

Keep only:
- `os` (for file paths)
- `pandas as pd` (for dataframe operations)

**Step 2: Remove the imports and constants**

Replace lines 19-45 with:

```python
import os
import pandas as pd
```

Remove lines 36-45 entirely (PACCAR config constants).

**Step 3: Verify no other references to removed imports exist**

Search for usage: `grep -n "requests\|json\|csv\|datetime\|time\." shadow-audit/analyze.py`

Expected: No matches (except in comments/docstrings)

**Step 4: Commit**

```bash
git add shadow-audit/analyze.py
git commit -m "refactor: remove PACCAR API imports and constants"
```

---

## Task 2: Remove PACCAR API functions

**Files:**
- Modify: `shadow-audit/analyze.py:51-255` (remove functions)

**Step 1: Remove helper functions**

Delete these complete function definitions:
- `get_headers()` (lines 51-56)
- `prompt_for_new_token()` (lines 58-69)
- `request_with_retry()` (lines 71-103)
- `fetch_not_communicating_vehicles()` (lines 109-172)
- `filter_t521_vehicles()` (lines 189-213)
- `fetch_shadow_state()` (lines 219-233)
- `extract_remote_diagnostics()` (lines 235-254)

Keep:
- `load_active_tig_devices()` (lines 260-288)

**Step 2: Verify file structure**

After deletion, the file should have:
- Imports (os, pandas)
- `load_active_tig_devices()` function
- `main()` function

Run: `grep -n "^def " shadow-audit/analyze.py`
Expected: 2 functions only

**Step 3: Commit**

```bash
git add shadow-audit/analyze.py
git commit -m "refactor: remove PACCAR API functions"
```

---

## Task 3: Simplify main() function

**Files:**
- Modify: `shadow-audit/analyze.py:294-420` (replace entire main function)

**Step 1: Replace main() with simplified version**

Replace the entire `main()` function (lines 294-420) with:

```python
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
```

**Step 2: Verify the main() function is syntactically correct**

Run: `python -m py_compile shadow-audit/analyze.py`
Expected: No output (success)

**Step 3: Commit**

```bash
git add shadow-audit/analyze.py
git commit -m "refactor: simplify main() to load and report TIG count"
```

---

## Task 4: Verify the simplified script works

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
SHADOW AUDIT: TIG Devices with Active Data Usage
============================================================

TIG devices with Cycle-to-date Data Usage > 0: <number>

Press Enter to continue...
```

Then press Enter to complete.

**Step 2: Verify data integrity**

The count should match devices that are:
- In OEM Historical Usage.xlsx with Cycle-to-date Data Usage > 0
- Present in devices.csv with ICCID match
- Have Device Type == "TIG"

(Manual verification: spot-check a few rows from the script's earlier debug output if available, or review the Excel/CSV files)

**Step 3: Commit test confirmation**

```bash
git add shadow-audit/analyze.py
git commit -m "test: verify simplified script runs successfully"
```

---

## Task 5: Update readme.txt

**Files:**
- Modify: `shadow-audit/readme.txt`

**Step 1: Replace readme with simplified instructions**

Replace entire contents with:

```
# Shadow Audit - TIG Device Active Usage Checker

## Purpose
Identifies TIG devices with active cellular data usage by cross-referencing:
- OEM Historical Usage.xlsx (data usage records)
- devices.csv (device registry with ICCID/DSN/type mappings)

## Prerequisites
- Python 3.x
- pandas library: pip install pandas
- openpyxl library: pip install openpyxl

## Files Required
- OEM Historical Usage.xlsx (in shadow-audit/ directory)
- devices.csv (in shadow-audit/ directory)

## Running the Script
```bash
python analyze.py
```

The script will:
1. Load OEM Historical Usage data
2. Filter to units with Cycle-to-date Data Usage > 0
3. Join with devices.csv on ICCID
4. Filter to TIG device type only
5. Display count of matches and prompt to continue

## Output
Console output shows total count of active TIG devices. No files are generated.
```

**Step 2: Verify readme is clear**

Read it back and confirm it's accurate and follows the new workflow.

**Step 3: Commit**

```bash
git add shadow-audit/readme.txt
git commit -m "docs: update readme for simplified shadow-audit script"
```

---

## Summary

**Total changes:**
- Remove 200+ lines of PACCAR API code
- Simplify main() from 126 lines to ~30 lines
- Remove 7 functions (keep 1)
- Keep only essential imports (os, pandas)
- Update documentation

**Result:**
- Lightweight script: ~60 lines of active code
- No external API dependencies
- No auth token required
- Fast execution (pure local data processing)

**Testing approach:**
- Manual execution test (Step 4)
- No unit tests needed—simple procedural script with pandas operations

**Execution next step:**
Choose how to implement: subagent-driven (this session) or parallel session?
