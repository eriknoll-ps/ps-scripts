# Shadow-Audit Not Communicating Analysis Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add analysis to identify vehicles marked as "Not Communicating" in PACCAR that actually have active cellular data usage.

**Architecture:** Load Not Communicating vehicles from vehicles.csv, left join to the existing 170,424 TIG devices with active data (on DSN), and count matches. This reveals potential monitoring issues where devices send data but don't report status to PACCAR.

**Tech Stack:** Python 3, pandas (existing)

---

## Task 1: Add function to load Not Communicating vehicles

**Files:**
- Modify: `shadow-audit/analyze.py:26-54` (add after `load_active_tig_devices()`)

**Step 1: Write the new function**

Add this function after `load_active_tig_devices()`:

```python
def load_not_communicating_vehicles():
    """
    Load vehicles.csv and filter to Not Communicating status.
    Returns a DataFrame with columns: DSN, Vin, Recommendation
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    vehicles_file = os.path.join(script_dir, "vehicles.csv")

    # Load vehicles - DSN is around column 34, Recommendation is column 8
    vehicles_df = pd.read_csv(vehicles_file, dtype={"DSN": str})
    vehicles_df["DSN"] = vehicles_df["DSN"].str.strip()

    # Filter to Not Communicating status
    not_comm_df = vehicles_df[vehicles_df["Recommendation"] == "Not Communicating"].copy()

    return not_comm_df
```

**Step 2: Verify function is syntactically correct**

Run: `python -m py_compile shadow-audit/analyze.py`
Expected: No output (success)

**Step 3: Commit**

```bash
git add shadow-audit/analyze.py
git commit -m "feat: add function to load not-communicating vehicles"
```

---

## Task 2: Add function to count Not Communicating with data

**Files:**
- Modify: `shadow-audit/analyze.py` (add after `load_not_communicating_vehicles()`)

**Step 1: Write the counting function**

Add this function after `load_not_communicating_vehicles()`:

```python
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
```

**Step 2: Verify syntax**

Run: `python -m py_compile shadow-audit/analyze.py`
Expected: No output (success)

**Step 3: Commit**

```bash
git add shadow-audit/analyze.py
git commit -m "feat: add function to count not-communicating vehicles with data"
```

---

## Task 3: Modify main() to use new functions

**Files:**
- Modify: `shadow-audit/analyze.py:60-87` (replace main function)

**Step 1: Update main() function**

Replace the current `main()` function with:

```python
def main():
    """
    Load OEM Historical Usage, filter to active TIG devices, analyze Not Communicating status.
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
        import traceback
        traceback.print_exc()
        return

    return None
```

**Step 2: Verify syntax**

Run: `python -m py_compile shadow-audit/analyze.py`
Expected: No output (success)

**Step 3: Commit**

```bash
git add shadow-audit/analyze.py
git commit -m "feat: update main() to analyze not-communicating vehicles"
```

---

## Task 4: Test the complete script

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

TIG devices with Cycle-to-date Data Usage > 0: 170424
Not Communicating vehicles with active cellular data: <number>

Press Enter to continue...
```

**Step 2: Verify data**

The script should:
- Load 170,424 TIG devices with active data ✓
- Load Not Communicating vehicles from vehicles.csv ✓
- Count matches (should be > 0) ✓
- Display both counts ✓

**Step 3: Verify no errors**

The script should complete without exceptions. If FileNotFoundError occurs, verify:
- `OEM Historical Usage.xlsx` exists
- `devices.csv` exists
- `vehicles.csv` exists

**Step 4: Commit test confirmation**

```bash
git add shadow-audit/analyze.py
git commit -m "test: verify not-communicating analysis works correctly"
```

---

## Task 5: Update readme.txt documentation

**Files:**
- Modify: `shadow-audit/readme.txt`

**Step 1: Update the readme**

Replace the "Running the Script" section with:

```
## Running the Script
```bash
python analyze.py
```

The script will:
1. Load OEM Historical Usage data
2. Filter to units with Cycle-to-date Data Usage > 0
3. Join with devices.csv on ICCID
4. Filter to TIG device type only
5. Load vehicles.csv and filter to Not Communicating status
6. Join Not Communicating vehicles to TIG devices on DSN
7. Display both counts:
   - TIG devices with active data usage
   - Not Communicating vehicles that have active cellular data
8. Prompt to continue
```

Also update the "Files Required" section to include:

```
## Files Required
- OEM Historical Usage.xlsx (in shadow-audit/ directory)
- devices.csv (in shadow-audit/ directory)
- vehicles.csv (in shadow-audit/ directory) - Not Communicating vehicles from PACCAR
```

And update the "Output" section:

```
## Output
Console output shows:
- Count of TIG devices with active cellular data
- Count of Not Communicating vehicles with active cellular data usage
The second count reveals potential monitoring issues where devices send data but don't report status.
```

**Step 2: Verify readme is clear**

Read it back and confirm both counts are explained.

**Step 3: Commit**

```bash
git add shadow-audit/readme.txt
git commit -m "docs: update readme for not-communicating analysis"
```

---

## Summary

**Total changes:**
- Add 2 new functions (~30 lines)
- Update main() to use new functions and display additional count
- Update documentation

**Result:**
- Identifies vehicles sending data but marked as Not Communicating
- Reveals potential monitoring/communication issues
- All changes are additive (no breaking changes)

**Testing approach:**
- Manual execution test (Task 4)
- No unit tests needed—simple procedural script with pandas operations
