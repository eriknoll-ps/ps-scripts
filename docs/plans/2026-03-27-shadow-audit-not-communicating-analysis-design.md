# Shadow-Audit Not Communicating Analysis Design

**Date:** 2026-03-27
**Status:** Approved

## Overview

Extend the shadow-audit script to identify vehicles marked as "Not Communicating" in PACCAR that actually have active cellular data usage. This reveals potential monitoring or communication issues where devices are actively using data but not reporting status to PACCAR.

## Current Workflow

The existing script performs:
1. Load OEM Historical Usage.xlsx
2. Filter to Cycle-to-date Data Usage > 0 (active devices)
3. Join with devices.csv on ICCID
4. Filter to Device Type == "TIG"
5. Display count of 170,424 TIG devices with active data usage

## New Workflow

Add two additional steps after the existing TIG filtering:

1. **Load vehicles.csv from PACCAR**
   - File contains all vehicles with status indicators
   - Key column: Recommendation (contains "Not Communicating" status)
   - Join key: DSN

2. **Filter to Not Communicating vehicles**
   - Keep only vehicles with Recommendation == "Not Communicating"

3. **Left join to TIG dataset on DSN**
   - Start with Not Communicating list (primary)
   - Left join to TIG devices (has active data usage)
   - Count matches = Not Communicating vehicles with active data

4. **Report final count**
   - Display count of Not Communicating TIG devices with active usage
   - This indicates devices sending data but not communicating status to PACCAR

## Data Flow

```
┌──────────────────────────────┐
│ OEM Historical Usage.xlsx    │
│ Filter: Data Usage > 0       │
└──────────────┬───────────────┘
               │ (307,878 active devices)
               ▼
        ┌─────────────┐
        │ devices.csv │ Join on ICCID
        └─────────────┘
               │ (292,521 matched)
               ▼
    Filter: Device Type == "TIG"
               │ (170,424 TIG devices)
               ▼
         tig_df created
               │
               │
        ┌──────▼──────────────────┐
        │ vehicles.csv from PACCAR│
        │ Filter: Not Communicating│
        └──────┬──────────────────┘
               │ (some count)
               ▼
    Left join to tig_df on DSN
               │
               ▼
    Count matches (Not Comm + active data)
               │
               ▼
    Display count and prompt
```

## Implementation Details

### Function: `load_not_communicating_vehicles()`
- Load vehicles.csv
- Filter Recommendation == "Not Communicating"
- Return DataFrame with DSN and relevant columns
- Handle missing/null values gracefully

### Function: `count_not_communicating_with_data(tig_df, not_comm_df)`
- Left join not_comm_df to tig_df on DSN
- Count non-null matches (devices in both datasets)
- Return count

### Main Script Flow
1. Keep existing TIG loading (steps 1-5)
2. Load Not Communicating vehicles
3. Count matches
4. Display both counts:
   - TIG devices with active data: 170,424
   - Not Communicating vehicles with active data: <count>
5. Prompt user

## Output Format

```
============================================================
SHADOW AUDIT: Not Communicating Vehicles with Active Data
============================================================

TIG devices with Cycle-to-date Data Usage > 0: 170424

Not Communicating vehicles with active cellular data: <count>

Press Enter to continue...
```

## Error Handling

- **Missing vehicles.csv:** Display error with helpful message
- **No matches:** Display count as 0 (valid result)
- **Malformed data:** Catch and report with traceback

## Success Criteria

- [ ] Script loads vehicles.csv without errors
- [ ] Correctly filters to "Not Communicating" status
- [ ] Successfully joins on DSN to TIG devices
- [ ] Displays accurate count of matches
- [ ] Shows both TIG count and Not Communicating count
- [ ] Prompts user to continue
- [ ] Handles edge cases (missing files, no matches)
