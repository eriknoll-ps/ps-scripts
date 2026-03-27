# Shadow-Audit Script Simplification Design

**Date:** 2026-03-27
**Status:** Approved

## Overview

Simplify the shadow-audit analysis workflow to focus exclusively on identifying TIG devices with active data usage by cross-referencing OEM Historical Usage with the device registry. Remove all PACCAR API integration and remote diagnostics checking.

## Current Workflow

The existing `analyze.py` script performs:
1. Load OEM Historical Usage.xlsx and filter to devices with `Cycle-to-date Data Usage > 0`
2. Join with devices.csv on ICCID
3. Filter to TIG device type
4. Fetch "Not Communicating" vehicles from PACCAR API
5. Filter those vehicles to T521 devices (DSN 20M–30M)
6. Fetch shadow state for each T521 device to extract remote diagnostics settings
7. Output results to CSV and JSON

## New Workflow

Simplified to a single analysis step:

1. **Load OEM Historical Usage.xlsx**
   - Read ICCID column as string to preserve all 20 digits
   - Preserve "Cycle-to-date Data Usage" column

2. **Filter to active devices**
   - Keep only rows where `Cycle-to-date Data Usage > 0`

3. **Join with devices.csv on ICCID**
   - Load devices.csv (handle "Device Type" column spacing)
   - Inner join on ICCID to match OEM records with device registry

4. **Filter to TIG devices**
   - Keep only rows where `Device Type == "TIG"`

5. **Report and prompt**
   - Display count of matched TIG devices
   - Show prompt to continue (user decision point for future extensions)

## Data Flow

```
OEM Historical Usage.xlsx
    ↓ (Filter: Cycle-to-date Data Usage > 0)
Active Devices DataFrame
    ↓ (Join on ICCID with devices.csv)
Matched Devices DataFrame
    ↓ (Filter: Device Type == "TIG")
TIG Devices with Active Usage
    ↓
Display Count → Prompt User
```

## Implementation Changes

**File:** `shadow-audit/analyze.py`

### Functions to Keep
- `load_active_tig_devices()` — already implements the complete filtering logic

### Functions to Remove
- `fetch_not_communicating_vehicles()`
- `filter_t521_vehicles()`
- `fetch_shadow_state()`
- `extract_remote_diagnostics()`
- `request_with_retry()`
- `get_headers()`
- `prompt_for_new_token()`
- All PACCAR API configuration (BASE_URL, T521_DSN_MIN, etc.)
- All API-related imports (requests, time retry logic)

### Main Function Changes
Replace multi-step workflow with simple sequence:
1. Load active TIG devices
2. Print count and prompt
3. Return (no CSV/JSON output)

### Dependencies
Keep only: `pandas`, `os`
Remove: `requests`, `json`, `time`, `csv`, `datetime`

## Success Criteria

- [ ] Script loads OEM Historical Usage.xlsx without errors
- [ ] Correctly filters to Cycle-to-date Data Usage > 0
- [ ] Successfully joins devices.csv on ICCID
- [ ] Filters result to TIG device type only
- [ ] Displays accurate count of matched devices
- [ ] Prompts user to continue
- [ ] All PACCAR API code removed
- [ ] Script runs without auth token requirement
