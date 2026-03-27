# Shadow-Audit PACCAR API Integration Design

**Date:** 2026-03-27
**Status:** Approved

## Overview

Add PACCAR Shadow API integration to shadow-audit script to fetch remote diagnostics settings for the 693 Not Communicating devices with active cellular data. Export results to CSV for analysis.

## Current State

The shadow-audit script currently:
1. Loads OEM Historical Usage data
2. Filters to TIG devices with active data usage
3. Identifies Not Communicating vehicles with data (693 devices)
4. Displays counts and sample DSNs

## New Functionality

Add PACCAR API integration to:
1. Fetch shadow state for each of the 693 devices
2. Extract remote diagnostics settings (reported/desired enabled status)
3. Export results to CSV with DSN, settings, and timestamp

## Workflow

```
Load TIG Devices (170,424)
    ↓
Identify Not Communicating with Data (693)
    ↓
Display Counts & Sample DSNs
    ↓
[NEW] Load/Refresh PACCAR Auth Token
    ↓
[NEW] Loop through 693 DSNs (with progress bar)
    ├─ Call /device-config/device-config/{dsn}
    ├─ Extract Remote Diagnostics settings
    └─ Handle errors, token refresh, retries
    ↓
[NEW] Export to CSV
    ├─ Columns: DSN, Reported Enabled, Desired Enabled, Fetch Timestamp
    └─ File: shadow-audit-results-YYYYMMDD_HHMMSS.csv
```

## Auth Token Handling

**Pattern:** Copy from wireless-audit/analyze.py

- **save_paccar_token(token)**: Save to `.paccar_token` file with mode 0o600 (read/write owner only)
- **load_paccar_token()**: Load from `.paccar_token` if exists, return None if missing
- **refresh_paccar_token(token)**: POST to `/refreshToken` endpoint to refresh expired token
  - URL: `https://security-gateway-rp.platform.fleethealth.io/refreshToken`
  - Payload: `{"encodedToken": token}`
  - Headers: `Authorization: Bearer {token}`, `X-Auth-Token: {token}`, `X-OEM: paccar`
  - Returns: New token on success, None on failure

**Token Flow:**
1. Check if `.paccar_token` exists
2. If yes, load and use
3. If no, prompt user to paste token from browser (DevTools → Application → Local Storage → pnet.portal.encodedToken)
4. During API calls: if 401 response, attempt refresh
5. If refresh fails, prompt for new token

## API Integration

**Shadow State Endpoint:**
- URL: `https://security-gateway-rp.platform.fleethealth.io/device-config/device-config/{dsn}`
- Method: GET
- Headers: `X-Auth-Token: {token}`
- Response: JSON with `reported` and `desired` sections
- Extract: `reported.remoteDiagnostics.enabled` and `desired.remoteDiagnostics.enabled`

**Functions to Add:**
- `get_headers(token)` - Returns API headers dict with X-Auth-Token
- `prompt_for_token()` - Prompt user to paste token
- `refresh_paccar_token(token)` - Refresh expired token
- `fetch_shadow_state(dsn, token)` - Fetch shadow state with retry logic
- `extract_remote_diagnostics(shadow_state)` - Extract enabled status (True/False/None)

## Error Handling

**Retry Strategy:**
- Connection errors: Retry up to 5 times with exponential backoff (2, 4, 8, 16, 32 seconds)
- 5xx errors: Retry as above
- 401 Unauthorized: Attempt token refresh, retry once
- 404 Not Found: Log and skip device
- Other errors: Log and skip device

**Error Reporting:**
- Track: devices fetched successfully, failed to fetch
- Display summary at end: "Fetched shadow state for X/693 devices"
- Still export CSV with whatever data was successfully retrieved

## CSV Export

**File:** `shadow-audit-results-YYYYMMDD_HHMMSS.csv`

**Columns:**
1. `DSN` - Device serial number
2. `Reported Enabled` - Reported remote diagnostics enabled status (True/False/None)
3. `Desired Enabled` - Desired remote diagnostics enabled status (True/False/None)
4. `Fetch Timestamp` - UTC timestamp when shadow state was fetched

**Example Rows:**
```
DSN,Reported Enabled,Desired Enabled,Fetch Timestamp
20095139,True,True,2026-03-27T14:32:45Z
20305225,False,True,2026-03-27T14:32:47Z
20224907,,False,2026-03-27T14:32:49Z
```

## Progress Reporting

Use tqdm progress bar during shadow state fetching:
```
Fetching shadow state for 693 devices...
[████████░░░░░░░░░░░░] 200/693 [29%, 01:23 elapsed, ETA 03:15]
```

Add status messages:
- Before loop: "Fetching shadow state for X devices..."
- After loop: "Successfully fetched X/X devices"
- After export: "Results saved to shadow-audit-results-YYYYMMDD_HHMMSS.csv"

## Success Criteria

- [ ] Auth token handling works (save/load/refresh)
- [ ] Can prompt user for token if needed
- [ ] Shadow API calls succeed for most devices
- [ ] Remote diagnostics extracted correctly
- [ ] Progress bar shows during fetching
- [ ] CSV file created with correct format
- [ ] Error handling doesn't crash script
- [ ] User can re-run to update results

## Dependencies

Add to requirements:
- `requests` - Already available in wireless-audit
- `tqdm` - Already available (used in wireless-audit)

## Constraints

- API calls limited to 693 devices
- Respect rate limiting (use reasonable delay between requests)
- Don't overload PACCAR API
- Auth token should only work for this user's account
