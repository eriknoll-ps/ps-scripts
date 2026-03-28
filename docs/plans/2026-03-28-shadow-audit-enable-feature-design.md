# Shadow-Audit Remote Diagnostics Enable Feature - Design

**Date:** 2026-03-28
**Status:** Approved

## Overview

Add an optional step to the shadow-audit workflow that enables remote diagnostics for devices where "Reported Enabled = False". After fetching shadow state and exporting results to CSV, the user can optionally enable those devices via PlatformScience API.

## Current State

The shadow-audit script currently:
1. Loads OEM Historical Usage data and filters to active TIG devices
2. Identifies 693 Not Communicating vehicles with active data
3. Fetches shadow state (remote diagnostics settings) from PACCAR API
4. Exports results to CSV in `shadow-audit/reports/`

## New Functionality

Add optional enable step after CSV export:
1. Filter shadow state results for devices with `reported_enabled == False`
2. Display filtered device list to user with count
3. Prompt user to confirm enable operation (twice, for safety)
4. Collect PlatformScience API credential (JWT Bearer token)
5. Loop through filtered devices and call enable API
6. Export enable operation results to CSV
7. Display summary with success/failure counts

## Workflow

```
Current:
Load Data → Fetch Shadow State → Export CSV → [Press Enter to exit]

New:
Load Data → Fetch Shadow State → Export CSV
  → "Enable remote diagnostics for X devices with disabled settings? (Y/N)"
  → [If yes] Display filtered list with count
  → "Confirm enable X devices? (Y/N)"
  → [If yes] Collect PlatformScience token
  → Loop through devices (with tqdm progress bar)
     ├─ Call /peoplenet/cf-gateway/v1/v2/application/send API
     ├─ Handle API responses (success, 401 token refresh, other errors)
     └─ Track enabled/failed counts
  → Export enable results to CSV
  → Display summary ("Successfully enabled X/Y, Failed Z")
  → [Press Enter to exit]
```

## Credential Management

### PlatformScience JWT Token

**Storage:** `.platformscience_token` file with 0o600 permissions (read/write owner only)

**Pattern:** Mirror PACCAR token handling
- Load from `.platformscience_token` if exists
- If missing or invalid, prompt user to paste token from browser
- User gets token from: Browser DevTools → Network tab → Copy Authorization header Bearer token from any request to cf-api.mc2.telematicsplatform.io
- Save successfully pasted token to `.platformscience_token`

**Refresh:** JWT tokens from `https://auth.kc.platformscience.com/realms/vvid` have expiration but no known refresh endpoint
- On 401 Unauthorized: prompt user to paste new token
- Retry the failed device once with new token
- If still 401, treat as failed and continue

**Expiration:** Check JWT `exp` claim if needed for user warning (optional enhancement)

## API Integration

### Enable Remote Diagnostics Endpoint

**URL:** `https://cf-api.mc2.telematicsplatform.io/peoplenet/cf-gateway/v1/v2/application/send`

**Method:** POST

**Headers:**
```
Authorization: Bearer {token}
Content-Type: application/json
```

**Payload:**
```json
{
  "deviceId": "{dsn}",
  "destinationTopic": "{\"remoteDiagnostics\":{\"enabled\": true}}",
  "payload": "[]",
  "payloadContentType": "application/json"
}
```

**Response:** 200 OK on success, various errors on failure

### Error Handling

**Retry Strategy:**
- Connection errors: Retry up to 5 times with exponential backoff (2^n seconds)
- 5xx server errors: Retry as above
- 401 Unauthorized: Attempt token refresh, retry once with new token
- 404 Not Found: Log and skip (device may not exist)
- Other errors: Log and skip

**No crash on individual failures:** If device enable fails, log it and continue to next device

## CSV Export (Enable Results)

**File:** `shadow-audit/reports/shadow-audit-enable-YYYYMMDD_HHMMSS.csv`

**Columns:**
1. `DSN` - Device serial number
2. `Enable Status` - "Success" / "Failed" / "Skipped"
3. `Reason` - Description if failed (e.g., "401 Unauthorized", "Network timeout", "Device not found")
4. `Timestamp` - UTC ISO-8601 timestamp when enable was attempted

**Example Rows:**
```
DSN,Enable Status,Reason,Timestamp
20095139,Success,,2026-03-28T14:32:45Z
20305225,Failed,401 Unauthorized,2026-03-28T14:32:47Z
20224907,Failed,Network timeout,2026-03-28T14:32:49Z
```

## Progress Reporting

Use tqdm progress bar during enable loop:
```
Enabling remote diagnostics for X devices...
[████████░░░░░░░░░░░░] 200/X [29%, 01:23 elapsed, ETA 03:15]
```

Status messages:
- Before loop: "Enabling remote diagnostics for X devices..."
- After loop: "Successfully enabled X/X devices"
- If failures: "Failed to enable Y devices (skipped)"
- After export: "Enable results exported to shadow-audit-enable-YYYYMMDD_HHMMSS.csv"

## User Interaction Flow

**Step 1: Initial Prompt**
```
Shadow state exported to: shadow-audit-results-20260328_143245.csv

Found 147 devices with Reported Enabled = False

Enable remote diagnostics for these 147 devices? (Y/N):
```

**Step 2: Confirmation**
```
First 10 devices to enable:
  1. 20095139
  2. 20305225
  ...
  10. 20224923
  ... and 137 more

Confirm enable these 147 devices? (Y/N):
```

**Step 3: Token Collection**
```
PlatformScience API credential required.
Get token from: Browser DevTools (F12) → Network tab → Copy Authorization header
Find request to: cf-api.mc2.telematicsplatform.io
Extract: Bearer {token portion after "Bearer "}

Paste token (or press Enter to abort):
```

**Step 4: Progress**
```
Enabling remote diagnostics for 147 devices...
[████████░░░░░░░░░░░░] 100/147 [68%, 02:15 elapsed, ETA 01:05]
```

**Step 5: Results**
```
Successfully enabled 145/147 devices
Failed to enable 2 devices (skipped)

Enable results exported to: shadow-audit/reports/shadow-audit-enable-20260328_143245.csv

Press Enter to continue...
```

## Success Criteria

- [ ] Device list displayed before any API calls made
- [ ] User confirms twice before enabling (safety check)
- [ ] PlatformScience token stored locally and reused
- [ ] Token refreshed on 401, with reprompt if refresh fails
- [ ] Progress bar shows during enable loop
- [ ] Failed devices don't crash script
- [ ] Enable results exported to CSV with timestamps
- [ ] Summary shows enabled/failed counts
- [ ] Script returns cleanly to prompt after completion

## Dependencies

Existing:
- `requests` - HTTP calls
- `tqdm` - Progress bar
- `pandas` - Data handling
- `datetime` - Timestamps

No new dependencies required.

## Constraints

- API calls limited to filtered devices (typically 0-700 devices)
- Respect rate limiting (reasonable delay between requests)
- Don't overload PlatformScience API
- PlatformScience token is user-specific (won't work for other accounts)

## Files to Modify

- `shadow-audit/analyze.py` - Add enable functions, integrate into main()
- `.gitignore` - Exclude `.platformscience_token` from version control
