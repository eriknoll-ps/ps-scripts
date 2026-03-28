# Shadow-Audit VinDiscovery Command Feature - Design

**Date:** 2026-03-28
**Status:** Approved

## Overview

Add a vinDiscovery command step to the shadow-audit workflow that requests VIN information from all Not Communicating devices with active cellular data (693 devices). Execute this step after fetching shadow state and before the optional enable remote diagnostics step.

## Current State

The shadow-audit script currently:
1. Loads OEM Historical Usage data and filters to TIG devices with active data
2. Identifies Not Communicating vehicles with active data (693 devices)
3. Fetches shadow state (remote diagnostics settings) from PACCAR API
4. Exports shadow state results to CSV
5. Optionally enables remote diagnostics via Trimble API

## New Functionality

Add vinDiscovery command execution between steps 3 and 5:
1. After exporting shadow state results
2. Send vinDiscovery command to all 693 Not Communicating devices
3. Export command results to CSV (success/failure, response data)
4. Continue to optional enable step

## Workflow

```
Load TIG Devices (170,424)
    ↓
Identify Not Communicating with Data (693)
    ↓
Fetch Shadow State from PACCAR API (693 devices)
    ↓
Export Shadow State Results to CSV
    ↓
[NEW] Send VinDiscovery Commands (693 devices)
    ├─ Call /transmit/command/synchronous for each device
    ├─ Rate limit: 0.1-0.5 sec delay between requests
    ├─ Skip device on any failure (no retries)
    └─ Track success/failure counts
    ↓
[NEW] Export VinDiscovery Results to CSV
    ├─ DSN, Command, Success, Response Result, Timestamp
    └─ File: shadow-audit-vin-discovery-YYYYMMDD_HHMMSS.csv
    ↓
Enable Remote Diagnostics (Optional)
    ├─ Prompt user to confirm
    ├─ Lookup appDeviceIds for disabled devices
    ├─ Send enable commands to Trimble API
    └─ Export enable results to CSV
    ↓
Exit
```

## API Integration

### VinDiscovery Command Endpoint

**URL:** `https://security-gateway-rp.platform.fleethealth.io/transmit/command/synchronous`

**Method:** POST

**Headers:**
```
X-Auth-Token: {paccar_token}
X-OEM: paccar
Content-Type: application/json
```

**Payload:**
```json
{
  "deviceIds": ["{dsn}"],
  "deviceType": "yogi",
  "isTestMessage": false,
  "data": {
    "command_id": null,
    "command": "vinDiscovery",
    "parameters": {}
  }
}
```

**Response:** 200 OK with JSON body
```json
{
  "command": "vinDiscovery",
  "commandId": "33fd0584-eb89-4e1f-b14e-0a50dd20845c",
  "dsn": "20013435",
  "response": {
    "result": "OK"
  },
  "success": true,
  "timestamp": 1774663166
}
```

### Error Handling

**Failure strategy:** Skip device, continue to next (no retries)
- Network errors (timeout, connection refused): Skip device
- HTTP errors (4xx, 5xx): Skip device
- JSON parse errors: Skip device
- Track failed count for summary reporting

**Success criteria:**
- HTTP status code: 200
- JSON response valid
- `success` field == true

## CSV Export

**File:** `shadow-audit-vin-discovery-YYYYMMDD_HHMMSS.csv`

**Location:** `shadow-audit/reports/` directory

**Columns:**
1. `DSN` - Device serial number
2. `Command` - Command name ("vinDiscovery")
3. `Success` - Command success status ("Success" / "Failed")
4. `Response Result` - Result from response.result field (e.g., "OK", error message)
5. `Timestamp` - UTC ISO-8601 timestamp when command was sent

**Example rows:**
```
DSN,Command,Success,Response Result,Timestamp
20013435,vinDiscovery,Success,OK,2026-03-28T14:32:45Z
20013436,vinDiscovery,Failed,,2026-03-28T14:32:46Z
20013437,vinDiscovery,Success,OK,2026-03-28T14:32:47Z
```

## Rate Limiting

- Delay between requests: 0.1-0.5 seconds (configurable, default 0.2)
- Purpose: Respect API rate limits, avoid overwhelming server
- Applied: After each successful request (before moving to next device)
- Logic: `time.sleep(delay)` between API calls

## Progress Reporting

Use tqdm progress bar during command execution:
```
Sending vinDiscovery commands to 693 devices...
[████████░░░░░░░░░░░░] 200/693 [29%, 01:23 elapsed, ETA 03:15]
```

Status messages:
- Before loop: "Sending vinDiscovery commands to {count} devices..."
- After loop: "Successfully sent {success}/{total} commands"
- If failures: "Failed to send {failed} commands (skipped)"
- After export: "VinDiscovery results exported to shadow-audit-vin-discovery-YYYYMMDD_HHMMSS.csv"

## Functions to Implement

1. **send_vin_discovery_command(dsn: str, token: str, delay: float = 0.2) -> tuple[bool, str]**
   - Send single vinDiscovery command to device
   - Returns (success: bool, result: str or error message)
   - Applies rate limit delay after request

2. **send_vin_discovery_loop(devices: list, token: str) -> list**
   - Loop through all Not Communicating devices
   - Send command to each with tqdm progress
   - Return list of results with DSN, success, result, timestamp
   - Display summary: success/failed counts

3. **export_vin_discovery_results(results: list, timestamp: str) -> str | None**
   - Export results to CSV
   - Return filename or None on failure
   - Pre-create reports directory

4. **Integration into main()**
   - Call send_vin_discovery_loop() after shadow state export
   - Display results and summary
   - Continue to enable prompt

## Implementation Order

1. Add `send_vin_discovery_command()` function with retry-free error handling
2. Add `send_vin_discovery_loop()` function with tqdm progress
3. Add `export_vin_discovery_results()` function with CSV export
4. Integrate into main() after shadow state export, before enable prompt
5. Test end-to-end workflow

## Success Criteria

- [ ] VinDiscovery commands sent to all 693 devices
- [ ] Rate limiting applied (0.1-0.5 sec delay)
- [ ] No retries on failure (skip and continue)
- [ ] Progress bar shows during execution
- [ ] CSV exported with correct format
- [ ] Success/failure summary displayed
- [ ] Script continues to enable prompt after completion
- [ ] Token reuse from shadow state fetch step

## Dependencies

Existing (already imported):
- `requests` - HTTP calls
- `tqdm` - Progress bar
- `time` - Rate limit delay
- `datetime` - Timestamps
- `csv` - CSV writing

No new dependencies required.

## Constraints

- API calls limited to 693 devices
- Rate limiting: 0.1-0.5 sec between requests (~2-6 minutes total)
- No retry logic (fail fast, continue)
- PACCAR token required and reused from earlier step
- Commands sent as production (isTestMessage: false)
