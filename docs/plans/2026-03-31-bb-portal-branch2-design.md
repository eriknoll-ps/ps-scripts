# BB Portal Lookup — Branch 2 TIG Units (None/Missing OTA)

**Date:** 2026-03-31  
**Script:** `pending-enable/pendingenable.py`

## Summary

Add a new step inside Branch 2 (TIG Units) that targets devices where `ota_reported` is None/missing after the shadow fetch. For these devices, query the BB Portal API to retrieve `backup_invalidation` and `last_scan`, display a summary, and save to CSV.

## Constants & Token Storage

- `BB_BASE_URL = "https://paccar.jarvis.blackberry.com/api"`
- `BB_TOKEN_FILE = ".bb_token"` — stores the `nexus` session cookie value
- `load_bb_token()` / `save_bb_token()` — same pattern as PACCAR/Trimble/Nexus helpers
- Auth sent as `Cookie: nexus=<value>` on every request
- New exception class `BBAuthError` raised on 401/403 responses

## API Functions

### `lookup_bb_device_id(dsn, bb_token) -> Optional[str]`
- GET `/api/devices?identifier={dsn}`
- Returns `id` from first element of response array
- Returns `None` if array is empty or on non-auth error
- Raises `BBAuthError` on 401/403

### `fetch_bb_device_attributes(device_id, bb_token) -> tuple`
- GET `/api/devices/{device_id}/attributes`
- Returns `(backup_invalidation, last_scan)` extracted from `attributes` dict
- Returns `(None, None)` on error
- Raises `BBAuthError` on 401/403

### `fetch_bb_data_for_device(dsn, bb_token) -> dict`
- Worker: chains both calls above
- Returns `{dsn, bb_device_id, backup_invalidation, last_scan, status, reason}`
- `status`: `"Success"` | `"Failed"` | `"No device found"`

## Branch 2 Integration

Placed after the existing `ota_reported = True` filter block.

### Flow
1. Filter `result` to `ota_reported` is None/missing → `ota_none`
2. Print: `Units with ota_reported = None/missing: X`
3. Prompt: `Retrieve BB Portal data for these X units? (y/n)`
4. Load cached BB token; prompt if missing (instructions to copy `nexus` cookie from DevTools)
5. Parallel fetch with `ThreadPoolExecutor(max_workers=5)` + tqdm progress bar
6. On 401/403 failures: collect, prompt for new cookie, retry those devices only
7. Print summary:
   - Total succeeded / failed
   - Count with `backup_invalidation` populated vs. null
8. Merge `bb_device_id`, `backup_invalidation`, `last_scan` onto `ota_none`
9. Save to `reports/tig_bb_portal_<timestamp>.csv`

## Error Handling

- `BBAuthError` mid-run: worker returns failure with reason; after batch, auth failures trigger token re-prompt and retry (same pattern as Trimble reset flow)
- Device not found in BB Portal: `status = "No device found"`, included in output with nulls
- Network/timeout errors: logged in `reason` field, included in CSV
