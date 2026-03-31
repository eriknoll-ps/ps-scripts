# BB Portal Branch 2 Integration Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a BB Portal lookup step in Branch 2 of pendingenable.py that queries device data for TIG units where `ota_reported` is None/missing, then displays a summary and saves to CSV.

**Architecture:** New constants, token helpers, exception class, and three API functions are added to `pendingenable.py`. A new block is appended to `_analyze_tig_units()` after the existing `ota_reported = True` filter, using `ThreadPoolExecutor(max_workers=5)` with tqdm — consistent with how PACCAR enrichment and shadow retrieval already work in the script.

**Tech Stack:** Python, `requests`, `pandas`, `concurrent.futures.ThreadPoolExecutor`, `tqdm`

---

### Task 1: Add BB Portal constants and exception class

**Files:**
- Modify: `pending-enable/pendingenable.py` — constants block (~line 31, after Nexus constants)

**Step 1: Add constants and exception after the Nexus block**

Find this line:
```python
NEXUS_RESET_MAX_WORKERS = 5
```

Add immediately after:
```python
BB_BASE_URL = "https://paccar.jarvis.blackberry.com/api"
BB_TOKEN_FILE = ".bb_token"
BB_MAX_WORKERS = 5
```

Find the `PACCARAuthenticationError` class definition and add a new exception class directly after it:
```python
class BBAuthError(Exception):
    """Raised when BB Portal API returns 401 or 403."""
    pass
```

**Step 2: Verify**

Run: `python -c "import pending-enable.pendingenable" `
(or just open the file and confirm no syntax errors with `python -m py_compile pending-enable/pendingenable.py`)

**Step 3: Commit**
```bash
git add pending-enable/pendingenable.py
git commit -m "feat: add BB Portal constants and BBAuthError exception"
```

---

### Task 2: Add BB Portal token helpers

**Files:**
- Modify: `pending-enable/pendingenable.py` — add after `save_nexus_token()` / before `set_nexus_ota_desired()`

**Step 1: Add load/save/prompt functions**

Find the line:
```python
def set_nexus_ota_desired(dsn: str, nexus_token: str, enabled: bool, max_retries: int = 5) -> tuple:
```

Insert before it:
```python
def load_bb_token() -> Optional[str]:
    """Load cached BB Portal nexus session cookie from file."""
    try:
        with open(BB_TOKEN_FILE, "r") as f:
            token = f.read().strip()
            if token:
                return token
    except Exception:
        pass
    return None


def save_bb_token(token: str) -> None:
    """Save BB Portal nexus session cookie to file."""
    try:
        with open(BB_TOKEN_FILE, "w") as f:
            f.write(token.strip())
    except Exception as e:
        print(f"[WARNING] Could not save BB token: {e}")


def _prompt_bb_token() -> Optional[str]:
    """Prompt the user to paste a BB Portal nexus session cookie."""
    print("\n  BB Portal token required.")
    print("  Get token from: Browser DevTools (F12) > Network tab")
    print("  Find a request to: paccar.jarvis.blackberry.com")
    print("  Copy the 'nexus' cookie value from the Cookie request header")
    token = input("\n  Paste BB Portal nexus cookie (or press Enter to skip): ").strip()
    if token:
        save_bb_token(token)
        return token
    return None
```

**Step 2: Verify syntax**
```bash
python -m py_compile pending-enable/pendingenable.py
```

**Step 3: Commit**
```bash
git add pending-enable/pendingenable.py
git commit -m "feat: add BB Portal token load/save/prompt helpers"
```

---

### Task 3: Add BB Portal API functions

**Files:**
- Modify: `pending-enable/pendingenable.py` — add after `_prompt_bb_token()`

**Step 1: Add the three API functions**

Find the line:
```python
def set_nexus_ota_desired(dsn: str, nexus_token: str, enabled: bool, max_retries: int = 5) -> tuple:
```

Insert before it:
```python
def lookup_bb_device_id(dsn: str, bb_token: str, max_retries: int = 3) -> Optional[str]:
    """
    Look up BB Portal device id by DSN.
    Returns device id string, None if not found, raises BBAuthError on 401/403.
    """
    import time
    url = f"{BB_BASE_URL}/devices"
    headers = {"accept": "application/json", "Cookie": f"nexus={bb_token}"}

    for attempt in range(max_retries):
        try:
            response = requests.get(url, params={"identifier": str(dsn)}, headers=headers, timeout=10)
            if response.status_code in (401, 403):
                raise BBAuthError(f"{response.status_code} Unauthorized - BB Portal cookie expired or invalid")
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list) and len(data) > 0:
                    return data[0].get("id")
                return None
            if response.status_code >= 500:
                if attempt < max_retries - 1:
                    time.sleep(2 ** (attempt + 1))
                    continue
            return None
        except BBAuthError:
            raise
        except requests.RequestException:
            if attempt < max_retries - 1:
                time.sleep(2 ** (attempt + 1))
                continue
            return None
    return None


def fetch_bb_device_attributes(device_id: str, bb_token: str, max_retries: int = 3) -> tuple:
    """
    Fetch BB Portal device attributes by device id.
    Returns (backup_invalidation, last_scan), raises BBAuthError on 401/403.
    """
    import time
    url = f"{BB_BASE_URL}/devices/{device_id}/attributes"
    headers = {"accept": "application/json", "Cookie": f"nexus={bb_token}"}

    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code in (401, 403):
                raise BBAuthError(f"{response.status_code} Unauthorized - BB Portal cookie expired or invalid")
            if response.status_code == 200:
                attrs = response.json().get("attributes", {})
                return attrs.get("backup_invalidation"), attrs.get("last_scan")
            if response.status_code >= 500:
                if attempt < max_retries - 1:
                    time.sleep(2 ** (attempt + 1))
                    continue
            return None, None
        except BBAuthError:
            raise
        except requests.RequestException:
            if attempt < max_retries - 1:
                time.sleep(2 ** (attempt + 1))
                continue
            return None, None
    return None, None


def fetch_bb_data_for_device(dsn: str, bb_token: str) -> dict:
    """
    Worker: look up device id then fetch attributes for one DSN.
    Returns {dsn, bb_device_id, backup_invalidation, last_scan, status, reason}.
    """
    try:
        device_id = lookup_bb_device_id(dsn, bb_token)
        if device_id is None:
            return {"dsn": dsn, "bb_device_id": None, "backup_invalidation": None,
                    "last_scan": None, "status": "No device found", "reason": ""}
        backup_invalidation, last_scan = fetch_bb_device_attributes(device_id, bb_token)
        return {"dsn": dsn, "bb_device_id": device_id, "backup_invalidation": backup_invalidation,
                "last_scan": last_scan, "status": "Success", "reason": ""}
    except BBAuthError as e:
        return {"dsn": dsn, "bb_device_id": None, "backup_invalidation": None,
                "last_scan": None, "status": "Failed", "reason": str(e)}
    except Exception as e:
        return {"dsn": dsn, "bb_device_id": None, "backup_invalidation": None,
                "last_scan": None, "status": "Failed", "reason": str(e)}
```

**Step 2: Verify syntax**
```bash
python -m py_compile pending-enable/pendingenable.py
```

**Step 3: Commit**
```bash
git add pending-enable/pendingenable.py
git commit -m "feat: add BB Portal API lookup functions"
```

---

### Task 4: Wire BB Portal block into _analyze_tig_units

**Files:**
- Modify: `pending-enable/pendingenable.py` — inside `_analyze_tig_units()`, after the `ota_reported = True` block (~line 1533)

**Step 1: Add the BB Portal block**

Find this line inside `_analyze_tig_units`:
```python
        if len(ota_true) > 0:
            filepath = save_results_to_csv(ota_true, filename=get_report_filename("tig_ota_true"))
            print(f"\nReport saved: {filepath}")
```

Add after the entire `if filter_true == "y":` block (i.e., after `print(f"\nReport saved: {filepath}")`):
```python
    # BB Portal lookup for units where ota_reported is None/missing
    ota_none = result[result["ota_reported"].isna()]
    print("\n" + "-"*70)
    print(f"Units with ota_reported = None/missing: {len(ota_none):,}")
    if len(ota_none) > 0:
        fetch_bb = input("Retrieve BB Portal data for these units? (y/n): ").strip().lower()
        if fetch_bb == "y":
            bb_token = load_bb_token()
            if bb_token:
                print("Using cached BB Portal token...")
            else:
                bb_token = _prompt_bb_token()

            if bb_token:
                dsns = ota_none["dsn"].dropna().astype(str).str.strip().tolist()
                print(f"\nFetching BB Portal data for {len(dsns):,} devices...")
                bb_results = []
                with ThreadPoolExecutor(max_workers=BB_MAX_WORKERS) as executor:
                    futures = {executor.submit(fetch_bb_data_for_device, dsn, bb_token): dsn for dsn in dsns}
                    for future in tqdm(as_completed(futures), total=len(dsns), desc="BB Portal lookup", unit="device"):
                        bb_results.append(future.result())

                # Retry auth failures with new token
                auth_failures = [r for r in bb_results if r["status"] == "Failed" and "Unauthorized" in (r["reason"] or "")]
                if auth_failures:
                    print(f"\n[WARNING] {len(auth_failures):,} device(s) failed with auth error.")
                    new_token = _prompt_bb_token()
                    if new_token:
                        print(f"\nRetrying {len(auth_failures):,} failed devices...")
                        retry_futures = {}
                        with ThreadPoolExecutor(max_workers=BB_MAX_WORKERS) as executor:
                            retry_futures = {executor.submit(fetch_bb_data_for_device, r["dsn"], new_token): r["dsn"]
                                             for r in auth_failures}
                            for future in tqdm(as_completed(retry_futures), total=len(auth_failures), desc="Retrying BB Portal", unit="device"):
                                bb_results.append(future.result())
                        # Remove original auth failures from results
                        failed_dsns = {r["dsn"] for r in auth_failures}
                        bb_results = [r for r in bb_results if r["dsn"] not in failed_dsns or r["status"] != "Failed"]

                succeeded = sum(1 for r in bb_results if r["status"] == "Success")
                not_found = sum(1 for r in bb_results if r["status"] == "No device found")
                failed = sum(1 for r in bb_results if r["status"] == "Failed")
                with_invalidation = sum(1 for r in bb_results if r.get("backup_invalidation"))
                print(f"\nBB Portal Summary:")
                print(f"  Succeeded:              {succeeded:,}")
                print(f"  No device found:        {not_found:,}")
                print(f"  Failed:                 {failed:,}")
                print(f"  With backup_invalidation: {with_invalidation:,}")
                print(f"  Without backup_invalidation: {succeeded - with_invalidation:,}")

                # Merge results back onto ota_none and save
                bb_df = pd.DataFrame(bb_results)[["dsn", "bb_device_id", "backup_invalidation", "last_scan", "status", "reason"]]
                bb_df["dsn"] = bb_df["dsn"].astype(str)
                ota_none = ota_none.copy()
                ota_none["dsn"] = ota_none["dsn"].astype(str)
                ota_none = ota_none.merge(bb_df, on="dsn", how="left")
                filepath = save_results_to_csv(ota_none, filename=get_report_filename("tig_bb_portal"))
                print(f"\nReport saved: {filepath}")
```

**Step 2: Verify syntax**
```bash
python -m py_compile pending-enable/pendingenable.py
```

**Step 3: Commit**
```bash
git add pending-enable/pendingenable.py
git commit -m "feat: add BB Portal lookup block to Branch 2 TIG units"
```
