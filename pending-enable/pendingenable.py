import io
import sys
import os
import logging
import datetime
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, Optional
from tqdm import tqdm


# Configuration
OTA_ENDPOINT = "https://ota-service.platform.fleethealth.io/reporting/analytics/software-logs/latest"
OTA_PARAMS = {
    "status": "PENDING_RESPONSE",
    "format": "csv",
    "fields": "vin,statusAdditionalInfo,updateDate"
}
OTA_TIMEOUT = 30

PACCAR_BASE_URL = "https://security-gateway-rp.platform.fleethealth.io"
PACCAR_TIMEOUT = 10
PACCAR_MAX_WORKERS = 8

REPORTS_DIR = "reports"
PACCAR_TOKEN_FILE = ".paccar_token"
SHADOW_ENDPOINT = "https://security-gateway-rp.platform.fleethealth.io/device-config/device-config"
SHADOW_MAX_WORKERS = 5

TRIMBLE_TOKEN_FILE = ".trimble_token"
TRIMBLE_APP_INSTANCE_ID = "d5554600-441f-4dda-b4b3-26dfbf79e91d"
TRIMBLE_UPDATE_SHADOW_URL = "https://cloud.api.trimble.com/devicegateway/management/1.0/OutboundMessage/UpdateDeviceShadow"

NEXUS_TOKEN_FILE = ".nexus_token"
NEXUS_SHADOW_URL = "https://cf-api.mc2.telematicsplatform.io/peoplenet/cf-gateway/v1/deviceconfig/shadowupdate"
NEXUS_APP_CUSTOMER_ID = "248869ca-27c5-4af6-b8a3-e04d1309578b"
NEXUS_RESET_MAX_WORKERS = 5

BB_BASE_URL = "https://paccar.jarvis.blackberry.com/api"
BB_TOKEN_FILE = ".bb_token"
BB_MAX_WORKERS = 5

# Create reports directory if it doesn't exist
os.makedirs(REPORTS_DIR, exist_ok=True)

logger = logging.getLogger(__name__)


class PACCARAuthenticationError(Exception):
    """Raised when PACCAR API returns 401 Unauthorized."""
    pass


class BBAuthError(Exception):
    """Raised when BB Portal API returns 401 or 403."""
    pass


def download_pending_updates(retry_on_vpn_error=True) -> Optional[pd.DataFrame]:
    """
    Download pending OTA updates from OTA service endpoint.

    Returns:
        pandas.DataFrame: Dataframe with columns: vin, statusAdditionalInfo, updateDate
        None if download fails

    Args:
        retry_on_vpn_error: If True, prompt user to check VPN and retry on connection error
    """
    try:
        print(f"Downloading pending updates from OTA service...")
        response = requests.get(
            OTA_ENDPOINT,
            params=OTA_PARAMS,
            timeout=OTA_TIMEOUT
        )

        # Check for HTTP errors
        if response.status_code != 200:
            print(f"Error: HTTP {response.status_code}")
            print(f"Response: {response.text[:500]}")
            return None

        # Parse CSV response
        df = pd.read_csv(io.StringIO(response.text))
        print(f"Downloaded {len(df)} pending updates")
        return df

    except requests.exceptions.ConnectionError as e:
        print(f"Connection error: {e}")
        if retry_on_vpn_error:
            print("\n⚠️  Unable to connect to OTA service. Make sure you're connected to the VPN.")
            retry = input("Try again? (y/n): ").strip().lower()
            if retry == "y":
                return download_pending_updates(retry_on_vpn_error=False)
        return None

    except requests.exceptions.Timeout:
        print(f"Timeout: OTA service request took longer than {OTA_TIMEOUT}s")
        if retry_on_vpn_error:
            retry = input("Try again? (y/n): ").strip().lower()
            if retry == "y":
                return download_pending_updates(retry_on_vpn_error=False)
        return None

    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None

    except pd.errors.ParserError as e:
        print(f"Error parsing CSV response: {e}")
        return None

    except Exception as e:
        print(f"Unexpected error: {type(e).__name__}: {e}")
        return None


def _extract_paccar_software_status(vin: str, bearer_token: Optional[str] = None, debug: bool = False) -> dict:
    """
    Extract software update status for a given VIN from PACCAR Solutions API.

    Args:
        vin: Vehicle Identification Number
        bearer_token: Optional PACCAR API bearer token
        debug: Enable debug logging

    Returns:
        Dictionary containing software status information

    Raises:
        PACCARAuthenticationError: If API returns 401 Unauthorized
    """
    result = {
        "vin": vin,
        "dsn": None,
        "softwareUpdateStatus": None,
        "softwareTruckStatus": None,
        "disabledOemLicense": None,
        "removalCategory": None,
        "lastUpdated": None,
        "make": None,
        "pmgSwVersion": None,
        "software_extraction_success": False,
        "error": None
    }

    try:
        # Prepare session with auth headers
        session = requests.Session()
        if bearer_token:
            session.headers.update({
                "X-Auth-Token": bearer_token,
                "X-OEM": "paccar"
            })
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })

        # Get vehicle details by VIN from PACCAR API
        url = f"{PACCAR_BASE_URL}/v2vehicles/{vin}"
        if debug:
            print(f"[DEBUG] Fetching PACCAR data for VIN {vin}...")

        response = session.get(url, timeout=PACCAR_TIMEOUT)

        # Check for authentication errors
        if response.status_code == 401:
            raise PACCARAuthenticationError("Unauthorized - Bearer token is invalid or expired")

        if response.status_code != 200:
            result["error"] = f"HTTP {response.status_code}"
            if debug:
                print(f"[DEBUG] Error for VIN {vin}: {result['error']}")
            return result

        vehicle_details = response.json()

        if isinstance(vehicle_details, dict):
            # Extract software status fields
            software_info = vehicle_details.get("softwareInfo", {})
            if isinstance(software_info, dict):
                result["softwareUpdateStatus"] = software_info.get("softwareUpdateStatus")
                result["softwareTruckStatus"] = software_info.get("softwareTruckStatus")

            # Extract license info fields
            license_info = vehicle_details.get("licenseInfo", {})
            if isinstance(license_info, dict):
                result["disabledOemLicense"] = license_info.get("disabledOemLicense")
                result["removalCategory"] = license_info.get("removalCategory")

            # Extract location info (lastUpdated)
            location_info = vehicle_details.get("locationInfo", {})
            if isinstance(location_info, dict):
                result["lastUpdated"] = location_info.get("lastUpdated")

            # Extract device info (dsn, make, pmgSwVersion)
            device_info_block = vehicle_details.get("deviceInfo", {})
            if isinstance(device_info_block, dict):
                # Extract DSN (try multiple possible locations)
                if "dsn" in device_info_block:
                    result["dsn"] = device_info_block.get("dsn")
                elif "serialNumber" in device_info_block:
                    result["dsn"] = device_info_block.get("serialNumber")

                # Extract pmgSwVersion from pmgInfo
                pmg_info = device_info_block.get("pmgInfo", {})
                if isinstance(pmg_info, dict):
                    result["pmgSwVersion"] = pmg_info.get("pmgSwVersion")
                    # Also try to get DSN from pmgInfo if not found above
                    if not result["dsn"] and "dsn" in pmg_info:
                        result["dsn"] = pmg_info.get("dsn")

                # Extract make from vinRollCallData
                vin_roll_call_data = device_info_block.get("vinRollCallData", [])
                if isinstance(vin_roll_call_data, list) and len(vin_roll_call_data) > 0:
                    first_item = vin_roll_call_data[0]
                    if isinstance(first_item, dict):
                        component_id_data = first_item.get("componentIdData", {})
                        if isinstance(component_id_data, dict):
                            result["make"] = component_id_data.get("make")

            # Mark as success if we got any data
            result["software_extraction_success"] = True
            if debug:
                print(f"[DEBUG] Extracted PACCAR data for VIN {vin}: status={result['softwareUpdateStatus']}, disabled_oem={result['disabledOemLicense']}")

        session.close()

    except PACCARAuthenticationError:
        # Re-raise authentication errors
        raise
    except Exception as e:
        result["error"] = str(e)
        if debug:
            print(f"[DEBUG] Error extracting PACCAR data for VIN {vin}: {type(e).__name__}: {e}")

    return result


def retrieve_paccar_solutions_data(pending_df: pd.DataFrame, bearer_token: Optional[str] = None, debug: bool = False) -> pd.DataFrame:
    """
    Retrieve PACCAR Solutions data for all pending updates.

    Args:
        pending_df: DataFrame with pending updates (must contain 'vin' column)
        bearer_token: Optional PACCAR API bearer token
        debug: Enable debug logging

    Returns:
        DataFrame with added PACCAR Solutions columns
    """
    # Make a copy to avoid modifying original
    result_df = pending_df.copy()

    # Extract unique VINs
    vins = [str(v).strip() for v in result_df["vin"].dropna().unique().tolist()]
    if not vins:
        print("No VINs found in pending updates.")
        return result_df

    print(f"\nRetrieving PACCAR Solutions data for {len(vins)} unique VINs...")

    # Initialize PACCAR columns
    paccar_columns = [
        "dsn", "softwareUpdateStatus", "softwareTruckStatus", "disabledOemLicense",
        "removalCategory", "lastUpdated", "make", "pmgSwVersion",
        "paccar_retrieval_success", "paccar_error"
    ]
    for col in paccar_columns:
        if col not in result_df.columns:
            result_df[col] = None

    results = {}
    auth_error = None

    # Parallel retrieval with progress bar
    with ThreadPoolExecutor(max_workers=PACCAR_MAX_WORKERS) as executor:
        futures = {
            executor.submit(_extract_paccar_software_status, vin, bearer_token, debug): vin
            for vin in vins
        }
        for future in tqdm(as_completed(futures), total=len(vins), desc="PACCAR Solutions retrieval", unit="vehicle"):
            try:
                result = future.result()
                results[result["vin"]] = result
            except PACCARAuthenticationError as e:
                auth_error = e
                # Cancel remaining futures
                for f in futures:
                    f.cancel()
                break
            except Exception as e:
                vin = futures[future]
                if debug:
                    print(f"[DEBUG] Error for VIN {vin}: {type(e).__name__}: {e}")

    # Re-raise authentication error after executor cleanup
    if auth_error:
        raise auth_error

    # Map results back to DataFrame
    if results:
        vin_str = result_df["vin"].astype(str)
        result_df["dsn"] = vin_str.map(lambda v: results.get(v, {}).get("dsn"))
        result_df["softwareUpdateStatus"] = vin_str.map(lambda v: results.get(v, {}).get("softwareUpdateStatus"))
        result_df["softwareTruckStatus"] = vin_str.map(lambda v: results.get(v, {}).get("softwareTruckStatus"))
        result_df["disabledOemLicense"] = vin_str.map(lambda v: results.get(v, {}).get("disabledOemLicense"))
        result_df["removalCategory"] = vin_str.map(lambda v: results.get(v, {}).get("removalCategory"))
        result_df["lastUpdated"] = vin_str.map(lambda v: results.get(v, {}).get("lastUpdated"))
        result_df["make"] = vin_str.map(lambda v: results.get(v, {}).get("make"))
        result_df["pmgSwVersion"] = vin_str.map(lambda v: results.get(v, {}).get("pmgSwVersion"))
        result_df["paccar_retrieval_success"] = vin_str.map(lambda v: results.get(v, {}).get("software_extraction_success"))
        result_df["paccar_error"] = vin_str.map(lambda v: results.get(v, {}).get("error"))

    # Print summary
    successful = sum(1 for r in results.values() if r.get("software_extraction_success"))
    print(f"PACCAR Solutions data retrieved: {successful} / {len(vins)}")

    # Show failed VINs if any
    failed_vins = [vin for vin, r in results.items() if not r.get("software_extraction_success")]
    if failed_vins:
        print(f"  Failed to retrieve data for {len(failed_vins)} VIN(s):")
        for vin in sorted(failed_vins):
            error = results[vin].get("error", "Unknown error")
            print(f"    {vin}: {error}")

    return result_df


def get_report_filename(prefix: str = "pending_updates") -> str:
    """
    Generate timestamped report filename.

    Args:
        prefix: Filename prefix (default: "pending_updates")

    Returns:
        Filename with format: {prefix}_YYYY-MM-DD_HH-MM-SS.csv
    """
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return f"{prefix}_{timestamp}.csv"


def save_results_to_csv(df: pd.DataFrame, filename: Optional[str] = None) -> str:
    """
    Save results DataFrame to CSV file in reports directory.

    Args:
        df: DataFrame to save
        filename: Optional filename (if not provided, generates timestamped filename)

    Returns:
        Full path to saved CSV file
    """
    if filename is None:
        filename = get_report_filename("pending_updates")

    # Save to reports directory
    filepath = os.path.join(REPORTS_DIR, filename)
    df.to_csv(filepath, index=False)
    print(f"[OK] Results saved to: {filepath}")

    return filepath


def find_most_recent_csv(pattern: str = None) -> Optional[str]:
    """
    Find the most recently modified enriched CSV file in reports directory.
    Prefers 'pending_updates_enriched_*.csv' files (post-PACCAR, pre-filter),
    falling back to any 'pending_updates_*.csv' if none found.

    Returns:
        Full path to most recent file, or None if no files found
    """
    import glob
    # Prefer enriched files first
    for search_pattern in [
        os.path.join(REPORTS_DIR, "pending_updates_enriched_*.csv"),
        os.path.join(REPORTS_DIR, "pending_updates_*.csv"),
    ]:
        files = glob.glob(search_pattern)
        if files:
            return max(files, key=os.path.getmtime)
    return None


def load_paccar_token() -> Optional[str]:
    """
    Load cached PACCAR bearer token from .paccar_token file.

    Returns:
        Bearer token string or None if file doesn't exist
    """
    try:
        if os.path.exists(PACCAR_TOKEN_FILE):
            with open(PACCAR_TOKEN_FILE, "r") as f:
                token = f.read().strip()
                if token:
                    return token
    except Exception as e:
        print(f"[WARNING] Failed to load cached PACCAR token: {e}")
    return None


def save_paccar_token(token: str) -> bool:
    """
    Save PACCAR bearer token to .paccar_token file (read-protected).

    Args:
        token: Bearer token to cache

    Returns:
        True if saved successfully, False otherwise
    """
    try:
        with open(PACCAR_TOKEN_FILE, "w") as f:
            f.write(token)
        # Set file permissions to read-only for current user (Unix-like)
        try:
            os.chmod(PACCAR_TOKEN_FILE, 0o600)
        except:
            pass  # Windows doesn't support Unix permissions
        return True
    except Exception as e:
        print(f"[WARNING] Failed to save PACCAR token: {e}")
        return False


def refresh_paccar_token(current_token: str) -> Optional[str]:
    """
    Attempt to refresh expired PACCAR bearer token via POST request with encodedToken.

    Args:
        current_token: Current bearer token to refresh

    Returns:
        New bearer token if refresh successful, None if refresh fails
    """
    try:
        url = "https://security-gateway-rp.platform.fleethealth.io/refreshToken"
        headers = {
            "Authorization": f"Bearer {current_token}",
            "X-Auth-Token": current_token,
            "X-OEM": "paccar",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }

        # POST the current token as encodedToken in JSON payload
        payload = {
            "encodedToken": current_token
        }

        response = requests.post(url, json=payload, headers=headers, timeout=10, verify=False)

        new_token = None

        # Check if response was successful
        if response.status_code == 200:
            # Try to parse JSON response body for new token
            try:
                data = response.json()
                # Look for the new token in response
                for key in ["encodedToken", "token", "accessToken", "access_token", "bearer_token", "new_token"]:
                    if key in data and data[key]:
                        new_token = data[key]
                        if new_token:
                            break
            except (ValueError, json.JSONDecodeError):
                pass

        return new_token if new_token else None

    except requests.exceptions.RequestException as e:
        if isinstance(e, requests.exceptions.ConnectionError):
            print(f"[DEBUG] Token refresh connection failed: {e}")
        return None


def load_csv_file(filepath: str) -> Optional[pd.DataFrame]:
    """
    Load CSV file into DataFrame.

    Args:
        filepath: Path to CSV file

    Returns:
        DataFrame or None if load fails
    """
    try:
        # Try to parse both updateDate and lastUpdated, but don't fail if they don't exist
        df = pd.read_csv(filepath)
        if "updateDate" in df.columns:
            df["updateDate"] = pd.to_datetime(df["updateDate"], errors="coerce")
        if "lastUpdated" in df.columns:
            df["lastUpdated"] = pd.to_datetime(df["lastUpdated"], errors="coerce")
        print(f"[OK] Loaded {len(df)} rows from {filepath}")
        return df
    except Exception as e:
        print(f"[ERROR] Failed to load {filepath}: {e}")
        return None


def filter_exclude_dsn_range(df: pd.DataFrame, min_dsn: int = 30000000, max_dsn: int = 40000000, debug: bool = False) -> pd.DataFrame:
    """
    Filter DataFrame to exclude devices with DSN in specified range.

    Args:
        df: Input DataFrame (must contain 'dsn' column)
        min_dsn: Minimum DSN to exclude (default: 30000000)
        max_dsn: Maximum DSN to exclude (default: 40000000)
        debug: Enable debug output

    Returns:
        Filtered DataFrame with excluded DSN range removed
    """
    if "dsn" not in df.columns:
        if debug:
            print("[DEBUG] 'dsn' column not found. Skipping DSN range filter.")
        return df

    try:
        df = df.copy()

        # Convert DSN to numeric, handling any non-numeric values
        df["dsn_numeric"] = pd.to_numeric(df["dsn"], errors="coerce")

        # Create mask for devices outside the exclusion range
        # Keep devices where DSN is NaN or outside the range
        mask = (df["dsn_numeric"].isna()) | (df["dsn_numeric"] < min_dsn) | (df["dsn_numeric"] > max_dsn)
        filtered_df = df[mask].copy()

        # Drop the temporary numeric column
        filtered_df = filtered_df.drop(columns=["dsn_numeric"])

        removed_count = len(df) - len(filtered_df)
        print(f"Filtered out {removed_count} devices with DSN in range {min_dsn:,} - {max_dsn:,}")
        print(f"Remaining: {len(filtered_df)} devices")

        if debug and removed_count > 0:
            print(f"[DEBUG] Removed DSN values:")
            excluded = df[~mask][["vin", "dsn"]].drop_duplicates("dsn")
            print(excluded.to_string(index=False))

        return filtered_df

    except Exception as e:
        print(f"[ERROR] Failed to filter by DSN range: {e}")
        return df


def filter_by_last_updated(df: pd.DataFrame, hours: int = 24, debug: bool = False, use_local_tz: bool = True) -> pd.DataFrame:
    """
    Filter DataFrame to include only vehicles with lastUpdated within past X hours.
    Uses PACCAR lastUpdated field (device's last update time in PACCAR system).

    Args:
        df: Input DataFrame (must contain 'lastUpdated' column with ISO format timestamps)
        hours: Number of hours to look back (default: 24)
        debug: Enable debug output for troubleshooting
        use_local_tz: If True, use local timezone; if False, use UTC (default: True)

    Returns:
        Filtered DataFrame
    """
    if "lastUpdated" not in df.columns:
        print("[WARNING] 'lastUpdated' column not found. (Need to retrieve PACCAR Solutions data first)")
        return df

    try:
        # Parse lastUpdated as datetime (ISO format)
        df = df.copy()
        df["lastUpdated"] = pd.to_datetime(df["lastUpdated"], errors="coerce")

        # Calculate cutoff time (now - X hours)
        if use_local_tz:
            # Use local timezone (get timezone-aware "now" in local TZ)
            now = datetime.datetime.now().astimezone()
            tz_name = now.tzname()
            # Convert to UTC for consistent comparison with updateDate
            now_utc = now.astimezone(datetime.timezone.utc)
        else:
            # Use UTC
            now = datetime.datetime.now(datetime.timezone.utc)
            now_utc = now
            tz_name = "UTC"

        cutoff_time = now_utc - datetime.timedelta(hours=hours)

        if debug:
            print(f"[DEBUG] Current time ({tz_name}): {now}")
            print(f"[DEBUG] Current time (UTC): {now_utc}")
            print(f"[DEBUG] Cutoff time (now - {hours}h, UTC): {cutoff_time}")
            print(f"[DEBUG] lastUpdated dtype: {df['lastUpdated'].dtype}")
            print(f"[DEBUG] lastUpdated timezone: {df['lastUpdated'].dt.tz}")
            print(f"[DEBUG] Sample lastUpdated values:")
            print(df['lastUpdated'].head(3).to_string())

        # Convert cutoff_time to pandas Timestamp for comparison
        # Ensure it's compatible with the lastUpdated column
        cutoff_timestamp = pd.Timestamp(cutoff_time)

        # If lastUpdated is timezone-naive, strip timezone from cutoff
        if df["lastUpdated"].dt.tz is None:
            cutoff_timestamp = cutoff_timestamp.tz_localize(None)

        # Filter to rows within the time window
        mask = df["lastUpdated"] >= cutoff_timestamp
        filtered_df = df[mask].copy()

        removed_count = len(df) - len(filtered_df)
        print(f"Filtered to {len(filtered_df)} vehicles with lastUpdated within past {hours} hours")
        if removed_count > 0:
            print(f"  (Removed {removed_count} older vehicles)")

        # Show newest vehicles if we have results
        if len(filtered_df) > 0 and debug:
            print(f"\n[DEBUG] Newest {min(3, len(filtered_df))} lastUpdated times:")
            print(filtered_df.nlargest(3, "lastUpdated")[["vin", "lastUpdated"]].to_string())

        return filtered_df

    except Exception as e:
        print(f"[ERROR] Failed to filter by updateDate: {e}")
        if debug:
            import traceback
            traceback.print_exc()
        return df


def main():
    """Standalone usage: download pending updates and retrieve PACCAR Solutions data."""
    # Check for existing CSV files
    print("="*70)
    print("Pending Enable - Data Loading")
    print("="*70)

    most_recent = find_most_recent_csv()
    skipped_to_existing = False
    if most_recent:
        print(f"\nFound existing file: {most_recent}")
        use_existing = input("Use this file to skip download/PACCAR steps? (y/n): ").strip().lower()
        if use_existing == "y":
            pending_df = load_csv_file(most_recent)
            if pending_df is None:
                print("Failed to load file. Proceeding with fresh download.")
                pending_df = download_pending_updates()
            else:
                print(f"Skipping download and PACCAR retrieval steps.\n")
                skipped_to_existing = True
        else:
            pending_df = download_pending_updates()
    else:
        pending_df = download_pending_updates()

    if pending_df is None:
        print("Failed to download pending updates.")
        sys.exit(1)

    paccar_retrieved = skipped_to_existing

    # Apply DSN exclusion filter when loading from existing file (step 3)
    if skipped_to_existing and "dsn" in pending_df.columns:
        print("\n" + "="*70)
        print("Filtering by DSN Range")
        print("="*70)
        pending_df = filter_exclude_dsn_range(pending_df, min_dsn=30000000, max_dsn=40000000, debug=False)
        print("="*70)

    if not skipped_to_existing:
        # Ask for PACCAR bearer token
        print("\n" + "="*70)
        print("PACCAR Solutions Data Retrieval (Optional)")
        print("="*70)
        retrieve_paccar = input("Retrieve PACCAR Solutions data for these vehicles? (y/n): ").strip().lower()

    if not skipped_to_existing and retrieve_paccar == "y":
        # Try to load cached token first
        cached_token = load_paccar_token()
        bearer_token = None

        # Try cached token silently first
        if cached_token:
            print("Attempting to use cached PACCAR token...")
            try:
                pending_df = retrieve_paccar_solutions_data(pending_df, bearer_token=cached_token, debug=False)
                bearer_token = cached_token
                paccar_retrieved = True
            except Exception as e:
                if "PACCARAuthenticationError" in type(e).__name__:
                    print("[WARNING] Cached PACCAR token expired or invalid. Attempting to refresh...")
                    # Try to refresh the cached token
                    refreshed_token = refresh_paccar_token(cached_token)
                    if refreshed_token:
                        print("Token refreshed successfully!")
                        try:
                            pending_df = retrieve_paccar_solutions_data(pending_df, bearer_token=refreshed_token, debug=False)
                            bearer_token = refreshed_token
                            save_paccar_token(refreshed_token)
                            paccar_retrieved = True
                        except Exception as retry_e:
                            if "PACCARAuthenticationError" in type(retry_e).__name__:
                                print("Refreshed token also failed. Please provide a new token.")
                                bearer_token = None
                            else:
                                raise
                    else:
                        print("Token refresh failed. Please provide a new token.")
                        bearer_token = None
                else:
                    raise

        # If cached/refreshed token failed or doesn't exist, prompt for new token
        if not bearer_token:
            bearer_token = input("Enter PACCAR API bearer token (or press Enter to skip): ").strip()

            if bearer_token:
                # Try with new token
                try:
                    pending_df = retrieve_paccar_solutions_data(pending_df, bearer_token=bearer_token, debug=False)
                    # Save new token if successful
                    save_paccar_token(bearer_token)
                    paccar_retrieved = True
                except Exception as e:
                    if "PACCARAuthenticationError" in type(e).__name__:
                        print("[ERROR] Provided token is invalid or expired. Skipping PACCAR retrieval.")
                        bearer_token = None
                    else:
                        raise

        # Save enriched data after successful PACCAR retrieval
        if paccar_retrieved and not skipped_to_existing:
            print("\n" + "="*70)
            paccar_filepath = save_results_to_csv(pending_df, filename=get_report_filename("pending_updates_enriched"))
            print("="*70)

            # Filter by DSN range if DSN column exists
            if "dsn" in pending_df.columns:
                print("\n" + "="*70)
                print("Filtering by DSN Range")
                print("="*70)
                pending_df = filter_exclude_dsn_range(pending_df, min_dsn=30000000, max_dsn=40000000, debug=False)
                print("="*70)

    # Ask for lastUpdated filtering
    print("\n" + "="*70)
    print("Filter by Last Updated (Optional)")
    print("="*70)

    filter_applied = False
    while True:
        filter_dates = input("Filter to vehicles with lastUpdated within past X hours? (y/n): ").strip().lower()
        if filter_dates in ("y", "n"):
            break
        # Allow typing a number directly as a shortcut
        try:
            _hours_direct = int(filter_dates)
            filter_dates = "y"
            hours_input = str(_hours_direct)
            break
        except ValueError:
            print(f"  Please enter y or n.")

    if filter_dates == "y":
        if "hours_input" not in dir():
            hours_input = input("Enter number of hours (default 24): ").strip()
        try:
            hours = int(hours_input) if hours_input else 24
            pending_df = filter_by_last_updated(pending_df, hours=hours)
            filter_applied = True
        except ValueError:
            print(f"[ERROR] Invalid input '{hours_input}'. Using default 24 hours.")
            pending_df = filter_by_last_updated(pending_df, hours=24)
            filter_applied = True

    # Display summary
    print("\n" + "="*70)
    print("Results Summary")
    print("="*70)
    print(f"Total vehicles: {len(pending_df)}")
    print(f"Columns: {', '.join(pending_df.columns.tolist())}")

    # Preview first few rows
    print("\nFirst 10 rows:")
    print(pending_df.head(10).to_string(index=False))

    # Save results (if filtering was applied, save the filtered version)
    if filter_applied:
        print("\n" + "="*70)
        print("Saving Filtered Results")
        print("="*70)
        filepath = save_results_to_csv(pending_df)
        print("="*70)
        print(f"Filtered file saved (original PACCAR data also saved above).")
    elif paccar_retrieved:
        # PACCAR data was already saved above after retrieval
        print("\n" + "="*70)
        print("(PACCAR-enriched data already saved above)")
        print("="*70)
    else:
        # No PACCAR retrieval, save the download
        print("\n" + "="*70)
        filepath = save_results_to_csv(pending_df)
        print("="*70)
        print(f"File ready for subsequent processing steps.")

    # --- Branches 1-3: all operate on pending_df (data after step 3) ---

    # Branch 1: PMG units
    print("\n" + "="*70)
    run_pmg = input("Analyze PMG units? (y/n): ").strip().lower()
    if run_pmg == "y":
        _analyze_pmg_units(pending_df)

    # Branch 2: TIG units
    print("\n" + "="*70)
    run_tig = input("Analyze TIG units? (y/n): ").strip().lower()
    if run_tig == "y":
        _analyze_tig_units(pending_df)

    # Branch 3: TIG Nexus firmware
    print("\n" + "="*70)
    run_nexus = input("Analyze TIG Nexus firmware units? (y/n): ").strip().lower()
    if run_nexus == "y":
        _analyze_tig_nexus_units(pending_df)

    # Branch 4: All other TIG units
    print("\n" + "="*70)
    run_tig_other = input("Analyze all other TIG units? (y/n): ").strip().lower()
    if run_tig_other == "y":
        _analyze_tig_other_units(pending_df)


def load_trimble_token() -> Optional[str]:
    """Load cached Trimble API token from file."""
    try:
        if os.path.exists(TRIMBLE_TOKEN_FILE):
            with open(TRIMBLE_TOKEN_FILE, "r") as f:
                token = f.read().strip()
                if token:
                    return token
    except Exception as e:
        print(f"[WARNING] Could not load Trimble token: {e}")
    return None


def save_trimble_token(token: str) -> None:
    """Save Trimble API token to file."""
    try:
        with open(TRIMBLE_TOKEN_FILE, "w") as f:
            f.write(token.strip())
        try:
            os.chmod(TRIMBLE_TOKEN_FILE, 0o600)
        except Exception:
            pass
    except Exception as e:
        print(f"[WARNING] Could not save Trimble token: {e}")


def lookup_app_device_id(dsn: str, paccar_token: str, max_retries: int = 5) -> Optional[str]:
    """
    Lookup appDeviceId from DSN using PACCAR vehicledevices API.
    Returns appDeviceId string, or None on failure.
    Raises PACCARAuthenticationError on 401.
    """
    import time
    url = f"{PACCAR_BASE_URL}/vehicledevices/{dsn}"
    headers = {"X-Auth-Token": paccar_token}

    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=PACCAR_TIMEOUT)
            if response.status_code == 200:
                data = response.json()
                return data.get("provisioningInfo", {}).get("tpaasDevice", {}).get("appDeviceId")
            elif response.status_code == 401:
                raise PACCARAuthenticationError("Unauthorized - Bearer token is invalid or expired")
            elif response.status_code == 404:
                return None
            elif response.status_code >= 500:
                if attempt < max_retries - 1:
                    time.sleep(2 ** (attempt + 1))
                    continue
            return None
        except PACCARAuthenticationError:
            raise
        except requests.RequestException:
            if attempt < max_retries - 1:
                time.sleep(2 ** (attempt + 1))
                continue
            return None
    return None


def set_ota_desired_true(dsn: str, app_device_id: str, trimble_token: str, max_retries: int = 5) -> tuple:
    """
    Set otaApp.otaEnabled = True in device shadow desired state via Trimble API.
    Returns (success: bool, reason: str or None).
    """
    import time
    if not dsn or not app_device_id or not trimble_token:
        return False, "Missing required parameter (DSN, appDeviceId, or token)"

    headers = {
        "Authorization": f"Bearer {trimble_token}",
        "Content-Type": "application/json"
    }
    payload = {
        "AppInstanceId": TRIMBLE_APP_INSTANCE_ID,
        "AppDeviceId": app_device_id,
        "Action": "AutoSuppress",
        "OutboundMessage": {
            "Name": "",
            "Update": {
                "state": {
                    "desired": {
                        "otaApp": {
                            "otaEnabled": True
                        }
                    }
                }
            }
        }
    }

    for attempt in range(max_retries):
        try:
            response = requests.post(TRIMBLE_UPDATE_SHADOW_URL, json=payload, headers=headers, timeout=10)
            if response.status_code == 201:
                return True, None
            elif response.status_code == 401:
                return False, "401 Unauthorized - Trimble token expired or invalid"
            elif response.status_code == 404:
                return False, "404 Not Found - appDeviceId may be invalid"
            elif response.status_code == 400:
                try:
                    error_data = response.json()
                    detail = (error_data.get("message") or error_data.get("error")
                              or error_data.get("detail") or str(error_data))
                    return False, f"400 Bad Request - {detail}"
                except Exception:
                    return False, f"400 Bad Request - {response.text}"
            elif response.status_code >= 500:
                if attempt < max_retries - 1:
                    time.sleep(2 ** (attempt + 1))
                    continue
                return False, f"{response.status_code} Server error (max retries exceeded)"
            else:
                return False, f"{response.status_code} API error"
        except requests.Timeout:
            if attempt < max_retries - 1:
                time.sleep(2 ** (attempt + 1))
                continue
            return False, "Request timeout"
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                time.sleep(2 ** (attempt + 1))
                continue
            return False, f"Network error: {e}"
    return False, "Max retries exceeded"


def set_ota_desired_false(dsn: str, app_device_id: str, trimble_token: str, max_retries: int = 5) -> tuple:
    """
    Set otaApp.otaEnabled = False in device shadow desired state via Trimble API.
    Returns (success: bool, reason: str or None).
    """
    import time
    if not dsn or not app_device_id or not trimble_token:
        return False, "Missing required parameter (DSN, appDeviceId, or token)"

    headers = {
        "Authorization": f"Bearer {trimble_token}",
        "Content-Type": "application/json"
    }
    payload = {
        "AppInstanceId": TRIMBLE_APP_INSTANCE_ID,
        "AppDeviceId": app_device_id,
        "Action": "AutoSuppress",
        "OutboundMessage": {
            "Name": "",
            "Update": {
                "state": {
                    "desired": {
                        "otaApp": {
                            "otaEnabled": False
                        }
                    }
                }
            }
        }
    }

    for attempt in range(max_retries):
        try:
            response = requests.post(TRIMBLE_UPDATE_SHADOW_URL, json=payload, headers=headers, timeout=10)
            if response.status_code == 201:
                return True, None
            elif response.status_code == 401:
                return False, "401 Unauthorized - Trimble token expired or invalid"
            elif response.status_code == 404:
                return False, "404 Not Found - appDeviceId may be invalid"
            elif response.status_code == 400:
                try:
                    error_data = response.json()
                    detail = (error_data.get("message") or error_data.get("error")
                              or error_data.get("detail") or str(error_data))
                    return False, f"400 Bad Request - {detail}"
                except Exception:
                    return False, f"400 Bad Request - {response.text}"
            elif response.status_code >= 500:
                if attempt < max_retries - 1:
                    time.sleep(2 ** (attempt + 1))
                    continue
                return False, f"{response.status_code} Server error (max retries exceeded)"
            else:
                return False, f"{response.status_code} API error"
        except requests.Timeout:
            if attempt < max_retries - 1:
                time.sleep(2 ** (attempt + 1))
                continue
            return False, "Request timeout"
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                time.sleep(2 ** (attempt + 1))
                continue
            return False, f"Network error: {e}"
    return False, "Max retries exceeded"


def _reset_single_device(dsn: str, app_device_id: str, trimble_token: str) -> dict:
    """
    Worker: set otaEnabled=False, wait 60s, set otaEnabled=True for one device.
    Returns a result dict with dsn, app_device_id, status, reason, and failed_step.
    """
    import time as _time

    success, reason = set_ota_desired_false(dsn, app_device_id, trimble_token)
    if not success:
        return {"dsn": dsn, "app_device_id": app_device_id, "status": "Failed (set False)", "reason": reason, "failed_step": "set_false"}

    _time.sleep(60)

    success, reason = set_ota_desired_true(dsn, app_device_id, trimble_token)
    if success:
        return {"dsn": dsn, "app_device_id": app_device_id, "status": "Success", "reason": "", "failed_step": None}
    else:
        return {"dsn": dsn, "app_device_id": app_device_id, "status": "Failed (set True)", "reason": reason, "failed_step": "set_true"}


def reset_ota_shadow_for_devices(ota_false_df: pd.DataFrame, paccar_token: str, trimble_token: str) -> None:
    """
    Prompt once for the whole group, then in parallel (max 5 workers):
      1. Set otaApp.otaEnabled = False
      2. Wait 60 seconds (per worker)
      3. Set otaApp.otaEnabled = True
    """
    RESET_MAX_WORKERS = 5

    dsns = ota_false_df["dsn"].dropna().astype(str).str.strip().tolist()
    vins = ota_false_df.set_index(ota_false_df["dsn"].astype(str).str.strip())["vin"].to_dict()

    print(f"\nLooking up appDeviceId for {len(dsns):,} devices...")
    app_device_ids = {}
    with ThreadPoolExecutor(max_workers=PACCAR_MAX_WORKERS) as executor:
        futures = {executor.submit(lookup_app_device_id, dsn, paccar_token): dsn for dsn in dsns}
        for future in tqdm(as_completed(futures), total=len(dsns), desc="Looking up appDeviceId", unit="device"):
            dsn = futures[future]
            try:
                app_device_ids[dsn] = future.result()
            except PACCARAuthenticationError:
                print("[ERROR] PACCAR token expired during appDeviceId lookup. Aborting reset.")
                return
            except Exception:
                app_device_ids[dsn] = None

    actionable = [(dsn, app_device_ids[dsn]) for dsn in dsns if app_device_ids.get(dsn)]
    skipped = [dsn for dsn in dsns if not app_device_ids.get(dsn)]

    print(f"appDeviceId found for {len(actionable):,} / {len(dsns):,} devices")
    if skipped:
        print(f"Skipping {len(skipped):,} devices with no appDeviceId: {', '.join(skipped)}")

    if not actionable:
        return

    print(f"\nDevices to reset ({len(actionable):,}):")
    for dsn, _ in actionable:
        print(f"  DSN {dsn} / VIN {vins.get(dsn, 'unknown')}")

    answer = input(f"\nReset shadow (False → wait 60s → True) for all {len(actionable):,} devices? (y/n): ").strip().lower()
    if answer != "y":
        print("Reset cancelled.")
        return

    print(f"\nResetting {len(actionable):,} devices with up to {RESET_MAX_WORKERS} parallel workers...")
    results = []
    with ThreadPoolExecutor(max_workers=RESET_MAX_WORKERS) as executor:
        futures = {executor.submit(_reset_single_device, dsn, app_device_id, trimble_token): dsn
                   for dsn, app_device_id in actionable}
        for future in tqdm(as_completed(futures), total=len(actionable), desc="Resetting shadow", unit="device"):
            results.append(future.result())

    succeeded = sum(1 for r in results if r["status"] == "Success")
    failed = len(results) - succeeded
    print(f"\nReset complete: {succeeded:,} succeeded, {failed:,} failed")
    for r in results:
        if r["status"] != "Success":
            print(f"  [FAILED] DSN {r['dsn']}: {r['status']} - {r['reason']}")

    # Retry 401 failures with a new token
    auth_failures = [r for r in results if r["status"] != "Success" and "401" in (r["reason"] or "")]
    if auth_failures:
        print(f"\n[WARNING] {len(auth_failures):,} device(s) failed with 401 Unauthorized.")
        print("  Get token from: Browser DevTools (F12) > Network tab")
        print("  Find a request to: cloud.api.trimble.com")
        print("  Copy the Authorization header value (without 'Bearer ' prefix)")
        new_token = input("\n  Paste new Trimble token (or press Enter to skip retry): ").strip()
        if new_token.startswith("Bearer "):
            new_token = new_token[7:]
        if new_token:
            save_trimble_token(new_token)
            print(f"\nRetrying {len(auth_failures):,} failed devices...")
            retry_results = []
            with ThreadPoolExecutor(max_workers=RESET_MAX_WORKERS) as executor:
                futures = {}
                for r in auth_failures:
                    dsn = r["dsn"]
                    app_device_id = r["app_device_id"]
                    if r["failed_step"] == "set_true":
                        # Already waited 60s — just retry set True
                        futures[executor.submit(set_ota_desired_true, dsn, app_device_id, new_token)] = dsn
                    else:
                        # Full reset needed
                        futures[executor.submit(_reset_single_device, dsn, app_device_id, new_token)] = dsn
                for future in tqdm(as_completed(futures), total=len(auth_failures), desc="Retrying", unit="device"):
                    dsn = futures[future]
                    try:
                        raw = future.result()
                        if isinstance(raw, tuple):
                            success, reason = raw
                            retry_results.append({"dsn": dsn, "status": "Success" if success else "Failed (set True)", "reason": reason or ""})
                        else:
                            retry_results.append(raw)
                    except Exception as e:
                        retry_results.append({"dsn": dsn, "status": "Failed", "reason": str(e)})
            retry_ok = sum(1 for r in retry_results if r["status"] == "Success")
            retry_fail = len(retry_results) - retry_ok
            print(f"Retry complete: {retry_ok:,} succeeded, {retry_fail:,} failed")
            for r in retry_results:
                if r["status"] != "Success":
                    print(f"  [FAILED] DSN {r['dsn']}: {r['status']} - {r['reason']}")


def enable_ota_for_devices(df: pd.DataFrame, paccar_token: str, trimble_token: str) -> None:
    """
    For each device in df, lookup appDeviceId then set otaApp.otaEnabled = True.
    Prints a progress summary and saves a results report.
    """
    import time as _time

    dsns = df["dsn"].dropna().astype(str).str.strip().tolist()
    print(f"\nLooking up appDeviceId for {len(dsns):,} devices...")

    # Step 1: lookup appDeviceIds in parallel
    app_device_ids = {}
    with ThreadPoolExecutor(max_workers=PACCAR_MAX_WORKERS) as executor:
        futures = {executor.submit(lookup_app_device_id, dsn, paccar_token): dsn for dsn in dsns}
        for future in tqdm(as_completed(futures), total=len(dsns), desc="Looking up appDeviceId", unit="device"):
            dsn = futures[future]
            try:
                app_device_ids[dsn] = future.result()
            except PACCARAuthenticationError:
                print("[ERROR] PACCAR token expired during appDeviceId lookup. Aborting enable step.")
                return
            except Exception:
                app_device_ids[dsn] = None

    found = sum(1 for v in app_device_ids.values() if v)
    print(f"appDeviceId found for {found:,} / {len(dsns):,} devices")

    if found == 0:
        print("[WARNING] No appDeviceIds found. Cannot proceed with enable.")
        return

    # Step 2: send shadow update for each device with a valid appDeviceId
    print(f"\nSetting otaApp.otaEnabled = True...")
    results = []
    successful = 0
    failed = 0

    for dsn in tqdm(dsns, desc="Enabling OTA", unit="device"):
        app_device_id = app_device_ids.get(dsn)
        if not app_device_id:
            results.append({"dsn": dsn, "status": "Skipped", "reason": "No appDeviceId found"})
            failed += 1
            continue

        success, reason = set_ota_desired_true(dsn, app_device_id, trimble_token)
        results.append({
            "dsn": dsn,
            "status": "Success" if success else "Failed",
            "reason": reason or ""
        })
        if success:
            successful += 1
        else:
            failed += 1

    print(f"\nOTA enable complete: {successful:,} succeeded, {failed:,} failed/skipped")

    # Save results report
    results_df = pd.DataFrame(results)
    filepath = save_results_to_csv(results_df, filename=get_report_filename("tig_ota_enable_results"))
    print(f"Enable results saved: {filepath}")


def fetch_shadow_state(dsn: str, token: str, max_retries: int = 5) -> tuple:
    """
    Fetch shadow state for a device from PACCAR device-config API.
    Returns (shadow_dict, error_code) where:
      - shadow_dict is the JSON response on success, or None on failure
      - error_code is None on success, or a string like "401", "404", "timeout", "connection_error"
    """
    import time
    url = f"{SHADOW_ENDPOINT}/{dsn}"
    headers = {
        "X-Auth-Token": token,
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
    }

    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=PACCAR_TIMEOUT)
            if response.status_code == 200:
                return response.json(), None
            elif response.status_code >= 500:
                if attempt < max_retries - 1:
                    time.sleep(2 ** (attempt + 1))
                    continue
                return None, str(response.status_code)
            else:
                return None, str(response.status_code)
        except requests.Timeout:
            if attempt < max_retries - 1:
                time.sleep(2 ** (attempt + 1))
                continue
            return None, "timeout"
        except requests.ConnectionError:
            if attempt < max_retries - 1:
                time.sleep(2 ** (attempt + 1))
                continue
            return None, "connection_error"
        except requests.RequestException as e:
            return None, f"request_error: {e}"
    return None, "max_retries"


def extract_ota_enabled(shadow_state: Optional[dict]) -> tuple:
    """
    Extract otaApp.otaEnabled from reported and desired shadow state.
    Returns (reported_ota_enabled, desired_ota_enabled) - each is True/False/None.
    """
    if not shadow_state:
        return None, None
    try:
        reported = shadow_state.get("reported", {}) or {}
        desired = shadow_state.get("desired", {}) or {}
        reported_val = reported.get("otaApp", {}).get("otaEnabled")
        desired_val = desired.get("otaApp", {}).get("otaEnabled")
        return reported_val, desired_val
    except (KeyError, TypeError, AttributeError):
        return None, None


def retrieve_ota_shadow_data(dsns: list, token: str) -> dict:
    """
    Retrieve otaApp.otaEnabled shadow values for a list of DSNs in parallel.
    Returns dict mapping dsn -> {"reported": bool|None, "desired": bool|None}.
    """
    from collections import Counter
    results = {}
    error_counts = Counter()

    def fetch_one(dsn):
        shadow, error_code = fetch_shadow_state(dsn, token)
        reported, desired = extract_ota_enabled(shadow)
        return dsn, reported, desired, error_code

    with ThreadPoolExecutor(max_workers=SHADOW_MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_one, dsn): dsn for dsn in dsns}
        for future in tqdm(as_completed(futures), total=len(dsns), desc="Fetching OTA shadow state", unit="device"):
            try:
                dsn, reported, desired, error_code = future.result()
                results[dsn] = {"reported": reported, "desired": desired}
                if error_code:
                    error_counts[error_code] += 1
                else:
                    error_counts["200 OK"] += 1
            except Exception as e:
                dsn = futures[future]
                results[dsn] = {"reported": None, "desired": None}
                error_counts[f"exception: {type(e).__name__}"] += 1

    # Print API response summary
    print(f"\nShadow API response breakdown ({len(dsns):,} requests):")
    for code, count in sorted(error_counts.items()):
        print(f"  {code}: {count:,}")

    # If everything came back 401, the token is expired
    if error_counts.get("401", 0) == len(dsns):
        raise PACCARAuthenticationError("All shadow requests returned 401 - PACCAR token is expired or invalid")

    return results


def _analyze_pmg_units(df: pd.DataFrame) -> None:
    """Branch 1: PMG units with DSN in range 7,100,000 - 8,000,000."""
    print("="*70)
    print("Branch 1: PMG Units (DSN 7,100,000 - 8,000,000)")
    print("="*70)

    if "dsn" not in df.columns:
        print("[WARNING] 'dsn' column not found. Cannot filter PMG units.")
        return

    work = df.copy()
    work["_dsn_num"] = pd.to_numeric(work["dsn"], errors="coerce")
    mask = (work["_dsn_num"] >= 7_100_000) & (work["_dsn_num"] <= 8_000_000)
    result = work[mask].drop(columns=["_dsn_num"])

    print(f"PMG units found: {len(result):,}")
    if len(result) > 0:
        print("\nSample (up to 10):")
        print(result.head(10).to_string(index=False))


def _analyze_tig_units(df: pd.DataFrame) -> None:
    """Branch 2: TIG units with DSN in range 20,000,000 - 30,000,000 and firmware 002.003.006 or 002.003.007."""
    print("="*70)
    print("Branch 2: TIG Units (DSN 20,000,000 - 30,000,000, firmware 002.003.006 or 002.003.007)")
    print("="*70)

    if "dsn" not in df.columns:
        print("[WARNING] 'dsn' column not found. Cannot filter TIG units.")
        return

    work = df.copy()
    work["_dsn_num"] = pd.to_numeric(work["dsn"], errors="coerce")
    dsn_mask = (work["_dsn_num"] >= 20_000_000) & (work["_dsn_num"] <= 30_000_000)

    if "pmgSwVersion" in work.columns:
        fw_mask = work["pmgSwVersion"].astype(str).str.upper().isin(["002.003.006", "002.003.007"])
        result = work[dsn_mask & fw_mask].drop(columns=["_dsn_num"])
    else:
        print("[WARNING] 'pmgSwVersion' column not found. Filtering by DSN range only.")
        result = work[dsn_mask].drop(columns=["_dsn_num"])

    print(f"TIG units found: {len(result):,}")

    if len(result) == 0:
        return

    # Shadow retrieval step
    print("\n" + "-"*70)
    fetch_shadow = input("Retrieve otaApp.otaEnabled shadow values for these TIG units? (y/n): ").strip().lower()
    if fetch_shadow != "y":
        return

    token = load_paccar_token()
    if not token:
        token = input("Enter PACCAR API bearer token (or press Enter to skip): ").strip()
        if not token:
            print("[WARNING] No PACCAR token available. Skipping shadow retrieval.")
            return
        save_paccar_token(token)

    dsns = result["dsn"].dropna().astype(str).str.strip().unique().tolist()
    try:
        shadow_results = retrieve_ota_shadow_data(dsns, token)
    except PACCARAuthenticationError:
        print("[WARNING] PACCAR token expired. Attempting to refresh...")
        refreshed = refresh_paccar_token(token)
        if refreshed:
            print("Token refreshed successfully.")
            save_paccar_token(refreshed)
            token = refreshed
        else:
            print("Token refresh failed. Please provide a new token.")
            print("Get token from: Chrome > DevTools (F12) > Application > Local Storage > https://paccarsolutions.com")
            print("Find key: pnet.portal.encodedToken")
            token = input("Enter new PACCAR API bearer token (or press Enter to skip): ").strip()
            if not token:
                print("Skipping shadow retrieval.")
                return
            save_paccar_token(token)
        try:
            shadow_results = retrieve_ota_shadow_data(dsns, token)
        except PACCARAuthenticationError:
            print("[ERROR] Token still returning 401 after refresh/replacement. Skipping shadow retrieval.")
            return

    # Map shadow values back onto result
    result = result.copy()
    result["ota_reported"] = result["dsn"].astype(str).map(lambda d: shadow_results.get(d, {}).get("reported"))
    result["ota_desired"] = result["dsn"].astype(str).map(lambda d: shadow_results.get(d, {}).get("desired"))

    # Summary counts
    total = len(result)
    reported_true = (result["ota_reported"] == True).sum()
    reported_false = (result["ota_reported"] == False).sum()
    reported_none = result["ota_reported"].isna().sum()
    desired_true = (result["ota_desired"] == True).sum()
    desired_false = (result["ota_desired"] == False).sum()
    desired_none = result["ota_desired"].isna().sum()

    print(f"\nOTA Shadow Summary ({total:,} units):")
    print(f"  Reported otaEnabled: True={reported_true:,}  False={reported_false:,}  None/missing={reported_none:,}")
    print(f"  Desired  otaEnabled: True={desired_true:,}  False={desired_false:,}  None/missing={desired_none:,}")
    print("\nSample (up to 10):")
    print(result[["vin", "dsn", "ota_reported", "ota_desired"]].head(10).to_string(index=False))

    # Filter to ota_reported == False
    print("\n" + "-"*70)
    filter_false = input("Filter to units where ota_reported = False? (y/n): ").strip().lower()
    if filter_false == "y":
        ota_false = result[result["ota_reported"] == False]
        print(f"\nUnits with ota_reported = False: {len(ota_false):,}")
        if len(ota_false) > 0:
            print("\nSample (up to 10):")
            print(ota_false[["vin", "dsn", "ota_reported", "ota_desired"]].head(10).to_string(index=False))

            both_false = ota_false[ota_false["ota_desired"] == False]
            print(f"\n  Of these, units where ota_desired is also False: {len(both_false):,}")
            if len(both_false) > 0:
                print("\n  Sample (up to 10):")
                print(both_false[["vin", "dsn", "ota_reported", "ota_desired"]].head(10).to_string(index=False))

                # Prompt to enable OTA desired for both_false devices
                print("\n" + "-"*70)
                do_enable = input(f"Set ota_desired = True for these {len(both_false):,} units? (y/n): ").strip().lower()
                if do_enable == "y":
                    # Ensure we have a PACCAR token
                    paccar_token = load_paccar_token()
                    if not paccar_token:
                        paccar_token = input("Enter PACCAR API bearer token (or press Enter to skip): ").strip()
                        if not paccar_token:
                            print("[WARNING] No PACCAR token. Skipping enable step.")
                        else:
                            save_paccar_token(paccar_token)

                    if paccar_token:
                        # Get Trimble token
                        trimble_token = load_trimble_token()
                        if trimble_token:
                            print("Using cached Trimble token...")
                        else:
                            print("\n  Trimble API token required.")
                            print("  Get token from: Browser DevTools (F12) > Network tab")
                            print("  Find a request to: cloud.api.trimble.com")
                            print("  Copy the Authorization header value (without 'Bearer ' prefix)")
                            trimble_token = input("\n  Paste Trimble token (or press Enter to skip): ").strip()
                            if trimble_token.startswith("Bearer "):
                                trimble_token = trimble_token[7:]
                            if trimble_token:
                                save_trimble_token(trimble_token)

                        if trimble_token:
                            enable_ota_for_devices(both_false, paccar_token, trimble_token)
                        else:
                            print("[WARNING] No Trimble token provided. Skipping enable step.")

            # Reset shadow only for reported=False / desired=True units (not both_false — those just need enable)
            reset_candidates = ota_false[ota_false["ota_desired"] != False]
            print("\n" + "-"*70)
            do_reset = input(f"Reset shadow (False → wait 60s → True) for the {len(reset_candidates):,} ota_reported=False / ota_desired=True units? (y/n): ").strip().lower()
            if do_reset == "y":
                paccar_token = load_paccar_token()
                if not paccar_token:
                    paccar_token = input("Enter PACCAR API bearer token (or press Enter to skip): ").strip()
                    if not paccar_token:
                        print("[WARNING] No PACCAR token. Skipping reset step.")
                    else:
                        save_paccar_token(paccar_token)

                if paccar_token:
                    trimble_token = load_trimble_token()
                    if trimble_token:
                        print("Using cached Trimble token...")
                    else:
                        print("\n  Trimble API token required.")
                        print("  Get token from: Browser DevTools (F12) > Network tab")
                        print("  Find a request to: cloud.api.trimble.com")
                        print("  Copy the Authorization header value (without 'Bearer ' prefix)")
                        trimble_token = input("\n  Paste Trimble token (or press Enter to skip): ").strip()
                        if trimble_token.startswith("Bearer "):
                            trimble_token = trimble_token[7:]
                        if trimble_token:
                            save_trimble_token(trimble_token)

                    if trimble_token:
                        reset_ota_shadow_for_devices(reset_candidates, paccar_token, trimble_token)
                    else:
                        print("[WARNING] No Trimble token provided. Skipping reset step.")

            filepath = save_results_to_csv(ota_false, filename=get_report_filename("tig_ota_disabled"))
            print(f"\nReport saved: {filepath}")

    # Filter to ota_reported == True with deprovision exclusion
    print("\n" + "-"*70)
    filter_true = input("Filter to units where ota_reported = True? (y/n): ").strip().lower()
    if filter_true == "y":
        ota_true = result[result["ota_reported"] == True].copy()
        total_true = len(ota_true)
        print(f"\nUnits with ota_reported = True: {total_true:,}")

        DEPROVISION_FILE = "Units Impacted by PACCARs Deprovision Script.csv"
        excluded_count = 0
        try:
            deprov_df = pd.read_csv(DEPROVISION_FILE)
            vin_col = next((c for c in deprov_df.columns if c.strip().lower() == "vin"), None)
            if vin_col:
                excluded_vins = set(deprov_df[vin_col].dropna().astype(str).str.strip().str.upper())
                before = len(ota_true)
                ota_true = ota_true[~ota_true["vin"].astype(str).str.strip().str.upper().isin(excluded_vins)]
                excluded_count = before - len(ota_true)
            else:
                print(f"[WARNING] No 'Vin' column found in {DEPROVISION_FILE}. Skipping exclusion.")
        except FileNotFoundError:
            print(f"[WARNING] {DEPROVISION_FILE} not found. Skipping exclusion.")
        except Exception as e:
            print(f"[WARNING] Could not load {DEPROVISION_FILE}: {e}. Skipping exclusion.")

        print(f"  Excluded by deprovision file: {excluded_count:,}")
        print(f"  Remaining: {len(ota_true):,}")

        if len(ota_true) > 0:
            filepath = save_results_to_csv(ota_true, filename=get_report_filename("tig_ota_true"))
            print(f"\nReport saved: {filepath}")


def load_nexus_token() -> Optional[str]:
    """Load cached Nexus (PlatformScience) token from file."""
    try:
        with open(NEXUS_TOKEN_FILE, "r") as f:
            token = f.read().strip()
            if token:
                return token
    except Exception:
        pass
    return None


def save_nexus_token(token: str) -> None:
    """Save Nexus (PlatformScience) token to file."""
    try:
        with open(NEXUS_TOKEN_FILE, "w") as f:
            f.write(token.strip())
    except Exception as e:
        print(f"[WARNING] Could not save Nexus token: {e}")


def set_nexus_ota_desired(dsn: str, nexus_token: str, enabled: bool, max_retries: int = 5) -> tuple:
    """
    Set otaApp.otaEnabled for a Nexus device via PlatformScience cf-gateway API.
    Returns (success: bool, reason: str or None).
    """
    import uuid
    import time

    if not dsn or not nexus_token:
        return False, "Missing required parameter (DSN or token)"

    headers = {
        "Authorization": f"Bearer {nexus_token}",
        "Content-Type": "application/json",
        "x-application-customer": NEXUS_APP_CUSTOMER_ID,
    }
    payload = {
        "requestId": str(uuid.uuid4()),
        "statusTopic": "",
        "requests": [{
            "deviceType": "yogi",
            "deviceIds": [str(dsn)],
            "data": {
                "otaApp": {
                    "otaEnabled": enabled
                }
            }
        }]
    }

    for attempt in range(max_retries):
        try:
            response = requests.post(NEXUS_SHADOW_URL, json=payload, headers=headers, timeout=10)
            if response.status_code == 200:
                return True, None
            elif response.status_code == 401:
                return False, "401 Unauthorized - Nexus token expired or invalid"
            elif response.status_code == 404:
                return False, "404 Not Found"
            elif response.status_code == 400:
                try:
                    error_data = response.json()
                    detail = (error_data.get("message") or error_data.get("error")
                              or error_data.get("detail") or str(error_data))
                    return False, f"400 Bad Request - {detail}"
                except Exception:
                    return False, f"400 Bad Request - {response.text}"
            elif response.status_code >= 500:
                if attempt < max_retries - 1:
                    time.sleep(2 ** (attempt + 1))
                    continue
                return False, f"{response.status_code} Server error (max retries exceeded)"
            else:
                return False, f"{response.status_code} API error"
        except requests.Timeout:
            if attempt < max_retries - 1:
                time.sleep(2 ** (attempt + 1))
                continue
            return False, "Request timeout"
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                time.sleep(2 ** (attempt + 1))
                continue
            return False, f"Network error: {e}"
    return False, "Max retries exceeded"


def enable_nexus_ota_for_devices(df: pd.DataFrame, nexus_token: str) -> None:
    """Set otaApp.otaEnabled = True for each Nexus device in df."""
    dsns = df["dsn"].dropna().astype(str).str.strip().tolist()
    print(f"\nSetting otaEnabled = True for {len(dsns):,} Nexus devices...")
    results = []
    succeeded = 0
    failed = 0
    for dsn in tqdm(dsns, desc="Enabling OTA (Nexus)", unit="device"):
        success, reason = set_nexus_ota_desired(dsn, nexus_token, enabled=True)
        results.append({"dsn": dsn, "status": "Success" if success else "Failed", "reason": reason or ""})
        if success:
            succeeded += 1
        else:
            failed += 1
    print(f"\nNexus OTA enable complete: {succeeded:,} succeeded, {failed:,} failed")
    results_df = pd.DataFrame(results)
    filepath = save_results_to_csv(results_df, filename=get_report_filename("nexus_ota_enable_results"))
    print(f"Enable results saved: {filepath}")


def _reset_nexus_single_device(dsn: str, nexus_token: str) -> dict:
    """
    Worker: set otaEnabled=False, wait 60s, set otaEnabled=True for one Nexus device.
    Returns a result dict with dsn, status, reason, and failed_step.
    """
    import time as _time

    success, reason = set_nexus_ota_desired(dsn, nexus_token, enabled=False)
    if not success:
        return {"dsn": dsn, "status": "Failed (set False)", "reason": reason, "failed_step": "set_false"}

    _time.sleep(60)

    success, reason = set_nexus_ota_desired(dsn, nexus_token, enabled=True)
    if success:
        return {"dsn": dsn, "status": "Success", "reason": "", "failed_step": None}
    else:
        return {"dsn": dsn, "status": "Failed (set True)", "reason": reason, "failed_step": "set_true"}


def reset_nexus_ota_shadow_for_devices(ota_false_df: pd.DataFrame, nexus_token: str) -> None:
    """
    Prompt once for the whole group, then in parallel (max NEXUS_RESET_MAX_WORKERS):
      1. Set otaApp.otaEnabled = False
      2. Wait 60 seconds (per worker)
      3. Set otaApp.otaEnabled = True
    """
    dsns = ota_false_df["dsn"].dropna().astype(str).str.strip().tolist()
    vins = ota_false_df.set_index(ota_false_df["dsn"].astype(str).str.strip())["vin"].to_dict()

    print(f"\nDevices to reset ({len(dsns):,}):")
    for dsn in dsns:
        print(f"  DSN {dsn} / VIN {vins.get(dsn, 'unknown')}")

    answer = input(f"\nReset shadow (False → wait 60s → True) for all {len(dsns):,} Nexus devices? (y/n): ").strip().lower()
    if answer != "y":
        print("Reset cancelled.")
        return

    print(f"\nResetting {len(dsns):,} devices with up to {NEXUS_RESET_MAX_WORKERS} parallel workers...")
    results = []
    with ThreadPoolExecutor(max_workers=NEXUS_RESET_MAX_WORKERS) as executor:
        futures = {executor.submit(_reset_nexus_single_device, dsn, nexus_token): dsn for dsn in dsns}
        for future in tqdm(as_completed(futures), total=len(dsns), desc="Resetting shadow (Nexus)", unit="device"):
            results.append(future.result())

    succeeded = sum(1 for r in results if r["status"] == "Success")
    failed = len(results) - succeeded
    print(f"\nReset complete: {succeeded:,} succeeded, {failed:,} failed")
    for r in results:
        if r["status"] != "Success":
            print(f"  [FAILED] DSN {r['dsn']}: {r['status']} - {r['reason']}")

    # Retry 401 failures with a new token
    auth_failures = [r for r in results if r["status"] != "Success" and "401" in (r["reason"] or "")]
    if auth_failures:
        print(f"\n[WARNING] {len(auth_failures):,} device(s) failed with 401 Unauthorized.")
        new_token = _prompt_nexus_token()
        if new_token:
            print(f"\nRetrying {len(auth_failures):,} failed devices...")
            retry_results = []
            with ThreadPoolExecutor(max_workers=NEXUS_RESET_MAX_WORKERS) as executor:
                futures = {}
                for r in auth_failures:
                    dsn = r["dsn"]
                    if r["failed_step"] == "set_true":
                        # Already waited 60s — just retry set True
                        futures[executor.submit(set_nexus_ota_desired, dsn, new_token, True)] = dsn
                    else:
                        # Full reset needed
                        futures[executor.submit(_reset_nexus_single_device, dsn, new_token)] = dsn
                for future in tqdm(as_completed(futures), total=len(auth_failures), desc="Retrying (Nexus)", unit="device"):
                    dsn = futures[future]
                    try:
                        raw = future.result()
                        if isinstance(raw, tuple):
                            success, reason = raw
                            retry_results.append({"dsn": dsn, "status": "Success" if success else "Failed (set True)", "reason": reason or ""})
                        else:
                            retry_results.append(raw)
                    except Exception as e:
                        retry_results.append({"dsn": dsn, "status": "Failed", "reason": str(e)})
            retry_ok = sum(1 for r in retry_results if r["status"] == "Success")
            retry_fail = len(retry_results) - retry_ok
            print(f"Retry complete: {retry_ok:,} succeeded, {retry_fail:,} failed")
            for r in retry_results:
                if r["status"] != "Success":
                    print(f"  [FAILED] DSN {r['dsn']}: {r['status']} - {r['reason']}")


def _prompt_nexus_token() -> Optional[str]:
    """Prompt the user to paste a Nexus (PlatformScience) bearer token."""
    print("\n  Nexus (PlatformScience) token required.")
    print("  Get token from: Browser DevTools (F12) > Network tab")
    print("  Find a request to: cf-api.mc2.telematicsplatform.io")
    print("  Copy the Authorization header value (without 'Bearer ' prefix)")
    token = input("\n  Paste Nexus token (or press Enter to skip): ").strip()
    if token.startswith("Bearer "):
        token = token[7:]
    if token:
        save_nexus_token(token)
        return token
    return None


def _analyze_tig_nexus_units(df: pd.DataFrame) -> None:
    """Branch 3: TIG units running Nexus firmware 002.004.005, 007.003.026, or 007.002.026, DSN 200,000 - 30,000,000."""
    print("\n" + "="*70)
    print("Branch 3: TIG Nexus Firmware Units (DSN 200,000 - 30,000,000, firmware 002.004.005 / 007.003.026 / 007.002.026)")
    print("="*70)

    if "dsn" not in df.columns:
        print("[WARNING] 'dsn' column not found. Cannot filter TIG Nexus units.")
        return

    work = df.copy()
    work["_dsn_num"] = pd.to_numeric(work["dsn"], errors="coerce")
    dsn_mask = (work["_dsn_num"] >= 200_000) & (work["_dsn_num"] <= 30_000_000)

    if "pmgSwVersion" in work.columns:
        fw_mask = work["pmgSwVersion"].astype(str).str.upper().isin(["002.004.005", "007.003.026", "007.002.026"])
        result = work[dsn_mask & fw_mask].drop(columns=["_dsn_num"])
    else:
        print("[WARNING] 'pmgSwVersion' column not found. Filtering by DSN range only.")
        result = work[dsn_mask].drop(columns=["_dsn_num"])

    print(f"TIG Nexus units found: {len(result):,}")

    if len(result) == 0:
        return

    # Shadow retrieval step
    print("\n" + "-"*70)
    fetch_shadow = input("Retrieve otaApp.otaEnabled shadow values for these TIG Nexus units? (y/n): ").strip().lower()
    if fetch_shadow != "y":
        return

    token = load_paccar_token()
    if not token:
        token = input("Enter PACCAR API bearer token (or press Enter to skip): ").strip()
        if not token:
            print("[WARNING] No PACCAR token available. Skipping shadow retrieval.")
            return
        save_paccar_token(token)

    dsns = result["dsn"].dropna().astype(str).str.strip().unique().tolist()
    try:
        shadow_results = retrieve_ota_shadow_data(dsns, token)
    except PACCARAuthenticationError:
        print("[WARNING] PACCAR token expired. Attempting to refresh...")
        refreshed = refresh_paccar_token(token)
        if refreshed:
            print("Token refreshed successfully.")
            save_paccar_token(refreshed)
            token = refreshed
        else:
            print("Token refresh failed. Please provide a new token.")
            print("Get token from: Chrome > DevTools (F12) > Application > Local Storage > https://paccarsolutions.com")
            print("Find key: pnet.portal.encodedToken")
            token = input("Enter new PACCAR API bearer token (or press Enter to skip): ").strip()
            if not token:
                print("Skipping shadow retrieval.")
                return
            save_paccar_token(token)
        try:
            shadow_results = retrieve_ota_shadow_data(dsns, token)
        except PACCARAuthenticationError:
            print("[ERROR] Token still returning 401 after refresh/replacement. Skipping shadow retrieval.")
            return

    # Map shadow values back onto result
    result = result.copy()
    result["ota_reported"] = result["dsn"].astype(str).map(lambda d: shadow_results.get(d, {}).get("reported"))
    result["ota_desired"] = result["dsn"].astype(str).map(lambda d: shadow_results.get(d, {}).get("desired"))

    # Summary counts
    total = len(result)
    reported_true = (result["ota_reported"] == True).sum()
    reported_false = (result["ota_reported"] == False).sum()
    reported_none = result["ota_reported"].isna().sum()
    desired_true = (result["ota_desired"] == True).sum()
    desired_false = (result["ota_desired"] == False).sum()
    desired_none = result["ota_desired"].isna().sum()

    print(f"\nOTA Shadow Summary ({total:,} units):")
    print(f"  Reported otaEnabled: True={reported_true:,}  False={reported_false:,}  None/missing={reported_none:,}")
    print(f"  Desired  otaEnabled: True={desired_true:,}  False={desired_false:,}  None/missing={desired_none:,}")
    print("\nSample (up to 10):")
    print(result[["vin", "dsn", "ota_reported", "ota_desired"]].head(10).to_string(index=False))

    # Filter to ota_reported == False
    print("\n" + "-"*70)
    filter_false = input("Filter to units where ota_reported = False? (y/n): ").strip().lower()
    if filter_false == "y":
        ota_false = result[result["ota_reported"] == False]
        print(f"\nUnits with ota_reported = False: {len(ota_false):,}")
        if len(ota_false) > 0:
            print("\nSample (up to 10):")
            print(ota_false[["vin", "dsn", "ota_reported", "ota_desired"]].head(10).to_string(index=False))

            # ota_desired = False or None → just update shadow (enable), no reset needed
            needs_enable = ota_false[ota_false["ota_desired"].isna() | (ota_false["ota_desired"] == False)]
            print(f"\n  Units where ota_desired = False or None (update shadow only): {len(needs_enable):,}")
            if len(needs_enable) > 0:
                print("\n  Sample (up to 10):")
                print(needs_enable[["vin", "dsn", "ota_reported", "ota_desired"]].head(10).to_string(index=False))

                print("\n" + "-"*70)
                do_enable = input(f"Set ota_desired = True for these {len(needs_enable):,} units? (y/n): ").strip().lower()
                if do_enable == "y":
                    nexus_token = load_nexus_token()
                    if nexus_token:
                        print("Using cached Nexus token...")
                    else:
                        nexus_token = _prompt_nexus_token()

                    if nexus_token:
                        enable_nexus_ota_for_devices(needs_enable, nexus_token)
                    else:
                        print("[WARNING] No Nexus token provided. Skipping enable step.")

            # ota_desired = True → reset shadow (False → 60s → True)
            reset_candidates = ota_false[ota_false["ota_desired"] == True]
            print(f"\n  Units where ota_desired = True (reset shadow): {len(reset_candidates):,}")
            if len(reset_candidates) > 0:
                print("\n  Sample (up to 10):")
                print(reset_candidates[["vin", "dsn", "ota_reported", "ota_desired"]].head(10).to_string(index=False))

            print("\n" + "-"*70)
            do_reset = input(f"Reset shadow (False → wait 60s → True) for the {len(reset_candidates):,} ota_reported=False / ota_desired=True units? (y/n): ").strip().lower()
            if do_reset == "y":
                nexus_token = load_nexus_token()
                if nexus_token:
                    print("Using cached Nexus token...")
                else:
                    nexus_token = _prompt_nexus_token()

                if nexus_token:
                    reset_nexus_ota_shadow_for_devices(reset_candidates, nexus_token)
                else:
                    print("[WARNING] No Nexus token provided. Skipping reset step.")

            filepath = save_results_to_csv(ota_false, filename=get_report_filename("nexus_ota_disabled"))
            print(f"\nReport saved: {filepath}")


def _analyze_tig_other_units(df: pd.DataFrame) -> None:
    """Branch 4: TIG units (DSN 20,000,000 - 30,000,000) not matched by Branch 2 or Branch 3."""
    print("="*70)
    print("Branch 4: All Other TIG Units (DSN 20,000,000 - 30,000,000)")
    print("="*70)

    if "dsn" not in df.columns:
        print("[WARNING] 'dsn' column not found. Cannot filter TIG units.")
        return

    work = df.copy()
    work["_dsn_num"] = pd.to_numeric(work["dsn"], errors="coerce")
    dsn_mask = (work["_dsn_num"] >= 20_000_000) & (work["_dsn_num"] <= 30_000_000)

    if "pmgSwVersion" in work.columns:
        fw = work["pmgSwVersion"].astype(str).str.upper()
        branch2_mask = fw.isin(["002.003.006", "002.003.007"])
        branch3_mask = fw.isin(["002.004.005", "007.003.026", "007.002.026"])
        result = work[dsn_mask & ~branch2_mask & ~branch3_mask].drop(columns=["_dsn_num"])
    else:
        print("[WARNING] 'pmgSwVersion' column not found. Cannot exclude Branch 2/3 firmware versions.")
        result = work[dsn_mask].drop(columns=["_dsn_num"])

    print(f"Other TIG units (not matching Branch 2 or Branch 3): {len(result):,}")
    if len(result) > 0:
        if "pmgSwVersion" in result.columns:
            fw_counts = result["pmgSwVersion"].value_counts(dropna=False)
            print("\nFirmware version breakdown:")
            print(fw_counts.to_string())
        print("\nSample (up to 10):")
        print(result.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
