import io
import sys
import os
import logging
import datetime
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed, CancelledError
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
TDMG_RESET_HISTORY_FILE = "reports/tdmg_reset_history.csv"
TDMG_RESET_COOLDOWN_HOURS = 24
TDMG_REMEDIATION_COOLDOWN_HOURS = 24

AZURE_HISTORY_FILE = "reports/azure_history.csv"
AZURE_REBOOT_COOLDOWN_HOURS = 24
AZURE_REMEDIATION_COOLDOWN_HOURS = 24
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
NEXUS_LOG_REQUEST_URL = "https://cf-api.mc2.telematicsplatform.io/peoplenet/cf-gateway/v1/transportation/device/v1/devices/health/logfiles/request/types/gateway/{dsn}"
AZURE_REBOOT_URL = "https://cf-api.mc2.telematicsplatform.io/peoplenet/cf-gateway/v1/v2/application/send"
AZURE_REBOOT_DELAY_SECONDS = 15
AZURE_SHADOW_ARCHIVE_URL = "https://cf-api.mc2.telematicsplatform.io/peoplenet/cf-gateway/v1/transportation/device/comms/v1/archive/devices/{dsn}/events/configurations"
AZURE_SHADOW_LOOKBACK_DAYS = 90

BB_BASE_URL = "https://paccar.jarvis.blackberry.com/api"
BB_TOKEN_FILE = ".bb_token"
BB_MAX_WORKERS = 5

PACCAR_SOFTWARE_BASE_URL = "https://security-gateway-rp.fleethealth.io"
PACCAR_USER_ID = "b29c9ed6-d9e1-4c29-b2e9-ba9beb94dc14"
TRIMBLE_MQTT_URL = "https://cloud.api.trimble.com/devicegateway/management/1.0/OutboundMessage/SendMqttMessage"

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

    # Normalize DSN column to plain integer strings (prevents "20025982.0" from float-read CSVs)
    if "dsn" in pending_df.columns:
        pending_df["dsn"] = pending_df["dsn"].dropna().apply(
            lambda x: str(int(float(str(x).strip()))) if str(x).strip() not in ("", "nan") else x
        ).reindex(pending_df.index)

    # --- Branches 1-5: all operate on pending_df (data after step 3) ---

    # Branch 1: PMG units
    print("\n" + "="*70)
    run_pmg = input("Analyze PMG units? (y/n): ").strip().lower()
    if run_pmg == "y":
        _analyze_pmg_units(pending_df)

    # Branch 2: TIG TDMG units
    print("\n" + "="*70)
    run_tig = input("Analyze TIG TDMG units? (y/n): ").strip().lower()
    if run_tig == "y":
        _analyze_tig_units(pending_df)

    # Branch 3: TIG Azure units
    print("\n" + "="*70)
    run_tig_azure = input("Analyze TIG Azure units? (y/n): ").strip().lower()
    if run_tig_azure == "y":
        _analyze_tig_azure_units(pending_df)

    # Branch 4: TIG Nexus firmware
    print("\n" + "="*70)
    run_nexus = input("Analyze TIG Nexus firmware units? (y/n): ").strip().lower()
    if run_nexus == "y":
        _analyze_tig_nexus_units(pending_df)

    # Branch 5: All other TIG units
    print("\n" + "="*70)
    run_tig_other = input("Analyze all other TIG units? (y/n): ").strip().lower()
    if run_tig_other == "y":
        _analyze_tig_other_units(pending_df)

    # Final step: Trigger Enablement Flow Again
    print("\n" + "="*70)
    run_enable_flow = input("Trigger Enablement Flow Again? (y/n): ").strip().lower()
    if run_enable_flow == "y":
        _trigger_enablement_flow(pending_df)


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


def _load_tdmg_reset_history() -> pd.DataFrame:
    """Load TDMG reset/remediation history from CSV, returning an empty DataFrame if not found."""
    _COLS = ["dsn", "vin", "reset_count", "last_reset", "remediation_count", "last_remediation", "enable_count", "last_enable"]
    try:
        df = pd.read_csv(TDMG_RESET_HISTORY_FILE, dtype={"dsn": str})
        for col in _COLS:
            if col not in df.columns:
                df[col] = None
        df["last_reset"] = pd.to_datetime(df["last_reset"], utc=True, errors="coerce")
        df["last_remediation"] = pd.to_datetime(df["last_remediation"], utc=True, errors="coerce")
        df["last_enable"] = pd.to_datetime(df["last_enable"], utc=True, errors="coerce")
        df["reset_count"] = pd.to_numeric(df["reset_count"], errors="coerce").fillna(0).astype(int)
        df["remediation_count"] = pd.to_numeric(df["remediation_count"], errors="coerce").fillna(0).astype(int)
        df["enable_count"] = pd.to_numeric(df["enable_count"], errors="coerce").fillna(0).astype(int)
        return df
    except FileNotFoundError:
        return pd.DataFrame(columns=_COLS)
    except Exception as e:
        print(f"[WARNING] Could not load reset history: {e}. Starting fresh.")
        return pd.DataFrame(columns=_COLS)


def _save_tdmg_reset_history(history_df: pd.DataFrame) -> None:
    """Save TDMG reset/remediation history to CSV."""
    try:
        os.makedirs(REPORTS_DIR, exist_ok=True)
        out = history_df.copy()
        if "last_reset" in out.columns:
            out["last_reset"] = pd.to_datetime(out["last_reset"], utc=True, errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        if "last_remediation" in out.columns:
            out["last_remediation"] = pd.to_datetime(out["last_remediation"], utc=True, errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        if "last_enable" in out.columns:
            out["last_enable"] = pd.to_datetime(out["last_enable"], utc=True, errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        out.to_csv(TDMG_RESET_HISTORY_FILE, index=False)
    except Exception as e:
        print(f"[WARNING] Could not save reset history: {e}")


def _load_azure_history() -> pd.DataFrame:
    """Load Azure reboot/remediation history from CSV, returning an empty DataFrame if not found."""
    _COLS = ["dsn", "vin", "reboot_count", "last_reboot", "remediation_count", "last_remediation", "enable_count", "last_enable"]
    try:
        df = pd.read_csv(AZURE_HISTORY_FILE, dtype={"dsn": str})
        for col in _COLS:
            if col not in df.columns:
                df[col] = None
        df["last_reboot"] = pd.to_datetime(df["last_reboot"], utc=True, errors="coerce")
        df["last_remediation"] = pd.to_datetime(df["last_remediation"], utc=True, errors="coerce")
        df["last_enable"] = pd.to_datetime(df["last_enable"], utc=True, errors="coerce")
        df["reboot_count"] = pd.to_numeric(df["reboot_count"], errors="coerce").fillna(0).astype(int)
        df["remediation_count"] = pd.to_numeric(df["remediation_count"], errors="coerce").fillna(0).astype(int)
        df["enable_count"] = pd.to_numeric(df["enable_count"], errors="coerce").fillna(0).astype(int)
        return df
    except FileNotFoundError:
        return pd.DataFrame(columns=_COLS)
    except Exception as e:
        print(f"[WARNING] Could not load Azure history: {e}. Starting fresh.")
        return pd.DataFrame(columns=_COLS)


def _save_azure_history(history_df: pd.DataFrame) -> None:
    """Save Azure reboot/remediation history to CSV."""
    try:
        os.makedirs(REPORTS_DIR, exist_ok=True)
        out = history_df.copy()
        if "last_reboot" in out.columns:
            out["last_reboot"] = pd.to_datetime(out["last_reboot"], utc=True, errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        if "last_remediation" in out.columns:
            out["last_remediation"] = pd.to_datetime(out["last_remediation"], utc=True, errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        if "last_enable" in out.columns:
            out["last_enable"] = pd.to_datetime(out["last_enable"], utc=True, errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        out.to_csv(AZURE_HISTORY_FILE, index=False)
    except Exception as e:
        print(f"[WARNING] Could not save Azure history: {e}")


def reset_ota_shadow_for_devices(ota_false_df: pd.DataFrame, paccar_token: str, trimble_token: str) -> None:
    """
    Prompt once for the whole group, then in parallel (max 5 workers):
      1. Set otaApp.otaEnabled = False
      2. Wait 60 seconds (per worker)
      3. Set otaApp.otaEnabled = True
    """
    RESET_MAX_WORKERS = 5

    import datetime as _dt

    # Load reset history and apply cooldown filter
    history = _load_tdmg_reset_history()
    cooldown_hours = TDMG_RESET_COOLDOWN_HOURS
    cooldown_input = input(f"Exclude devices reset within the past how many hours? (default {cooldown_hours}): ").strip()
    if cooldown_input:
        try:
            cooldown_hours = int(cooldown_input)
        except ValueError:
            print(f"  Invalid input. Using default {cooldown_hours}h.")

    now_utc = _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0)
    cutoff = now_utc - _dt.timedelta(hours=cooldown_hours)

    all_dsns = ota_false_df["dsn"].dropna().astype(str).str.strip().tolist()
    all_vins = ota_false_df.set_index(ota_false_df["dsn"].astype(str).str.strip())["vin"].to_dict() if "vin" in ota_false_df.columns else {}

    if not history.empty:
        recent = history[history["last_reset"] >= cutoff]
        recent_dsns = set(recent["dsn"].astype(str).str.strip())
        skipped_recent = [d for d in all_dsns if d in recent_dsns]
        vin_updated = False
        if skipped_recent:
            print(f"\nExcluding {len(skipped_recent):,} device(s) reset within the past {cooldown_hours}h:")
            for d in skipped_recent:
                mask = history["dsn"] == d
                row = history[mask].iloc[0]
                if pd.isna(row["vin"]) or str(row["vin"]).strip() == "":
                    vin_val = all_vins.get(d, "")
                    if vin_val:
                        history.loc[mask, "vin"] = vin_val
                        vin_updated = True
                vin_display = history[mask].iloc[0]["vin"] if not pd.isna(history[mask].iloc[0]["vin"]) else all_vins.get(d, "unknown")
                print(f"  DSN {d} / VIN {vin_display} — {row['reset_count']:,} total reset(s), last: {row['last_reset'].strftime('%Y-%m-%d %H:%M UTC')}")
            if vin_updated:
                _save_tdmg_reset_history(history)
        filtered_df = ota_false_df[~ota_false_df["dsn"].astype(str).str.strip().isin(recent_dsns)].copy()
    else:
        skipped_recent = []
        filtered_df = ota_false_df.copy()

    if filtered_df.empty:
        print(f"\nAll devices were reset within the past {cooldown_hours}h. Nothing to do.")
        return

    dsns = filtered_df["dsn"].dropna().astype(str).str.strip().tolist()
    vins = filtered_df.set_index(filtered_df["dsn"].astype(str).str.strip())["vin"].to_dict()

    def _lookup_app_device_ids(dsns, token):
        ids = {}
        with ThreadPoolExecutor(max_workers=PACCAR_MAX_WORKERS) as executor:
            futures = {executor.submit(lookup_app_device_id, dsn, token): dsn for dsn in dsns}
            for future in tqdm(as_completed(futures), total=len(dsns), desc="Looking up appDeviceId", unit="device"):
                dsn = futures[future]
                try:
                    ids[dsn] = future.result()
                except PACCARAuthenticationError:
                    return None  # signal auth failure
                except Exception:
                    ids[dsn] = None
        return ids

    print(f"\nLooking up appDeviceId for {len(dsns):,} devices...")
    app_device_ids = _lookup_app_device_ids(dsns, paccar_token)
    if app_device_ids is None:
        print("[WARNING] PACCAR token expired during appDeviceId lookup. Attempting to refresh...")
        refreshed = refresh_paccar_token(paccar_token)
        if refreshed:
            print("Token refreshed successfully.")
            save_paccar_token(refreshed)
            paccar_token = refreshed
        else:
            print("Token refresh failed. Please provide a new token.")
            print("Get token from: Chrome > DevTools (F12) > Application > Local Storage > https://paccarsolutions.com")
            print("Find key: pnet.portal.encodedToken")
            paccar_token = input("Enter new PACCAR API bearer token (or press Enter to abort): ").strip()
            if not paccar_token:
                print("Aborting reset.")
                return
            save_paccar_token(paccar_token)
        print(f"Retrying appDeviceId lookup for {len(dsns):,} devices...")
        app_device_ids = _lookup_app_device_ids(dsns, paccar_token)
        if app_device_ids is None:
            print("[ERROR] Token still returning 401. Aborting reset.")
            return

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
    cancelled_pairs = []  # (dsn, app_device_id) for futures cancelled on 401 detection

    with ThreadPoolExecutor(max_workers=RESET_MAX_WORKERS) as executor:
        futures = {executor.submit(_reset_single_device, dsn, app_device_id, trimble_token): (dsn, app_device_id)
                   for dsn, app_device_id in actionable}
        auth_detected = False
        pbar = tqdm(total=len(actionable), desc="Resetting shadow", unit="device")
        for future in as_completed(futures):
            dsn, app_device_id = futures[future]
            try:
                result = future.result()
            except CancelledError:
                cancelled_pairs.append((dsn, app_device_id))
                pbar.update(1)
                continue
            results.append(result)
            pbar.update(1)
            if not auth_detected and "401" in (result.get("reason") or ""):
                auth_detected = True
                n_cancelled = sum(1 for f in futures if f.cancel())
                pbar.write(f"\n  [WARNING] Trimble 401 detected — cancelled {n_cancelled:,} pending task(s). Waiting for in-flight devices to finish...")
        pbar.close()

    succeeded = sum(1 for r in results if r["status"] == "Success")
    failed = len(results) - succeeded
    print(f"\nReset complete: {succeeded:,} succeeded, {failed:,} failed, {len(cancelled_pairs):,} cancelled")
    for r in results:
        if r["status"] != "Success":
            print(f"  [FAILED] DSN {r['dsn']}: {r['status']} - {r['reason']}")

    # Retry 401 failures + cancelled devices with a new token
    auth_failures = [r for r in results if r["status"] != "Success" and "401" in (r["reason"] or "")]
    if auth_failures or cancelled_pairs:
        if auth_failures:
            print(f"\n[WARNING] {len(auth_failures):,} device(s) failed with 401 Unauthorized.")
        if cancelled_pairs:
            print(f"  {len(cancelled_pairs):,} device(s) were cancelled after 401 was detected.")
        print("  Get token from: Browser DevTools (F12) > Network tab")
        print("  Find a request to: cloud.api.trimble.com")
        print("  Copy the Authorization header value (without 'Bearer ' prefix)")
        new_token = input("\n  Paste new Trimble token (or press Enter to skip retry): ").strip()
        if new_token.startswith("Bearer "):
            new_token = new_token[7:]
        if new_token:
            save_trimble_token(new_token)
            n_retry = len(auth_failures) + len(cancelled_pairs)
            print(f"\nRetrying {n_retry:,} devices...")
            retry_results = []
            with ThreadPoolExecutor(max_workers=RESET_MAX_WORKERS) as executor:
                retry_futures = {}
                for r in auth_failures:
                    dsn = r["dsn"]
                    app_device_id = r["app_device_id"]
                    if r["failed_step"] == "set_true":
                        # Already waited 60s — just retry set True
                        retry_futures[executor.submit(set_ota_desired_true, dsn, app_device_id, new_token)] = dsn
                    else:
                        # Full reset needed
                        retry_futures[executor.submit(_reset_single_device, dsn, app_device_id, new_token)] = dsn
                for dsn, app_device_id in cancelled_pairs:
                    retry_futures[executor.submit(_reset_single_device, dsn, app_device_id, new_token)] = dsn
                for future in tqdm(as_completed(retry_futures), total=len(retry_futures), desc="Retrying", unit="device"):
                    dsn = retry_futures[future]
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
            results = results + retry_results

    # Update reset history for all succeeded devices
    all_results = results
    succeeded_dsns = [r["dsn"] for r in all_results if r["status"] == "Success"]
    if succeeded_dsns:
        history = _load_tdmg_reset_history()
        now_utc = _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0)
        for dsn in succeeded_dsns:
            vin = vins.get(dsn, "")
            mask = history["dsn"] == dsn
            if mask.any():
                history.loc[mask, "reset_count"] += 1
                history.loc[mask, "last_reset"] = now_utc
                if vin and (pd.isna(history.loc[mask, "vin"]).all() or (history.loc[mask, "vin"] == "").all()):
                    history.loc[mask, "vin"] = vin
            else:
                history = pd.concat([history, pd.DataFrame([{
                    "dsn": dsn,
                    "vin": vin,
                    "reset_count": 1,
                    "last_reset": now_utc,
                }])], ignore_index=True)
        _save_tdmg_reset_history(history)
        print(f"\nReset history updated for {len(succeeded_dsns):,} device(s) → {TDMG_RESET_HISTORY_FILE}")


def enable_ota_for_devices(df: pd.DataFrame, paccar_token: str, trimble_token: str) -> None:
    """
    For each device in df, lookup appDeviceId then set otaApp.otaEnabled = True.
    Prints a progress summary and saves a results report.
    """
    import time as _time

    dsns = df["dsn"].dropna().astype(str).str.strip().tolist()

    def _lookup_ids(dsns, token):
        ids = {}
        with ThreadPoolExecutor(max_workers=PACCAR_MAX_WORKERS) as executor:
            futures = {executor.submit(lookup_app_device_id, dsn, token): dsn for dsn in dsns}
            for future in tqdm(as_completed(futures), total=len(dsns), desc="Looking up appDeviceId", unit="device"):
                dsn = futures[future]
                try:
                    ids[dsn] = future.result()
                except PACCARAuthenticationError:
                    return None
                except Exception:
                    ids[dsn] = None
        return ids

    print(f"\nLooking up appDeviceId for {len(dsns):,} devices...")
    app_device_ids = _lookup_ids(dsns, paccar_token)
    if app_device_ids is None:
        print("[WARNING] PACCAR token expired during appDeviceId lookup. Attempting to refresh...")
        refreshed = refresh_paccar_token(paccar_token)
        if refreshed:
            print("Token refreshed successfully.")
            save_paccar_token(refreshed)
            paccar_token = refreshed
        else:
            print("Token refresh failed. Please provide a new token.")
            print("Get token from: Chrome > DevTools (F12) > Application > Local Storage > https://paccarsolutions.com")
            print("Find key: pnet.portal.encodedToken")
            paccar_token = input("Enter new PACCAR API bearer token (or press Enter to abort): ").strip()
            if not paccar_token:
                print("Aborting enable step.")
                return
            save_paccar_token(paccar_token)
        print(f"Retrying appDeviceId lookup for {len(dsns):,} devices...")
        app_device_ids = _lookup_ids(dsns, paccar_token)
        if app_device_ids is None:
            print("[ERROR] Token still returning 401. Aborting enable step.")
            return

    found = sum(1 for v in app_device_ids.values() if v)
    print(f"appDeviceId found for {found:,} / {len(dsns):,} devices")

    if found == 0:
        print("[WARNING] No appDeviceIds found. Cannot proceed with enable.")
        return

    # Step 2: send shadow update for each device with a valid appDeviceId
    def _run_enable(token, ids):
        results = []
        for dsn in tqdm(dsns, desc="Enabling OTA", unit="device"):
            app_device_id = ids.get(dsn)
            if not app_device_id:
                results.append({"dsn": dsn, "status": "Skipped", "reason": "No appDeviceId found"})
                continue
            success, reason = set_ota_desired_true(dsn, app_device_id, token)
            results.append({"dsn": dsn, "status": "Success" if success else "Failed", "reason": reason or ""})
        return results

    print(f"\nSetting otaApp.otaEnabled = True...")
    results = _run_enable(trimble_token, app_device_ids)

    # Retry 401 failures with a new Trimble token
    auth_failures = [r for r in results if r["status"] == "Failed" and "401" in r["reason"]]
    if auth_failures:
        print(f"\n[WARNING] {len(auth_failures):,} device(s) failed with 401 Unauthorized (Trimble token expired).")
        print("\n  Trimble API token required.")
        print("  Get token from: Browser DevTools (F12) > Network tab")
        print("  Find a request to: cloud.api.trimble.com")
        print("  Copy the Authorization header value (without 'Bearer ' prefix)")
        new_trimble = input("\n  Paste new Trimble token (or press Enter to skip): ").strip()
        if new_trimble.startswith("Bearer "):
            new_trimble = new_trimble[7:]
        if new_trimble:
            save_trimble_token(new_trimble)
            failed_dsns = {r["dsn"] for r in auth_failures}
            results = [r for r in results if r["dsn"] not in failed_dsns]
            print(f"\nRetrying {len(auth_failures):,} failed devices...")
            results.extend(_run_enable(new_trimble, {dsn: app_device_ids.get(dsn) for dsn in failed_dsns}))
        else:
            print(f"[WARNING] {len(auth_failures):,} device(s) skipped — no new token provided.")

    successful = sum(1 for r in results if r["status"] == "Success")
    failed = sum(1 for r in results if r["status"] != "Success")
    print(f"\nOTA enable complete: {successful:,} succeeded, {failed:,} failed/skipped")

    # Save results report — merge in vin and pmgSwVersion from input df
    results_df = pd.DataFrame(results)
    meta_cols = ["dsn", "vin"] + (["pmgSwVersion"] if "pmgSwVersion" in df.columns else [])
    meta = df[meta_cols].copy()
    meta["dsn"] = meta["dsn"].astype(str).str.strip()
    results_df = results_df.merge(meta, on="dsn", how="left")
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
    import threading

    # --- DEBUG: probe the first DSN before running the full batch ---
    if dsns:
        _probe_dsn = dsns[0]
        _probe_url = f"{SHADOW_ENDPOINT}/{_probe_dsn}"
        print(f"\n[DEBUG] Shadow probe request:")
        print(f"  URL: {_probe_url}")
        print(f"  Header: X-Auth-Token = {token[:20]}...{token[-10:]}")
        try:
            _r = requests.get(_probe_url, headers={"X-Auth-Token": token, "Accept": "application/json"}, timeout=PACCAR_TIMEOUT, verify=False)
            print(f"  Status: {_r.status_code}")
            print(f"  Response headers: {dict(_r.headers)}")
            _body = _r.text[:500] if _r.text else "(empty)"
            print(f"  Response body: {_body}")
        except Exception as _e:
            print(f"  Probe error: {_e}")
        print()
    # --- END DEBUG ---

    results = {}
    error_counts = Counter()
    abort_event = threading.Event()

    def fetch_one(dsn):
        if abort_event.is_set():
            return dsn, None, None, "aborted"
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
                    if error_code == "404" and error_counts["404"] >= SHADOW_MAX_WORKERS:
                        abort_event.set()
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

    # If all completed (non-aborted) requests came back 401 or 404, the token is invalid
    completed = len(dsns) - error_counts.get("aborted", 0)
    if completed > 0 and error_counts.get("401", 0) == completed:
        raise PACCARAuthenticationError("All shadow requests returned 401 - PACCAR token is expired or invalid")
    if completed > 0 and error_counts.get("404", 0) == completed:
        raise PACCARAuthenticationError("All shadow requests returned 404 - PACCAR token may be invalid or endpoint URL changed")

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


def clear_bb_directory(app_device_id: str, trimble_token: str, max_retries: int = 3) -> tuple:
    """
    Send a reboot+clear MQTT message to wipe DATA/blkberry/provisioning via Trimble API.
    Returns (success: bool, reason: str or None).
    Accepts 201 or 403 as success (403 = message already queued).
    """
    import time, json
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
            "Format": "",
            "TopicSuffix": "reboot",
            "Message": json.dumps({"delaySeconds": 10, "clearPath": "DATA/blkberry/provisioning"})
        }
    }
    for attempt in range(max_retries):
        try:
            response = requests.post(TRIMBLE_MQTT_URL, json=payload, headers=headers, timeout=10)
            if response.status_code in (201, 403):
                return True, None
            elif response.status_code == 401:
                return False, "401 Unauthorized - Trimble token expired or invalid"
            elif response.status_code >= 500:
                if attempt < max_retries - 1:
                    time.sleep(2 ** (attempt + 1))
                    continue
                return False, f"{response.status_code} Server error"
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


def clear_bb_directory_azure(dsn: str, nexus_token: str, max_retries: int = 3) -> tuple:
    """
    Send a reboot+clear command to wipe DATA/blkberry/provisioning via PlatformScience API.
    Returns (success: bool, reason: str or None).
    """
    import time, json
    headers = {
        "Authorization": f"Bearer {nexus_token}",
        "x-application-customer": NEXUS_APP_CUSTOMER_ID,
        "Content-Type": "application/json"
    }
    payload = {
        "deviceId": str(dsn),
        "destinationTopic": "reboot",
        "payload": json.dumps({"delaySeconds": "10", "clearPath": "DATA/blkberry/provisioning"}),
        "payloadContentType": "application/json"
    }
    for attempt in range(max_retries):
        try:
            response = requests.post(AZURE_REBOOT_URL, json=payload, headers=headers, timeout=10)
            if response.status_code == 200:
                return True, None
            elif response.status_code == 401:
                return False, "401 Unauthorized - Nexus token expired or invalid"
            elif response.status_code >= 500:
                if attempt < max_retries - 1:
                    time.sleep(2 ** (attempt + 1))
                    continue
                return False, f"{response.status_code} Server error"
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


def check_software_status(vin: str, paccar_token: str) -> tuple:
    """
    GET software log for VIN and return (current_status, status_additional_info).
    Returns (None, None) on error.
    """
    url = f"{PACCAR_SOFTWARE_BASE_URL}/software/log"
    headers = {"X-Auth-Token": paccar_token, "Content-Type": "application/json"}
    try:
        response = requests.get(url, params={"page": 0, "pageSize": 1, "vin": vin},
                                headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            rows = data.get("data", [])
            if rows:
                row = rows[0]
                source = row.get("_source", {})
                return source.get("currentStatus"), source.get("statusAdditionalInfo")
            return None, None
        elif response.status_code == 401:
            raise PACCARAuthenticationError("Unauthorized - PACCAR token expired")
    except PACCARAuthenticationError:
        raise
    except Exception:
        pass
    return None, None


def check_subscription_active(vin: str, paccar_token: str) -> Optional[bool]:
    """
    POST to lastSubscriptionStatusHistory and return whether subscription is active.
    Returns None on error.
    """
    url = f"{PACCAR_SOFTWARE_BASE_URL}/subscription/lastSubscriptionStatusHistory"
    headers = {"X-Auth-Token": paccar_token, "Content-Type": "application/json"}
    try:
        response = requests.post(url, json=[vin], headers=headers, timeout=10)
        if response.status_code == 200:
            return response.json().get("active")
        elif response.status_code == 401:
            raise PACCARAuthenticationError("Unauthorized - PACCAR token expired")
    except PACCARAuthenticationError:
        raise
    except Exception:
        pass
    return None


def activate_pending_enable(vin: str, dsn: str, paccar_token: str) -> tuple:
    """
    POST to software/activate to trigger pending enable for a VIN/DSN.
    Returns (success: bool, reason: str or None).
    """
    import datetime
    url = f"{PACCAR_SOFTWARE_BASE_URL}/software/activate"
    headers = {"X-Auth-Token": paccar_token, "Content-Type": "application/json"}
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    payload = {
        "vehicles": [{
            "timestamp": timestamp,
            "user": PACCAR_USER_ID,
            "vin": vin,
            "dsn": str(dsn)
        }]
    }
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        if response.status_code == 200:
            return True, None
        elif response.status_code == 401:
            raise PACCARAuthenticationError("Unauthorized - PACCAR token expired")
        else:
            return False, f"{response.status_code} API error"
    except PACCARAuthenticationError:
        raise
    except requests.RequestException as e:
        return False, f"Network error: {e}"


def remediate_no_device_found(vin: str, dsn: str, paccar_token: str, trimble_token: str) -> str:
    """
    For a device not found in BB Portal:
      1. Look up appDeviceId
      2. Clear BB directory via Trimble MQTT
      3. Hit pending enable button via software/activate (if applicable)
    Returns a status string describing the outcome.
    """
    import time as _time

    # Step 1: look up appDeviceId
    print(f"  [1/3] Looking up appDeviceId for DSN {dsn}...")
    try:
        app_device_id = lookup_app_device_id(dsn, paccar_token)
    except PACCARAuthenticationError:
        return "Failed - PACCAR token expired (refresh token and retry)"
    if not app_device_id:
        return "Failed - no appDeviceId found"

    # Step 2: clear BB directory
    print(f"  [2/3] Clearing BB directory (DATA/blkberry/provisioning)...")
    success, reason = clear_bb_directory(app_device_id, trimble_token)
    if not success:
        return f"Failed - clear BB directory: {reason}"
    print(f"        OK")
    print(f"        Waiting 15 seconds before checking software status...")
    _time.sleep(15)

    # Step 3: check software status and activate if appropriate
    print(f"  [3/3] Checking software status for VIN {vin}...")
    try:
        current_status, additional_info = check_software_status(vin, paccar_token)
    except PACCARAuthenticationError:
        return "Failed - PACCAR token expired during status check (refresh token and retry)"
    print(f"        Status: {current_status}, AdditionalInfo: {additional_info}")

    if current_status is None:
        print(f"        Status check returned no data. Proceeding with activate anyway...")
    elif current_status != "UNPROVISIONED":
        return f"Skipped activate - status is '{current_status}' (not UNPROVISIONED)"

    if additional_info is None:
        if current_status is not None:
            active = check_subscription_active(vin, paccar_token)
            if not active:
                return "Skipped activate - subscription not active"
            print(f"        Subscription active. Activating...")
        else:
            print(f"        Activating...")
    elif additional_info == "PENDING_RESPONSE":
        print(f"        Already PENDING_RESPONSE. Re-activating...")
    else:
        return f"Skipped activate - unexpected statusAdditionalInfo: '{additional_info}'"

    try:
        success, reason = activate_pending_enable(vin, dsn, paccar_token)
    except PACCARAuthenticationError:
        return "Failed - PACCAR token expired during activate (refresh token and retry)"
    if success:
        return "Success"
    return f"Failed - activate: {reason}"


def remediate_no_device_found_azure(vin: str, dsn: str, paccar_token: str, nexus_token: str) -> str:
    """
    For an Azure TIG device not found in BB Portal:
      1. Clear BB directory via PlatformScience application/send API
      2. Hit pending enable button via software/activate (if applicable)
    Returns a status string describing the outcome.
    """
    import time as _time

    # Step 1: clear BB directory
    print(f"  [1/2] Clearing BB directory (DATA/blkberry/provisioning)...")
    success, reason = clear_bb_directory_azure(dsn, nexus_token)
    if not success:
        return f"Failed - clear BB directory: {reason}"
    print(f"        OK")
    print(f"        Waiting 15 seconds before checking software status...")
    _time.sleep(15)

    # Step 2: check software status and activate if appropriate
    print(f"  [2/2] Checking software status for VIN {vin}...")
    try:
        current_status, additional_info = check_software_status(vin, paccar_token)
    except PACCARAuthenticationError:
        return "Failed - PACCAR token expired during status check (refresh token and retry)"
    print(f"        Status: {current_status}, AdditionalInfo: {additional_info}")

    if current_status is None:
        print(f"        Status check returned no data. Proceeding with activate anyway...")
    elif current_status != "UNPROVISIONED":
        return f"Skipped activate - status is '{current_status}' (not UNPROVISIONED)"

    if additional_info is None:
        if current_status is not None:
            active = check_subscription_active(vin, paccar_token)
            if not active:
                return "Skipped activate - subscription not active"
            print(f"        Subscription active. Activating...")
        else:
            print(f"        Activating...")
    elif additional_info == "PENDING_RESPONSE":
        print(f"        Already PENDING_RESPONSE. Re-activating...")
    else:
        return f"Skipped activate - unexpected statusAdditionalInfo: '{additional_info}'"

    try:
        success, reason = activate_pending_enable(vin, dsn, paccar_token)
    except PACCARAuthenticationError:
        return "Failed - PACCAR token expired during activate (refresh token and retry)"
    if success:
        return "Success"
    return f"Failed - activate: {reason}"


def _analyze_tig_units(df: pd.DataFrame) -> None:
    """Branch 2: TIG TDMG units with DSN in range 20,000,000 - 30,000,000 and firmware 002.003.006 or 002.003.007."""
    print("="*70)
    print("Branch 2: TIG TDMG Units (DSN 20,000,000 - 30,000,000, firmware 002.003.006 / 002.003.007)")
    print("="*70)

    if "dsn" not in df.columns:
        print("[WARNING] 'dsn' column not found. Cannot filter TIG TDMG units.")
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

    print(f"TIG TDMG units found: {len(result):,}")

    if len(result) == 0:
        return

    # Shadow retrieval step
    print("\n" + "-"*70)
    fetch_shadow = input("Retrieve otaApp.otaEnabled shadow values for these TIG TDMG units? (y/n): ").strip().lower()
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
            print("Token refresh did not resolve the issue. Please provide a new token manually.")
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
                print("[ERROR] Token still failing after manual replacement. Skipping shadow retrieval.")
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

    # Filter to ota_reported == False or None
    print("\n" + "-"*70)
    filter_false = input("Filter to units where ota_reported = False or None? (y/n): ").strip().lower()
    if filter_false == "y":
        ota_false = result[(result["ota_reported"] == False) | result["ota_reported"].isna()]
        print(f"\nUnits with ota_reported = False or None: {len(ota_false):,}")
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

    # Enable OTA for units where both ota_reported and ota_desired are None/missing
    both_none = result[result["ota_reported"].isna() & result["ota_desired"].isna()]
    print("\n" + "-"*70)
    print(f"Units where ota_reported = None AND ota_desired = None: {len(both_none):,}")
    if len(both_none) > 0:
        do_enable_none = input(f"Set ota_desired = True for these {len(both_none):,} units? (y/n): ").strip().lower()
        if do_enable_none == "y":
            paccar_token = load_paccar_token()
            if not paccar_token:
                paccar_token = input("Enter PACCAR API bearer token (or press Enter to skip): ").strip()
                if not paccar_token:
                    print("[WARNING] No PACCAR token. Skipping enable step.")
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
                    enable_ota_for_devices(both_none, paccar_token, trimble_token)
                else:
                    print("[WARNING] No Trimble token provided. Skipping enable step.")

    # Filter to ota_reported == True with deprovision exclusion
    # Always compute ota_true so the BB Portal block can use it
    ota_true = result[result["ota_reported"] == True].copy()
    total_true = len(ota_true)

    DEPROVISION_FILE = "Units Impacted by PACCARs Deprovision Script.csv"
    excluded_count = 0
    deprov_excluded = pd.DataFrame()
    try:
        deprov_df = pd.read_csv(DEPROVISION_FILE)
        vin_col = next((c for c in deprov_df.columns if c.strip().lower() == "vin"), None)
        if vin_col:
            excluded_vins = set(deprov_df[vin_col].dropna().astype(str).str.strip().str.upper())
            excluded_mask = ota_true["vin"].astype(str).str.strip().str.upper().isin(excluded_vins)
            deprov_excluded = ota_true[excluded_mask].copy()
            ota_true = ota_true[~excluded_mask]
            excluded_count = len(deprov_excluded)
        else:
            print(f"[WARNING] No 'Vin' column found in {DEPROVISION_FILE}. Skipping exclusion.")
    except FileNotFoundError:
        print(f"[WARNING] {DEPROVISION_FILE} not found. Skipping exclusion.")
    except Exception as e:
        print(f"[WARNING] Could not load {DEPROVISION_FILE}: {e}. Skipping exclusion.")

    print("\n" + "-"*70)
    filter_true = input("Filter to units where ota_reported = True? (y/n): ").strip().lower()
    if filter_true == "y":
        print(f"\nUnits with ota_reported = True: {total_true:,}")
        print(f"  Excluded by deprovision file: {excluded_count:,}")
        print(f"  Remaining: {len(ota_true):,}")

        if len(ota_true) > 0:
            filepath = save_results_to_csv(ota_true, filename=get_report_filename("tig_ota_true"))
            print(f"\nReport saved: {filepath}")

        # Offer to reset the deprovision-excluded units individually
        if len(deprov_excluded) > 0:
            print("\n" + "-"*70)
            do_reset_excluded = input(f"Reset shadow for all {total_true:,} ota_reported=True units? (y/n): ").strip().lower()
            if do_reset_excluded == "y":
                paccar_token = load_paccar_token()
                if not paccar_token:
                    paccar_token = input("Enter PACCAR API bearer token (or press Enter to skip): ").strip()
                    if not paccar_token:
                        print("[WARNING] No PACCAR token. Skipping reset.")
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
                        all_reset_candidates = pd.concat(
                            [df for df in [deprov_excluded, ota_true] if len(df) > 0],
                            ignore_index=True
                        )
                        reset_ota_shadow_for_devices(all_reset_candidates, paccar_token, trimble_token)
                    else:
                        print("[WARNING] No Trimble token provided. Skipping reset.")

    # BB Portal lookup — ota_reported=True units remaining after deprovision exclusion
    print("\n" + "-"*70)
    print(f"Units for BB Portal lookup (ota_reported=True, not in deprovision file): {len(ota_true):,}")

    if len(ota_true) > 0:
        fetch_bb = input("Retrieve BB Portal data for these units? (y/n): ").strip().lower()
        if fetch_bb == "y":
            bb_token = load_bb_token()
            if bb_token:
                print("Using cached BB Portal token...")
            else:
                bb_token = _prompt_bb_token()

            if bb_token:
                dsns = ota_true["dsn"].dropna().astype(str).str.strip().unique().tolist()
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
                        failed_dsns = {r["dsn"] for r in auth_failures}
                        bb_results = [r for r in bb_results if r["dsn"] not in failed_dsns]
                        with ThreadPoolExecutor(max_workers=BB_MAX_WORKERS) as executor:
                            retry_futures = {executor.submit(fetch_bb_data_for_device, r["dsn"], new_token): r["dsn"]
                                             for r in auth_failures}
                            for future in tqdm(as_completed(retry_futures), total=len(auth_failures), desc="Retrying BB Portal", unit="device"):
                                bb_results.append(future.result())
                    else:
                        print(f"[WARNING] {len(auth_failures):,} device(s) skipped — no new token provided.")

                succeeded = sum(1 for r in bb_results if r["status"] == "Success")
                not_found = sum(1 for r in bb_results if r["status"] == "No device found")
                failed = sum(1 for r in bb_results if r["status"] == "Failed")
                with_invalidation = sum(1 for r in bb_results if r["status"] == "Success" and r.get("backup_invalidation"))
                print(f"\nBB Portal Summary:")
                print(f"  Succeeded:                   {succeeded:,}")
                print(f"  No device found:             {not_found:,}")
                print(f"  Failed:                      {failed:,}")
                print(f"  With backup_invalidation:    {with_invalidation:,}")
                print(f"  Without backup_invalidation: {succeeded - with_invalidation:,}")

                # Merge results back onto ota_true and save
                bb_df = pd.DataFrame(bb_results)[["dsn", "bb_device_id", "backup_invalidation", "last_scan", "status", "reason"]]
                bb_df["dsn"] = bb_df["dsn"].astype(str)
                ota_true_out = ota_true.copy()
                ota_true_out["dsn"] = ota_true_out["dsn"].astype(str)
                ota_true_out = ota_true_out.merge(bb_df, on="dsn", how="left")
                filepath = save_results_to_csv(ota_true_out, filename=get_report_filename("tig_bb_portal"))
                print(f"\nReport saved: {filepath}")

                # Remediation loop for "No device found" devices
                no_device = [r for r in bb_results if r["status"] == "No device found"]
                if no_device:
                    # Build DSN→VIN map from ota_true
                    dsn_to_vin = ota_true.set_index(
                        ota_true["dsn"].astype(str).str.strip()
                    )["vin"].to_dict()

                    import datetime as _dt
                    print(f"\n" + "-"*70)
                    print(f"Remediation: {len(no_device):,} devices not found in BB Portal")

                    # Apply remediation cooldown
                    rem_history = _load_tdmg_reset_history()
                    rem_cooldown = TDMG_REMEDIATION_COOLDOWN_HOURS
                    rem_cooldown_input = input(f"Exclude devices remediated within the past how many hours? (default {rem_cooldown}): ").strip()
                    if rem_cooldown_input:
                        try:
                            rem_cooldown = int(rem_cooldown_input)
                        except ValueError:
                            print(f"  Invalid input. Using default {rem_cooldown}h.")
                    rem_now_utc = _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0).replace(microsecond=0)
                    rem_cutoff = rem_now_utc - _dt.timedelta(hours=rem_cooldown)

                    if not rem_history.empty and rem_history["last_remediation"].notna().any():
                        rem_recent = rem_history[rem_history["last_remediation"] >= rem_cutoff]
                        rem_recent_dsns = set(rem_recent["dsn"].astype(str).str.strip())
                        rem_skipped = [r for r in no_device if str(r["dsn"]) in rem_recent_dsns]
                        if rem_skipped:
                            print(f"\nExcluding {len(rem_skipped):,} device(s) remediated within the past {rem_cooldown}h:")
                            rem_vin_updated = False
                            for r in rem_skipped:
                                d = str(r["dsn"])
                                mask = rem_history["dsn"] == d
                                row = rem_history[mask].iloc[0]
                                if pd.isna(row["vin"]) or str(row["vin"]).strip() == "":
                                    vin_val = dsn_to_vin.get(d, "")
                                    if vin_val:
                                        rem_history.loc[mask, "vin"] = vin_val
                                        rem_vin_updated = True
                                vin_display = rem_history[mask].iloc[0]["vin"] if not pd.isna(rem_history[mask].iloc[0]["vin"]) else dsn_to_vin.get(d, "unknown")
                                print(f"  DSN {d} / VIN {vin_display} — {int(row['remediation_count']):,} total remediation(s), last: {row['last_remediation'].strftime('%Y-%m-%d %H:%M UTC')}")
                            if rem_vin_updated:
                                _save_tdmg_reset_history(rem_history)
                        no_device = [r for r in no_device if str(r["dsn"]) not in rem_recent_dsns]

                    if not no_device:
                        print(f"All devices were remediated within the past {rem_cooldown}h. Nothing to do.")
                        do_remediate = "n"
                    else:
                        print(f"Devices eligible for remediation: {len(no_device):,}")
                        do_remediate = input("Remediate these devices (look up ID, clear BB dir, enable)? (y/n): ").strip().lower()
                    if do_remediate == "y":
                        # Ensure tokens
                        paccar_token = load_paccar_token()
                        if not paccar_token:
                            paccar_token = input("Enter PACCAR API bearer token (or press Enter to skip): ").strip()
                            if not paccar_token:
                                print("[WARNING] No PACCAR token. Skipping remediation.")
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
                                remediation_results = []
                                for r in no_device:
                                    dsn = r["dsn"]
                                    vin = dsn_to_vin.get(str(dsn), "unknown")
                                    print(f"\nDSN {dsn} / VIN {vin}")
                                    answer = input("  Remediate this device? (y/n/q to quit): ").strip().lower()
                                    if answer == "q":
                                        print("Stopping remediation loop.")
                                        break
                                    if answer != "y":
                                        remediation_results.append({"dsn": dsn, "vin": vin, "result": "Skipped"})
                                        continue
                                    result_status = remediate_no_device_found(vin, dsn, paccar_token, trimble_token)
                                    if "PACCAR token expired" in result_status:
                                        print(f"  Result: {result_status}")
                                        print("\n  [WARNING] PACCAR token expired.")
                                        new_paccar = input("  Enter new PACCAR API bearer token (or press Enter to skip): ").strip()
                                        if new_paccar:
                                            save_paccar_token(new_paccar)
                                            paccar_token = new_paccar
                                            result_status = remediate_no_device_found(vin, dsn, paccar_token, trimble_token)
                                        else:
                                            print("  No token provided. Stopping remediation.")
                                            remediation_results.append({"dsn": dsn, "vin": vin, "result": result_status})
                                            break
                                    if "401 Unauthorized - Trimble token expired" in result_status:
                                        print(f"  Result: {result_status}")
                                        print("\n  [WARNING] Trimble token expired.")
                                        print("  Get token from: Browser DevTools (F12) > Network tab")
                                        print("  Find a request to: cloud.api.trimble.com")
                                        print("  Copy the Authorization header value (without 'Bearer ' prefix)")
                                        new_trimble = input("\n  Paste Trimble token (or press Enter to skip): ").strip()
                                        if new_trimble.startswith("Bearer "):
                                            new_trimble = new_trimble[7:]
                                        if new_trimble:
                                            save_trimble_token(new_trimble)
                                            trimble_token = new_trimble
                                            result_status = remediate_no_device_found(vin, dsn, paccar_token, trimble_token)
                                        else:
                                            print("  No token provided. Stopping remediation.")
                                            remediation_results.append({"dsn": dsn, "vin": vin, "result": result_status})
                                            break
                                    print(f"  Result: {result_status}")
                                    remediation_results.append({"dsn": dsn, "vin": vin, "result": result_status})

                                if remediation_results:
                                    rem_df = pd.DataFrame(remediation_results)
                                    filepath = save_results_to_csv(rem_df, filename=get_report_filename("tig_bb_remediation"))
                                    print(f"\nRemediation report saved: {filepath}")

                                    # Update history for successful remediations
                                    succeeded_rem_dsns = [r["dsn"] for r in remediation_results if r["result"] == "Success"]
                                    if succeeded_rem_dsns:
                                        rem_history = _load_tdmg_reset_history()
                                        rem_now_utc = _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0).replace(microsecond=0)
                                        for dsn in succeeded_rem_dsns:
                                            vin = dsn_to_vin.get(str(dsn), "")
                                            mask = rem_history["dsn"] == dsn
                                            if mask.any():
                                                rem_history.loc[mask, "remediation_count"] = rem_history.loc[mask, "remediation_count"].fillna(0).astype(int) + 1
                                                rem_history.loc[mask, "last_remediation"] = rem_now_utc
                                                if vin and (pd.isna(rem_history.loc[mask, "vin"]).all() or (rem_history.loc[mask, "vin"] == "").all()):
                                                    rem_history.loc[mask, "vin"] = vin
                                            else:
                                                rem_history = pd.concat([rem_history, pd.DataFrame([{
                                                    "dsn": dsn,
                                                    "vin": vin,
                                                    "reset_count": 0,
                                                    "last_reset": None,
                                                    "remediation_count": 1,
                                                    "last_remediation": rem_now_utc,
                                                }])], ignore_index=True)
                                        _save_tdmg_reset_history(rem_history)
                                        print(f"Remediation history updated for {len(succeeded_rem_dsns):,} device(s) → {TDMG_RESET_HISTORY_FILE}")
                            else:
                                print("[WARNING] No Trimble token provided. Skipping remediation.")


def fetch_azure_shadow_state(dsn: str, nexus_token: str) -> dict:
    """
    Fetch the most recent reported and desired otaApp.otaEnabled values for a
    TIG Azure device from the PlatformScience archive/configurations endpoint.

    Returns:
        {"reported": bool|None, "desired": bool|None, "error": str|None}
    """
    import json as _json
    import datetime as _dt

    PAGE_SIZE = 50
    MAX_EVENTS = 1000  # cap at 1000 events to avoid runaway pagination

    now = _dt.datetime.now(_dt.timezone.utc)
    start = now - _dt.timedelta(days=AZURE_SHADOW_LOOKBACK_DAYS)
    base_params = {
        "startTime": start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "endTime": now.strftime("%Y-%m-%dT%H:%M:%S.999Z"),
        "limit": PAGE_SIZE,
    }
    headers = {
        "Authorization": f"Bearer {nexus_token}",
        "x-application-customer": NEXUS_APP_CUSTOMER_ID,
        "Accept": "application/json",
    }
    url = AZURE_SHADOW_ARCHIVE_URL.format(dsn=dsn)

    reported_val = None
    desired_val = None
    reported_ts = None
    desired_ts = None
    offset = 0

    try:
        while offset < MAX_EVENTS:
            params = {**base_params, "offset": offset}
            resp = requests.get(url, params=params, headers=headers, timeout=30)
            if resp.status_code in (401, 403):
                return {"reported": None, "desired": None, "error": f"{resp.status_code} Unauthorized - Nexus token expired or invalid"}
            if resp.status_code != 200:
                return {"reported": None, "desired": None, "error": f"HTTP {resp.status_code}"}

            body = resp.json()
            data = body.get("data", [])
            total_hits = body.get("totalHits", 0)

            for item in data:
                event_type = item.get("eventType")
                ts = item.get("timestamp")
                raw = item.get("data", "{}")
                try:
                    parsed = _json.loads(raw)
                except Exception:
                    continue

                ota_app = parsed.get("otaApp")
                if not isinstance(ota_app, dict):
                    continue
                ota_enabled = ota_app.get("otaEnabled")

                if event_type == "reported" and (reported_ts is None or ts > reported_ts):
                    reported_val = ota_enabled
                    reported_ts = ts
                elif event_type == "desired" and (desired_ts is None or ts > desired_ts):
                    desired_val = ota_enabled
                    desired_ts = ts

            # Stop if both found, no more pages, or first page already has reported
            # (reported is always recent; only keep paging if desired still missing)
            offset += PAGE_SIZE
            if desired_val is not None:
                break
            if offset >= total_hits:
                break
            # If we already have reported from the first page, only continue paging
            # to find desired — skip if reported is also still missing (device offline)
            if reported_ts is not None and desired_ts is not None:
                break

        return {"reported": reported_val, "desired": desired_val, "error": None}

    except requests.exceptions.RequestException as e:
        return {"reported": None, "desired": None, "error": str(e)}


def retrieve_azure_shadow_data(dsns: list, nexus_token: str) -> dict:
    """
    Fetch Azure shadow state (otaApp.otaEnabled reported/desired) for a list of DSNs
    in parallel. Returns {dsn: {"reported": bool|None, "desired": bool|None}}.
    Raises a ValueError with the 401 message if any response indicates an expired token.
    """
    results = {}
    auth_error = None

    with ThreadPoolExecutor(max_workers=SHADOW_MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_azure_shadow_state, dsn, nexus_token): dsn for dsn in dsns}
        for future in tqdm(as_completed(futures), total=len(dsns), desc="Fetching Azure shadow state", unit="device"):
            dsn = futures[future]
            result = future.result()
            if result.get("error") and "Unauthorized" in (result["error"] or ""):
                auth_error = result["error"]
            results[dsn] = {"reported": result["reported"], "desired": result["desired"]}

    # Surface auth errors after all futures complete so the caller can re-prompt
    if auth_error and all(v["reported"] is None and v["desired"] is None for v in results.values()):
        raise ValueError(auth_error)

    return results


def request_nexus_logs(dsn: str, nexus_token: str, max_retries: int = 3) -> tuple:
    """
    Request gateway logs for a Nexus device via PlatformScience log request API.

    Returns:
        (success: bool, reason: str)
    """
    url = NEXUS_LOG_REQUEST_URL.format(dsn=dsn)
    headers = {
        "Authorization": f"Bearer {nexus_token}",
        "x-application-customer": NEXUS_APP_CUSTOMER_ID,
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
    }
    payload = {"logType": "{\"system\":true,\"product\":true,\"platform\":true}"}

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=30)
            if resp.status_code == 200:
                return True, ""
            if resp.status_code in (401, 403):
                return False, f"{resp.status_code} Unauthorized - Nexus token expired or invalid"
            return False, f"HTTP {resp.status_code}: {resp.text[:200]}"
        except requests.exceptions.RequestException as e:
            if attempt == max_retries:
                return False, f"Request error: {e}"
    return False, "Max retries exceeded"


def send_azure_reboot(dsn: str, nexus_token: str, max_retries: int = 3) -> tuple:
    """
    Send a reboot command to a TIG Azure device via the PlatformScience application/send API.

    Returns:
        (success: bool, reason: str)
    """
    import uuid
    payload = {
        "deviceId": str(dsn),
        "destinationTopic": "reboot",
        "payload": "{\"delaySeconds\":\"1\"}",
        "payloadContentType": "application/json",
    }
    headers = {
        "Authorization": f"Bearer {nexus_token}",
        "x-application-customer": NEXUS_APP_CUSTOMER_ID,
        "Content-Type": "application/json",
    }
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(AZURE_REBOOT_URL, json=payload, headers=headers, timeout=30)
            if resp.status_code == 200:
                return True, ""
            if resp.status_code in (401, 403):
                return False, f"{resp.status_code} Unauthorized - Nexus token expired or invalid"
            return False, f"HTTP {resp.status_code}: {resp.text[:200]}"
        except requests.exceptions.RequestException as e:
            if attempt == max_retries:
                return False, f"Request error: {e}"
    return False, "Max retries exceeded"


def _analyze_tig_azure_units(df: pd.DataFrame) -> None:
    """Branch 3: TIG Azure units with DSN in range 20,000,000 - 30,000,000 and firmware 007.002.026 or 007.003.026."""
    print("="*70)
    print("Branch 3: TIG Azure Units (DSN 20,000,000 - 30,000,000, firmware 007.002.026 / 007.003.026)")
    print("="*70)

    if "dsn" not in df.columns:
        print("[WARNING] 'dsn' column not found. Cannot filter TIG Azure units.")
        return

    work = df.copy()
    work["_dsn_num"] = pd.to_numeric(work["dsn"], errors="coerce")
    dsn_mask = (work["_dsn_num"] >= 20_000_000) & (work["_dsn_num"] <= 30_000_000)

    if "pmgSwVersion" in work.columns:
        fw_mask = work["pmgSwVersion"].astype(str).str.upper().isin(["007.002.026", "007.003.026"])
        result = work[dsn_mask & fw_mask].drop(columns=["_dsn_num"])
    else:
        print("[WARNING] 'pmgSwVersion' column not found. Filtering by DSN range only.")
        result = work[dsn_mask].drop(columns=["_dsn_num"])

    print(f"TIG Azure units found: {len(result):,}")

    if len(result) == 0:
        return

    # Split by firmware version
    if "pmgSwVersion" in result.columns:
        fw = result["pmgSwVersion"].astype(str).str.upper()
        units_726 = result[fw == "007.002.026"]
        units_736 = result[fw == "007.003.026"]
    else:
        units_726 = result.iloc[0:0]
        units_736 = result

    if len(units_726) > 0:
        print(f"\n7.2.26 units: {len(units_726):,}")
        vins_726 = units_726["vin"].dropna().astype(str).tolist() if "vin" in units_726.columns else []
        for vin in vins_726:
            print(f"  {vin}")

    # Shadow retrieval step — 7.3.26 units only
    print("\n" + "-"*70)
    fetch_shadow = input(f"Retrieve otaApp.otaEnabled shadow values for {len(units_736):,} TIG Azure 7.3.26 units? (y/n): ").strip().lower()
    if fetch_shadow != "y":
        return

    result = units_736

    nexus_token = load_nexus_token()
    if nexus_token:
        print("Using cached Nexus token...")
    else:
        nexus_token = _prompt_nexus_token()
        if not nexus_token:
            print("[WARNING] No Nexus token available. Skipping shadow retrieval.")
            return

    dsns = result["dsn"].dropna().astype(str).str.strip().unique().tolist()
    try:
        shadow_results = retrieve_azure_shadow_data(dsns, nexus_token)
    except ValueError as e:
        if "Unauthorized" in str(e):
            print(f"\n[WARNING] Nexus token expired or invalid during shadow retrieval.")
            nexus_token = _prompt_nexus_token()
            if not nexus_token:
                print("Skipping shadow retrieval.")
                return
            try:
                shadow_results = retrieve_azure_shadow_data(dsns, nexus_token)
            except ValueError:
                print("[ERROR] Token still failing after replacement. Skipping shadow retrieval.")
                return
        else:
            raise

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

    # Filter to ota_reported == False or None
    print("\n" + "-"*70)
    filter_false = input("Filter to units where ota_reported = False or None? (y/n): ").strip().lower()
    if filter_false == "y":
        ota_false = result[(result["ota_reported"] == False) | result["ota_reported"].isna()]
        print(f"\nUnits with ota_reported = False or None: {len(ota_false):,}")
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
                    nexus_token = load_nexus_token()
                    if nexus_token:
                        print("Using cached Nexus token...")
                    else:
                        nexus_token = _prompt_nexus_token()

                    if nexus_token:
                        enable_nexus_ota_for_devices(both_false, nexus_token)
                    else:
                        print("[WARNING] No Nexus token provided. Skipping enable step.")

            # Reset shadow only for reported=False / desired=True units
            reset_candidates = ota_false[ota_false["ota_desired"] != False]
            print("\n" + "-"*70)
            do_reset = input(f"Reset shadow (False → wait 60s → True) for the {len(reset_candidates):,} ota_reported=False / ota_desired=True units? (y/n): ").strip().lower()
            if do_reset == "y":
                nexus_token = load_nexus_token()
                if nexus_token:
                    print("Using cached Nexus token...")
                else:
                    nexus_token = _prompt_nexus_token()

                if nexus_token:
                    reset_nexus_ota_shadow_for_devices(reset_candidates, nexus_token, device_type="pmg")
                else:
                    print("[WARNING] No Nexus token provided. Skipping reset step.")

            filepath = save_results_to_csv(ota_false, filename=get_report_filename("tig_azure_ota_disabled"))
            print(f"\nReport saved: {filepath}")

    # Enable OTA for units where both ota_reported and ota_desired are None/missing
    both_none = result[result["ota_reported"].isna() & result["ota_desired"].isna()]
    print("\n" + "-"*70)
    print(f"Units where ota_reported = None AND ota_desired = None: {len(both_none):,}")
    if len(both_none) > 0:
        do_enable_none = input(f"Set ota_desired = True for these {len(both_none):,} units? (y/n): ").strip().lower()
        if do_enable_none == "y":
            nexus_token = load_nexus_token()
            if nexus_token:
                print("Using cached Nexus token...")
            else:
                nexus_token = _prompt_nexus_token()

            if nexus_token:
                import datetime as _dt
                import time as _time

                # Load history and apply reboot cooldown filter
                azure_history = _load_azure_history()
                reboot_cooldown_hours = AZURE_REBOOT_COOLDOWN_HOURS
                cooldown_input = input(f"Exclude devices rebooted within the past how many hours? (default {reboot_cooldown_hours}): ").strip()
                if cooldown_input:
                    try:
                        reboot_cooldown_hours = int(cooldown_input)
                    except ValueError:
                        print(f"  Invalid input. Using default {reboot_cooldown_hours}h.")

                now_utc = _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0)
                reboot_cutoff = now_utc - _dt.timedelta(hours=reboot_cooldown_hours)

                if not azure_history.empty:
                    recent_rebooted = azure_history[azure_history["last_reboot"] >= reboot_cutoff]
                    recent_reboot_dsns = set(recent_rebooted["dsn"].astype(str).str.strip())
                    skipped_reboot = both_none[both_none["dsn"].astype(str).str.strip().isin(recent_reboot_dsns)]
                    if len(skipped_reboot) > 0:
                        print(f"\nExcluding {len(skipped_reboot):,} device(s) rebooted within the past {reboot_cooldown_hours}h:")
                        az_vin_updated = False
                        for _, sr in skipped_reboot.iterrows():
                            d = str(sr["dsn"]).strip()
                            mask = azure_history["dsn"] == d
                            hr = azure_history[mask].iloc[0]
                            if pd.isna(hr["vin"]) or str(hr["vin"]).strip() == "":
                                vin_val = str(sr["vin"]).strip() if "vin" in sr and pd.notna(sr["vin"]) else ""
                                if vin_val:
                                    azure_history.loc[mask, "vin"] = vin_val
                                    az_vin_updated = True
                            vin_display = azure_history[mask].iloc[0]["vin"] if not pd.isna(azure_history[mask].iloc[0]["vin"]) else str(sr.get("vin", "unknown"))
                            print(f"  DSN {d} / VIN {vin_display} — {hr['reboot_count']:,} total reboot(s), last: {hr['last_reboot'].strftime('%Y-%m-%d %H:%M UTC')}")
                        if az_vin_updated:
                            _save_azure_history(azure_history)
                    both_none_filtered = both_none[~both_none["dsn"].astype(str).str.strip().isin(recent_reboot_dsns)]
                else:
                    both_none_filtered = both_none

                if both_none_filtered.empty:
                    print(f"\nAll devices were rebooted within the past {reboot_cooldown_hours}h. Nothing to do.")
                else:
                    enable_results = []
                    stop_loop = False
                    for _, row in both_none_filtered.iterrows():
                        if stop_loop:
                            break
                        dsn = str(row["dsn"]).strip()
                        vin = str(row.get("vin", "unknown"))
                        fw = str(row.get("pmgSwVersion", "unknown"))
                        answer = input(f"\n  DSN {dsn} / VIN {vin} / FW {fw} — enable OTA? (y/n/q to quit): ").strip().lower()
                        if answer == "q":
                            print("  Stopping enable loop.")
                            break
                        if answer != "y":
                            enable_results.append({"dsn": dsn, "vin": vin, "status": "Skipped", "reboot": ""})
                            continue
                        success, reason, _ = set_nexus_ota_desired(dsn, nexus_token, enabled=True, device_type="pmg")
                        if not success and "401" in (reason or ""):
                            print(f"  Result: Failed: {reason}")
                            print("\n  [WARNING] Nexus token expired or invalid.")
                            new_token = _prompt_nexus_token()
                            if not new_token:
                                print("  No token provided. Stopping enable loop.")
                                enable_results.append({"dsn": dsn, "vin": vin, "status": f"Failed: {reason}", "reboot": ""})
                                stop_loop = True
                                continue
                            nexus_token = new_token
                            success, reason, _ = set_nexus_ota_desired(dsn, nexus_token, enabled=True, device_type="pmg")

                        if success:
                            print(f"  Result: Success — waiting {AZURE_REBOOT_DELAY_SECONDS}s before reboot...")
                            _time.sleep(AZURE_REBOOT_DELAY_SECONDS)
                            reboot_ok, reboot_reason = send_azure_reboot(dsn, nexus_token)
                            if reboot_ok:
                                print(f"  Reboot: Sent")
                                status = "Success"
                                reboot_status = "Sent"
                            else:
                                print(f"  Reboot: Failed — {reboot_reason}")
                                status = "Success"
                                reboot_status = f"Failed: {reboot_reason}"
                        else:
                            status = f"Failed: {reason}"
                            reboot_status = "Skipped"
                            print(f"  Result: {status}")
                        enable_results.append({"dsn": dsn, "vin": vin, "status": status, "reboot": reboot_status})

                    if enable_results:
                        attempted = [r for r in enable_results if r["status"] != "Skipped"]
                        succeeded = sum(1 for r in attempted if r["status"] == "Success")
                        print(f"\nEnable complete: {succeeded:,}/{len(attempted):,} succeeded, {len(both_none_filtered) - len(enable_results):,} not reached.")

                    # Update reboot history for successfully rebooted devices
                    rebooted_dsns = [r["dsn"] for r in enable_results if r.get("reboot") == "Sent"]
                    if rebooted_dsns:
                        azure_history = _load_azure_history()
                        now_utc = _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0)
                        enable_vin_map = {r["dsn"]: r.get("vin", "") for r in enable_results}
                        for dsn in rebooted_dsns:
                            vin = enable_vin_map.get(dsn, "")
                            mask = azure_history["dsn"] == dsn
                            if mask.any():
                                azure_history.loc[mask, "reboot_count"] += 1
                                azure_history.loc[mask, "last_reboot"] = now_utc
                                if vin and (pd.isna(azure_history.loc[mask, "vin"]).all() or (azure_history.loc[mask, "vin"] == "").all()):
                                    azure_history.loc[mask, "vin"] = vin
                            else:
                                azure_history = pd.concat([azure_history, pd.DataFrame([{
                                    "dsn": dsn, "vin": vin, "reboot_count": 1, "last_reboot": now_utc,
                                    "remediation_count": 0, "last_remediation": None,
                                }])], ignore_index=True)
                        _save_azure_history(azure_history)
                        print(f"\nReboot history updated for {len(rebooted_dsns):,} device(s) → {AZURE_HISTORY_FILE}")
            else:
                print("[WARNING] No Nexus token provided. Skipping enable step.")

    # Filter to ota_reported == True with deprovision exclusion
    ota_true = result[result["ota_reported"] == True].copy()
    total_true = len(ota_true)

    DEPROVISION_FILE = "Units Impacted by PACCARs Deprovision Script.csv"
    excluded_count = 0
    try:
        deprov_df = pd.read_csv(DEPROVISION_FILE)
        vin_col = next((c for c in deprov_df.columns if c.strip().lower() == "vin"), None)
        if vin_col:
            excluded_vins = set(deprov_df[vin_col].dropna().astype(str).str.strip().str.upper())
            ota_true = ota_true[~ota_true["vin"].astype(str).str.strip().str.upper().isin(excluded_vins)]
            excluded_count = total_true - len(ota_true)
        else:
            print(f"[WARNING] No 'Vin' column found in {DEPROVISION_FILE}. Skipping exclusion.")
    except FileNotFoundError:
        print(f"[WARNING] {DEPROVISION_FILE} not found. Skipping exclusion.")
    except Exception as e:
        print(f"[WARNING] Could not load {DEPROVISION_FILE}: {e}. Skipping exclusion.")

    print("\n" + "-"*70)
    filter_true = input("Filter to units where ota_reported = True? (y/n): ").strip().lower()
    if filter_true == "y":
        print(f"\nUnits with ota_reported = True: {total_true:,}")
        print(f"  Excluded by deprovision file: {excluded_count:,}")
        print(f"  Remaining: {len(ota_true):,}")

        if len(ota_true) > 0:
            filepath = save_results_to_csv(ota_true, filename=get_report_filename("tig_azure_ota_true"))
            print(f"\nReport saved: {filepath}")

    # BB Portal lookup — ota_reported=True units remaining after deprovision exclusion
    print("\n" + "-"*70)
    print(f"Units for BB Portal lookup (ota_reported=True, not in deprovision file): {len(ota_true):,}")

    if len(ota_true) > 0:
        fetch_bb = input("Retrieve BB Portal data for these units? (y/n): ").strip().lower()
        if fetch_bb == "y":
            bb_token = load_bb_token()
            if bb_token:
                print("Using cached BB Portal token...")
            else:
                bb_token = _prompt_bb_token()

            if bb_token:
                dsns = ota_true["dsn"].dropna().astype(str).str.strip().unique().tolist()
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
                        failed_dsns = {r["dsn"] for r in auth_failures}
                        bb_results = [r for r in bb_results if r["dsn"] not in failed_dsns]
                        with ThreadPoolExecutor(max_workers=BB_MAX_WORKERS) as executor:
                            retry_futures = {executor.submit(fetch_bb_data_for_device, r["dsn"], new_token): r["dsn"]
                                             for r in auth_failures}
                            for future in tqdm(as_completed(retry_futures), total=len(auth_failures), desc="Retrying BB Portal", unit="device"):
                                bb_results.append(future.result())
                    else:
                        print(f"[WARNING] {len(auth_failures):,} device(s) skipped — no new token provided.")

                succeeded = sum(1 for r in bb_results if r["status"] == "Success")
                not_found = sum(1 for r in bb_results if r["status"] == "No device found")
                failed = sum(1 for r in bb_results if r["status"] == "Failed")
                with_invalidation = sum(1 for r in bb_results if r["status"] == "Success" and r.get("backup_invalidation"))
                print(f"\nBB Portal Summary:")
                print(f"  Succeeded:                   {succeeded:,}")
                print(f"  No device found:             {not_found:,}")
                print(f"  Failed:                      {failed:,}")
                print(f"  With backup_invalidation:    {with_invalidation:,}")
                print(f"  Without backup_invalidation: {succeeded - with_invalidation:,}")

                # Merge results back onto ota_true and save
                bb_df = pd.DataFrame(bb_results)[["dsn", "bb_device_id", "backup_invalidation", "last_scan", "status", "reason"]]
                bb_df["dsn"] = bb_df["dsn"].astype(str)
                ota_true_out = ota_true.copy()
                ota_true_out["dsn"] = ota_true_out["dsn"].astype(str)
                ota_true_out = ota_true_out.merge(bb_df, on="dsn", how="left")
                filepath = save_results_to_csv(ota_true_out, filename=get_report_filename("tig_azure_bb_portal"))
                print(f"\nReport saved: {filepath}")

                # Remediation loop for "No device found" devices
                no_device = [r for r in bb_results if r["status"] == "No device found"]
                if no_device:
                    dsn_to_vin = ota_true.set_index(
                        ota_true["dsn"].astype(str).str.strip()
                    )["vin"].to_dict()

                    print(f"\n" + "-"*70)
                    print(f"Remediation: {len(no_device):,} devices not found in BB Portal")
                    do_remediate = input("Remediate these devices (look up ID, clear BB dir, enable)? (y/n): ").strip().lower()
                    if do_remediate == "y":
                        paccar_token = load_paccar_token()
                        if not paccar_token:
                            paccar_token = input("Enter PACCAR API bearer token (or press Enter to skip): ").strip()
                            if not paccar_token:
                                print("[WARNING] No PACCAR token. Skipping remediation.")
                            else:
                                save_paccar_token(paccar_token)

                        if paccar_token:
                            if not nexus_token:
                                nexus_token = _prompt_nexus_token()

                            if nexus_token:
                                import datetime as _dt

                                # Load history and apply remediation cooldown filter
                                azure_history = _load_azure_history()
                                rem_cooldown_hours = AZURE_REMEDIATION_COOLDOWN_HOURS
                                cooldown_input = input(f"Exclude devices remediated within the past how many hours? (default {rem_cooldown_hours}): ").strip()
                                if cooldown_input:
                                    try:
                                        rem_cooldown_hours = int(cooldown_input)
                                    except ValueError:
                                        print(f"  Invalid input. Using default {rem_cooldown_hours}h.")

                                now_utc = _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0)
                                rem_cutoff = now_utc - _dt.timedelta(hours=rem_cooldown_hours)

                                if not azure_history.empty:
                                    recent_rem = azure_history[azure_history["last_remediation"] >= rem_cutoff]
                                    recent_rem_dsns = set(recent_rem["dsn"].astype(str).str.strip())
                                    no_device_filtered = [r for r in no_device if str(r["dsn"]).strip() not in recent_rem_dsns]
                                    skipped_rem_count = len(no_device) - len(no_device_filtered)
                                    if skipped_rem_count > 0:
                                        print(f"\nExcluding {skipped_rem_count:,} device(s) remediated within the past {rem_cooldown_hours}h:")
                                        az_rem_vin_updated = False
                                        for r in no_device:
                                            d = str(r["dsn"]).strip()
                                            if d in recent_rem_dsns:
                                                mask = azure_history["dsn"] == d
                                                hr = azure_history[mask].iloc[0]
                                                if pd.isna(hr["vin"]) or str(hr["vin"]).strip() == "":
                                                    vin_val = dsn_to_vin.get(d, "")
                                                    if vin_val:
                                                        azure_history.loc[mask, "vin"] = vin_val
                                                        az_rem_vin_updated = True
                                                vin_display = azure_history[mask].iloc[0]["vin"] if not pd.isna(azure_history[mask].iloc[0]["vin"]) else dsn_to_vin.get(d, "unknown")
                                                print(f"  DSN {d} / VIN {vin_display} — {hr['remediation_count']:,} total remediation(s), last: {hr['last_remediation'].strftime('%Y-%m-%d %H:%M UTC')}")
                                        if az_rem_vin_updated:
                                            _save_azure_history(azure_history)
                                else:
                                    no_device_filtered = no_device

                                remediation_results = []
                                for r in no_device_filtered:
                                    dsn = r["dsn"]
                                    vin = dsn_to_vin.get(str(dsn), "unknown")
                                    print(f"\nDSN {dsn} / VIN {vin}")
                                    answer = input("  Remediate this device? (y/n/q to quit): ").strip().lower()
                                    if answer == "q":
                                        print("Stopping remediation loop.")
                                        break
                                    if answer != "y":
                                        remediation_results.append({"dsn": dsn, "vin": vin, "result": "Skipped"})
                                        continue
                                    result_status = remediate_no_device_found_azure(vin, dsn, paccar_token, nexus_token)
                                    if "PACCAR token expired" in result_status:
                                        print(f"  Result: {result_status}")
                                        print("\n  [WARNING] PACCAR token expired.")
                                        new_paccar = input("  Enter new PACCAR API bearer token (or press Enter to skip): ").strip()
                                        if new_paccar:
                                            save_paccar_token(new_paccar)
                                            paccar_token = new_paccar
                                            result_status = remediate_no_device_found_azure(vin, dsn, paccar_token, nexus_token)
                                        else:
                                            print("  No token provided. Stopping remediation.")
                                            remediation_results.append({"dsn": dsn, "vin": vin, "result": result_status})
                                            break
                                    if "401 Unauthorized - Nexus token expired" in result_status:
                                        print(f"  Result: {result_status}")
                                        print("\n  [WARNING] Nexus token expired.")
                                        new_nexus = _prompt_nexus_token()
                                        if new_nexus:
                                            nexus_token = new_nexus
                                            result_status = remediate_no_device_found_azure(vin, dsn, paccar_token, nexus_token)
                                        else:
                                            print("  No token provided. Stopping remediation.")
                                            remediation_results.append({"dsn": dsn, "vin": vin, "result": result_status})
                                            break
                                    print(f"  Result: {result_status}")
                                    remediation_results.append({"dsn": dsn, "vin": vin, "result": result_status})

                                if remediation_results:
                                    rem_df = pd.DataFrame(remediation_results)
                                    filepath = save_results_to_csv(rem_df, filename=get_report_filename("tig_azure_bb_remediation"))
                                    print(f"\nRemediation report saved: {filepath}")

                                # Update remediation history for successful remediations
                                remediated_dsns = [r["dsn"] for r in remediation_results if r["result"] == "Success"]
                                if remediated_dsns:
                                    azure_history = _load_azure_history()
                                    now_utc = _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0)
                                    for dsn in remediated_dsns:
                                        vin = dsn_to_vin.get(str(dsn), "")
                                        mask = azure_history["dsn"] == dsn
                                        if mask.any():
                                            azure_history.loc[mask, "remediation_count"] += 1
                                            azure_history.loc[mask, "last_remediation"] = now_utc
                                            if vin and (pd.isna(azure_history.loc[mask, "vin"]).all() or (azure_history.loc[mask, "vin"] == "").all()):
                                                azure_history.loc[mask, "vin"] = vin
                                        else:
                                            azure_history = pd.concat([azure_history, pd.DataFrame([{
                                                "dsn": dsn, "vin": vin, "reboot_count": 0, "last_reboot": None,
                                                "remediation_count": 1, "last_remediation": now_utc,
                                            }])], ignore_index=True)
                                    _save_azure_history(azure_history)
                                    print(f"\nRemediation history updated for {len(remediated_dsns):,} device(s) → {AZURE_HISTORY_FILE}")
                            else:
                                print("[WARNING] No Nexus token provided. Skipping remediation.")


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


def set_nexus_ota_desired(dsn: str, nexus_token: str, enabled: bool, max_retries: int = 5, device_type: str = "yogi") -> tuple:
    """
    Set otaApp.otaEnabled for a device via PlatformScience cf-gateway API.
    device_type: "yogi" for Nexus/TIG units, "pmg" for Azure TIG units.
    Returns (success: bool, reason: str or None, http_status: int or None).
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
            "deviceType": device_type,
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
                return True, None, 200
            elif response.status_code == 401:
                return False, "401 Unauthorized - Nexus token expired or invalid", 401
            elif response.status_code == 404:
                return False, "404 Not Found", 404
            elif response.status_code == 400:
                try:
                    error_data = response.json()
                    detail = (error_data.get("message") or error_data.get("error")
                              or error_data.get("detail") or str(error_data))
                    return False, f"400 Bad Request - {detail}", 400
                except Exception:
                    return False, f"400 Bad Request - {response.text}", 400
            elif response.status_code >= 500:
                if attempt < max_retries - 1:
                    time.sleep(2 ** (attempt + 1))
                    continue
                return False, f"{response.status_code} Server error (max retries exceeded)", response.status_code
            else:
                return False, f"{response.status_code} API error", response.status_code
        except requests.Timeout:
            if attempt < max_retries - 1:
                time.sleep(2 ** (attempt + 1))
                continue
            return False, "Request timeout", None
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                time.sleep(2 ** (attempt + 1))
                continue
            return False, f"Network error: {e}", None
    return False, "Max retries exceeded", None


def enable_nexus_ota_for_devices(df: pd.DataFrame, nexus_token: str) -> None:
    """Set otaApp.otaEnabled = True for each Nexus device in df."""
    dsns = df["dsn"].dropna().astype(str).str.strip().tolist()
    print(f"\nSetting otaEnabled = True for {len(dsns):,} Nexus devices...")

    def _run_batch(token, batch_dsns):
        results = []
        for dsn in tqdm(batch_dsns, desc="Enabling OTA (Nexus)", unit="device"):
            success, reason, http_status = set_nexus_ota_desired(dsn, token, enabled=True)
            results.append({"dsn": dsn, "status": "Success" if success else "Failed", "reason": reason or "", "http_status": http_status})
        return results

    results = _run_batch(nexus_token, dsns)

    # Check for auth failures and retry with new token
    auth_failures = [r for r in results if r["status"] == "Failed" and "401" in r["reason"]]
    if auth_failures:
        print(f"\n[WARNING] {len(auth_failures):,} device(s) failed with 401 Unauthorized.")
        new_token = _prompt_nexus_token()
        if new_token:
            failed_dsns = {r["dsn"] for r in auth_failures}
            results = [r for r in results if r["dsn"] not in failed_dsns]
            print(f"\nRetrying {len(auth_failures):,} failed devices...")
            retry_results = _run_batch(new_token, [r["dsn"] for r in auth_failures])
            results.extend(retry_results)
        else:
            print(f"[WARNING] {len(auth_failures):,} device(s) skipped — no new token provided.")

    succeeded = sum(1 for r in results if r["status"] == "Success")
    failed = sum(1 for r in results if r["status"] == "Failed")
    print(f"\nNexus OTA enable complete: {succeeded:,} succeeded, {failed:,} failed")

    from collections import Counter
    http_counts = Counter(r["http_status"] for r in results)
    print("\nHTTP response summary:")
    for code, count in sorted(http_counts.items(), key=lambda x: (x[0] is None, x[0])):
        label = {200: "OK", 400: "Bad Request", 401: "Unauthorized", 404: "Not Found"}.get(code, "")
        code_str = str(code) if code is not None else "None (timeout/network error)"
        print(f"  {code_str}{' ' + label if label else ''}: {count:,}")

    results_df = pd.DataFrame(results)
    filepath = save_results_to_csv(results_df, filename=get_report_filename("nexus_ota_enable_results"))
    print(f"Enable results saved: {filepath}")


def _reset_nexus_single_device(dsn: str, nexus_token: str, device_type: str = "yogi") -> dict:
    """
    Worker: set otaEnabled=False, wait 60s, set otaEnabled=True for one device.
    device_type: "yogi" for Nexus/TIG units, "pmg" for Azure TIG units.
    Returns a result dict with dsn, status, reason, and failed_step.
    """
    import time as _time

    success, reason, _ = set_nexus_ota_desired(dsn, nexus_token, enabled=False, device_type=device_type)
    if not success:
        return {"dsn": dsn, "status": "Failed (set False)", "reason": reason, "failed_step": "set_false"}

    _time.sleep(60)

    success, reason, _ = set_nexus_ota_desired(dsn, nexus_token, enabled=True, device_type=device_type)
    if success:
        return {"dsn": dsn, "status": "Success", "reason": "", "failed_step": None}
    else:
        return {"dsn": dsn, "status": "Failed (set True)", "reason": reason, "failed_step": "set_true"}


def reset_nexus_ota_shadow_for_devices(ota_false_df: pd.DataFrame, nexus_token: str, device_type: str = "yogi") -> None:
    """
    Prompt once for the whole group, then in parallel (max NEXUS_RESET_MAX_WORKERS):
      1. Set otaApp.otaEnabled = False
      2. Wait 60 seconds (per worker)
      3. Set otaApp.otaEnabled = True
    device_type: "yogi" for Nexus/TIG units, "pmg" for Azure TIG units.
    """
    dsns = ota_false_df["dsn"].dropna().astype(str).str.strip().tolist()
    vins = ota_false_df.set_index(ota_false_df["dsn"].astype(str).str.strip())["vin"].to_dict()

    print(f"\nDevices to reset ({len(dsns):,}):")
    for dsn in dsns:
        print(f"  DSN {dsn} / VIN {vins.get(dsn, 'unknown')}")

    answer = input(f"\nReset shadow (False → wait 60s → True) for all {len(dsns):,} devices? (y/n): ").strip().lower()
    if answer != "y":
        print("Reset cancelled.")
        return

    print(f"\nResetting {len(dsns):,} devices with up to {NEXUS_RESET_MAX_WORKERS} parallel workers...")
    results = []
    with ThreadPoolExecutor(max_workers=NEXUS_RESET_MAX_WORKERS) as executor:
        futures = {executor.submit(_reset_nexus_single_device, dsn, nexus_token, device_type): dsn for dsn in dsns}
        for future in tqdm(as_completed(futures), total=len(dsns), desc="Resetting shadow", unit="device"):
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
                        futures[executor.submit(set_nexus_ota_desired, dsn, new_token, True, 5, device_type)] = dsn
                    else:
                        # Full reset needed
                        futures[executor.submit(_reset_nexus_single_device, dsn, new_token, device_type)] = dsn
                for future in tqdm(as_completed(futures), total=len(auth_failures), desc="Retrying (Nexus)", unit="device"):
                    dsn = futures[future]
                    try:
                        raw = future.result()
                        if isinstance(raw, tuple):
                            success, reason, _ = raw
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
    """Branch 3: TIG units running Nexus firmware 002.004.005, DSN 200,000 - 30,000,000."""
    print("\n" + "="*70)
    print("Branch 3: TIG Nexus Firmware Units (DSN 200,000 - 30,000,000, firmware 002.004.005)")
    print("="*70)

    if "dsn" not in df.columns:
        print("[WARNING] 'dsn' column not found. Cannot filter TIG Nexus units.")
        return

    work = df.copy()
    work["_dsn_num"] = pd.to_numeric(work["dsn"], errors="coerce")
    dsn_mask = (work["_dsn_num"] >= 200_000) & (work["_dsn_num"] <= 30_000_000)

    if "pmgSwVersion" in work.columns:
        fw_mask = work["pmgSwVersion"].astype(str).str.upper().isin(["002.004.005"])
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
            print("Token refresh did not resolve the issue. Please provide a new token manually.")
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
                print("[ERROR] Token still failing after manual replacement. Skipping shadow retrieval.")
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

    # Filter to ota_reported == False or None
    print("\n" + "-"*70)
    filter_false = input("Filter to units where ota_reported = False or None? (y/n): ").strip().lower()
    if filter_false == "y":
        ota_false = result[(result["ota_reported"] == False) | result["ota_reported"].isna()]
        print(f"\nUnits with ota_reported = False or None: {len(ota_false):,}")
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

    # Enable OTA for units where both ota_reported and ota_desired are None/missing
    both_none = result[result["ota_reported"].isna() & result["ota_desired"].isna()]
    print("\n" + "-"*70)
    print(f"Units where ota_reported = None AND ota_desired = None: {len(both_none):,}")
    if len(both_none) > 0:
        do_enable_none = input(f"Set ota_desired = True for these {len(both_none):,} units? (y/n): ").strip().lower()
        if do_enable_none == "y":
            nexus_token = load_nexus_token()
            if nexus_token:
                print("Using cached Nexus token...")
            else:
                nexus_token = _prompt_nexus_token()

            if nexus_token:
                enable_nexus_ota_for_devices(both_none, nexus_token)
            else:
                print("[WARNING] No Nexus token provided. Skipping enable step.")

    if filter_false == "y":
        if len(ota_false) > 0:
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

    # Filter to ota_reported == True with deprovision exclusion
    # Always computed so the BB Portal block can use it
    ota_true = result[result["ota_reported"] == True].copy()
    total_true = len(ota_true)

    DEPROVISION_FILE = "Units Impacted by PACCARs Deprovision Script.csv"
    excluded_count = 0
    try:
        deprov_df = pd.read_csv(DEPROVISION_FILE)
        vin_col = next((c for c in deprov_df.columns if c.strip().lower() == "vin"), None)
        if vin_col:
            excluded_vins = set(deprov_df[vin_col].dropna().astype(str).str.strip().str.upper())
            ota_true = ota_true[~ota_true["vin"].astype(str).str.strip().str.upper().isin(excluded_vins)]
            excluded_count = total_true - len(ota_true)
        else:
            print(f"[WARNING] No 'Vin' column found in {DEPROVISION_FILE}. Skipping exclusion.")
    except FileNotFoundError:
        print(f"[WARNING] {DEPROVISION_FILE} not found. Skipping exclusion.")
    except Exception as e:
        print(f"[WARNING] Could not load {DEPROVISION_FILE}: {e}. Skipping exclusion.")

    print("\n" + "-"*70)
    filter_true = input("Filter to units where ota_reported = True? (y/n): ").strip().lower()
    if filter_true == "y":
        print(f"\nUnits with ota_reported = True: {total_true:,}")
        print(f"  Excluded by deprovision file: {excluded_count:,}")
        print(f"  Remaining: {len(ota_true):,}")

        if len(ota_true) > 0:
            filepath = save_results_to_csv(ota_true, filename=get_report_filename("nexus_ota_true"))
            print(f"\nReport saved: {filepath}")

    # Log request loop — ota_reported=True units remaining after deprovision exclusion
    print("\n" + "-"*70)
    print(f"Units for log request (ota_reported=True, not in deprovision file): {len(ota_true):,}")

    if len(ota_true) > 0:
        do_logs = input("Request gateway logs for these units? (y/n): ").strip().lower()
        if do_logs == "y":
            nexus_token = load_nexus_token()
            if nexus_token:
                print("Using cached Nexus token...")
            else:
                nexus_token = _prompt_nexus_token()

            if nexus_token:
                log_results = []
                for _, row in ota_true.iterrows():
                    dsn = str(row["dsn"]).strip()
                    vin = str(row.get("vin", "unknown"))
                    answer = input(f"\n  DSN {dsn} / VIN {vin} — request logs? (y/n/q to quit): ").strip().lower()
                    if answer == "q":
                        print("  Stopping log request loop.")
                        break
                    if answer != "y":
                        log_results.append({"dsn": dsn, "vin": vin, "log_status": "Skipped"})
                        continue
                    success, reason = request_nexus_logs(dsn, nexus_token)
                    if not success and "401" in (reason or ""):
                        print(f"  Result: Failed: {reason}")
                        print("\n  [WARNING] Nexus token expired or invalid.")
                        nexus_token = _prompt_nexus_token()
                        if not nexus_token:
                            print("  No token provided. Stopping log request loop.")
                            log_results.append({"dsn": dsn, "vin": vin, "log_status": f"Failed: {reason}"})
                            break
                        success, reason = request_nexus_logs(dsn, nexus_token)
                    status = "Requested" if success else f"Failed: {reason}"
                    print(f"  Result: {status}")
                    log_results.append({"dsn": dsn, "vin": vin, "log_status": status})

                if log_results:
                    requested = sum(1 for r in log_results if r["log_status"] == "Requested")
                    print(f"\nLog requests complete: {requested:,}/{len(log_results):,} sent.")
            else:
                print("[WARNING] No Nexus token provided. Skipping log requests.")

    # BB Portal lookup — ota_reported=True units remaining after deprovision exclusion
    print("\n" + "-"*70)
    print(f"Units for BB Portal lookup (ota_reported=True, not in deprovision file): {len(ota_true):,}")

    if len(ota_true) > 0:
        fetch_bb = input("Retrieve BB Portal data for these units? (y/n): ").strip().lower()
        if fetch_bb == "y":
            bb_token = load_bb_token()
            if bb_token:
                print("Using cached BB Portal token...")
            else:
                bb_token = _prompt_bb_token()

            if bb_token:
                dsns = ota_true["dsn"].dropna().astype(str).str.strip().unique().tolist()
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
                        failed_dsns = {r["dsn"] for r in auth_failures}
                        bb_results = [r for r in bb_results if r["dsn"] not in failed_dsns]
                        with ThreadPoolExecutor(max_workers=BB_MAX_WORKERS) as executor:
                            retry_futures = {executor.submit(fetch_bb_data_for_device, r["dsn"], new_token): r["dsn"]
                                             for r in auth_failures}
                            for future in tqdm(as_completed(retry_futures), total=len(auth_failures), desc="Retrying BB Portal", unit="device"):
                                bb_results.append(future.result())
                    else:
                        print(f"[WARNING] {len(auth_failures):,} device(s) skipped — no new token provided.")

                succeeded = sum(1 for r in bb_results if r["status"] == "Success")
                not_found = sum(1 for r in bb_results if r["status"] == "No device found")
                failed = sum(1 for r in bb_results if r["status"] == "Failed")
                with_invalidation = sum(1 for r in bb_results if r["status"] == "Success" and r.get("backup_invalidation"))
                print(f"\nBB Portal Summary:")
                print(f"  Succeeded:                   {succeeded:,}")
                print(f"  No device found:             {not_found:,}")
                print(f"  Failed:                      {failed:,}")
                print(f"  With backup_invalidation:    {with_invalidation:,}")
                print(f"  Without backup_invalidation: {succeeded - with_invalidation:,}")

                # Merge results back onto ota_true and save
                bb_df = pd.DataFrame(bb_results)[["dsn", "bb_device_id", "backup_invalidation", "last_scan", "status", "reason"]]
                bb_df["dsn"] = bb_df["dsn"].astype(str)
                ota_true_out = ota_true.copy()
                ota_true_out["dsn"] = ota_true_out["dsn"].astype(str)
                ota_true_out = ota_true_out.merge(bb_df, on="dsn", how="left")
                filepath = save_results_to_csv(ota_true_out, filename=get_report_filename("nexus_bb_portal"))
                print(f"\nReport saved: {filepath}")


def _analyze_tig_other_units(df: pd.DataFrame) -> None:
    """Branch 5: TIG units (DSN 20,000,000 - 30,000,000) not matched by Branches 2, 3, or 4."""
    print("="*70)
    print("Branch 5: All Other TIG Units (DSN 20,000,000 - 30,000,000)")
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
        branch3_mask = fw.isin(["007.002.026", "007.003.026"])
        branch4_mask = fw.isin(["002.004.005"])
        result = work[dsn_mask & ~branch2_mask & ~branch3_mask & ~branch4_mask].drop(columns=["_dsn_num"])
    else:
        print("[WARNING] 'pmgSwVersion' column not found. Cannot exclude Branch 2/3/4 firmware versions.")
        result = work[dsn_mask].drop(columns=["_dsn_num"])

    print(f"Other TIG units (not matching Branch 2 or Branch 3): {len(result):,}")
    if len(result) > 0:
        if "pmgSwVersion" in result.columns:
            fw_counts = result["pmgSwVersion"].value_counts(dropna=False)
            print("\nFirmware version breakdown:")
            print(fw_counts.to_string())

        display_cols = ["vin", "dsn", "pmgSwVersion", "lastUpdated"]
        available_cols = [c for c in display_cols if c in result.columns]
        print(f"\nUnit details:")
        print(result[available_cols].to_string(index=False))


def _trigger_enablement_flow(pending_df: pd.DataFrame) -> None:
    """
    Final step: for devices that appear in pending_df AND either TDMG or Azure history,
    filter by a configurable cooldown since last enable, then call activate_pending_enable
    for each eligible device.
    """
    import datetime as _dt

    print("="*70)
    print("Final Step: Trigger Enablement Flow Again")
    print("="*70)

    if "dsn" not in pending_df.columns or "vin" not in pending_df.columns:
        print("[WARNING] pending_df missing 'dsn' or 'vin' column. Cannot run enablement flow.")
        return

    # Load both history files
    tdmg_history = _load_tdmg_reset_history()
    azure_history = _load_azure_history()

    def _norm_dsn(s):
        """Normalize a DSN series to plain integer strings (strips .0 from float-read values)."""
        return pd.to_numeric(s, errors="coerce").dropna().astype("int64").astype(str)

    # Tag source so we know which file to update
    tdmg_dsns = set(_norm_dsn(tdmg_history["dsn"])) if not tdmg_history.empty else set()
    azure_dsns = set(_norm_dsn(azure_history["dsn"])) if not azure_history.empty else set()
    known_dsns = tdmg_dsns | azure_dsns

    if not known_dsns:
        print("No history records found in either TDMG or Azure history files. Nothing to do.")
        return

    # Find pending devices that have a history record
    pending_df = pending_df.copy()
    pending_df["_dsn_str"] = pd.to_numeric(pending_df["dsn"], errors="coerce").astype("Int64").astype(str).str.replace("<NA>", "", regex=False).str.strip()
    eligible = pending_df[pending_df["_dsn_str"].isin(known_dsns)].copy()
    eligible = eligible.drop(columns=["_dsn_str"])

    pending_dsn_norm = pd.to_numeric(pending_df["dsn"], errors="coerce").astype("Int64").astype(str).str.replace("<NA>", "", regex=False).str.strip()
    print(f"Devices in pending list with a history record: {len(eligible):,} "
          f"(TDMG: {pending_dsn_norm.isin(tdmg_dsns).sum():,}, "
          f"Azure: {pending_dsn_norm.isin(azure_dsns).sum():,})")

    if eligible.empty:
        print("No matching devices found. Nothing to do.")
        return

    # Cooldown filter
    DEFAULT_COOLDOWN = 24
    cooldown_input = input(f"\nExclude devices enabled within the past how many hours? (default {DEFAULT_COOLDOWN}): ").strip()
    try:
        cooldown_hours = int(cooldown_input) if cooldown_input else DEFAULT_COOLDOWN
    except ValueError:
        print(f"  Invalid input. Using default {DEFAULT_COOLDOWN}h.")
        cooldown_hours = DEFAULT_COOLDOWN

    now_utc = _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0)
    cutoff = now_utc - _dt.timedelta(hours=cooldown_hours)

    # Build a combined last_enable lookup from both history files
    last_enable_map = {}
    for _, row in tdmg_history.iterrows():
        d = str(row["dsn"]).strip()
        if pd.notna(row["last_enable"]):
            last_enable_map[d] = row["last_enable"]
    for _, row in azure_history.iterrows():
        d = str(row["dsn"]).strip()
        if pd.notna(row["last_enable"]):
            # Take the more recent timestamp if present in both
            existing = last_enable_map.get(d)
            if existing is None or row["last_enable"] > existing:
                last_enable_map[d] = row["last_enable"]

    skipped = []
    candidates = []
    for _, row in eligible.iterrows():
        d = str(row["dsn"]).strip()
        last_enable = last_enable_map.get(d)
        if last_enable is not None and last_enable >= cutoff:
            skipped.append((d, str(row.get("vin", "unknown")), last_enable))
        else:
            candidates.append(row)

    if skipped:
        print(f"\nExcluding {len(skipped):,} device(s) enabled within the past {cooldown_hours}h:")
        for d, vin, ts in skipped:
            print(f"  DSN {d} / VIN {vin} — last enabled: {ts.strftime('%Y-%m-%d %H:%M UTC')}")

    if not candidates:
        print(f"\nAll devices were enabled within the past {cooldown_hours}h. Nothing to do.")
        return

    print(f"\nDevices eligible for enablement: {len(candidates):,}")
    do_enable = input("Trigger enablement for these devices? (y/n): ").strip().lower()
    if do_enable != "y":
        print("Skipping enablement.")
        return

    paccar_token = load_paccar_token()
    if paccar_token:
        print("Using cached PACCAR token...")
    else:
        paccar_token = input("Enter PACCAR API bearer token (or press Enter to skip): ").strip()
        if not paccar_token:
            print("[WARNING] No PACCAR token. Skipping enablement.")
            return
        save_paccar_token(paccar_token)

    enable_results = []
    for row in candidates:
        dsn = str(row["dsn"]).strip()
        vin = str(row.get("vin", "unknown"))
        try:
            success, reason = activate_pending_enable(vin, dsn, paccar_token)
        except PACCARAuthenticationError:
            print(f"\n  [WARNING] PACCAR token expired on DSN {dsn}.")
            new_paccar = input("  Enter new PACCAR API bearer token (or press Enter to stop): ").strip()
            if new_paccar:
                save_paccar_token(new_paccar)
                paccar_token = new_paccar
                try:
                    success, reason = activate_pending_enable(vin, dsn, paccar_token)
                except PACCARAuthenticationError:
                    enable_results.append({"dsn": dsn, "vin": vin, "result": "Failed - PACCAR token expired after retry"})
                    print("  Stopping enablement loop.")
                    break
            else:
                print("  No token provided. Stopping enablement loop.")
                enable_results.append({"dsn": dsn, "vin": vin, "result": "Failed - PACCAR token expired"})
                break

        result_str = "Success" if success else f"Failed: {reason}"
        print(f"  DSN {dsn} / VIN {vin}: {result_str}")
        enable_results.append({"dsn": dsn, "vin": vin, "result": result_str})

    if not enable_results:
        return

    # Summary
    attempted = [r for r in enable_results if r["result"] != "Skipped"]
    succeeded_results = [r for r in attempted if r["result"] == "Success"]
    print(f"\nEnablement complete: {len(succeeded_results):,}/{len(attempted):,} succeeded")

    # Save report
    report_df = pd.DataFrame(enable_results)
    filepath = save_results_to_csv(report_df, filename=get_report_filename("enablement_flow"))
    print(f"Report saved: {filepath}")

    # Update enable history in whichever file each DSN belongs to
    succeeded_dsns = [r["dsn"] for r in succeeded_results]
    if not succeeded_dsns:
        return

    # Reload both files fresh before updating
    tdmg_history = _load_tdmg_reset_history()
    azure_history = _load_azure_history()
    tdmg_dsns = set(tdmg_history["dsn"].astype(str).str.strip()) if not tdmg_history.empty else set()
    azure_dsns = set(azure_history["dsn"].astype(str).str.strip()) if not azure_history.empty else set()

    vin_map = {r["dsn"]: r["vin"] for r in enable_results}
    tdmg_updated = False
    azure_updated = False

    for dsn in succeeded_dsns:
        vin = vin_map.get(dsn, "")

        if dsn in tdmg_dsns:
            mask = tdmg_history["dsn"] == dsn
            tdmg_history.loc[mask, "enable_count"] = tdmg_history.loc[mask, "enable_count"].fillna(0).astype(int) + 1
            tdmg_history.loc[mask, "last_enable"] = now_utc
            if vin and (pd.isna(tdmg_history.loc[mask, "vin"]).all() or (tdmg_history.loc[mask, "vin"] == "").all()):
                tdmg_history.loc[mask, "vin"] = vin
            tdmg_updated = True

        if dsn in azure_dsns:
            mask = azure_history["dsn"] == dsn
            azure_history.loc[mask, "enable_count"] = azure_history.loc[mask, "enable_count"].fillna(0).astype(int) + 1
            azure_history.loc[mask, "last_enable"] = now_utc
            if vin and (pd.isna(azure_history.loc[mask, "vin"]).all() or (azure_history.loc[mask, "vin"] == "").all()):
                azure_history.loc[mask, "vin"] = vin
            azure_updated = True

        # DSN not yet in either file — add to TDMG as a new row (fallback)
        if dsn not in tdmg_dsns and dsn not in azure_dsns:
            tdmg_history = pd.concat([tdmg_history, pd.DataFrame([{
                "dsn": dsn, "vin": vin,
                "reset_count": 0, "last_reset": None,
                "remediation_count": 0, "last_remediation": None,
                "enable_count": 1, "last_enable": now_utc,
            }])], ignore_index=True)
            tdmg_updated = True

    if tdmg_updated:
        _save_tdmg_reset_history(tdmg_history)
        print(f"TDMG history updated → {TDMG_RESET_HISTORY_FILE}")
    if azure_updated:
        _save_azure_history(azure_history)
        print(f"Azure history updated → {AZURE_HISTORY_FILE}")


if __name__ == "__main__":
    main()
