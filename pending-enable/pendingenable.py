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
PACCAR_MAX_WORKERS = 5

REPORTS_DIR = "reports"
PACCAR_TOKEN_FILE = ".paccar_token"

# Create reports directory if it doesn't exist
os.makedirs(REPORTS_DIR, exist_ok=True)

logger = logging.getLogger(__name__)


class PACCARAuthenticationError(Exception):
    """Raised when PACCAR API returns 401 Unauthorized."""
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


def find_most_recent_csv(pattern: str = "pending_updates_*.csv") -> Optional[str]:
    """
    Find the most recently modified CSV file matching the pattern in reports directory.

    Args:
        pattern: Glob pattern for files to search (default: "pending_updates_*.csv")

    Returns:
        Full path to most recent file, or None if no files found
    """
    import glob
    search_pattern = os.path.join(REPORTS_DIR, pattern)
    files = glob.glob(search_pattern)
    if not files:
        return None
    # Sort by modification time (most recent first)
    return max(files, key=os.path.getmtime)


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
        else:
            pending_df = download_pending_updates()
    else:
        pending_df = download_pending_updates()

    if pending_df is None:
        print("Failed to download pending updates.")
        sys.exit(1)

    # Ask for PACCAR bearer token
    print("\n" + "="*70)
    print("PACCAR Solutions Data Retrieval (Optional)")
    print("="*70)
    retrieve_paccar = input("Retrieve PACCAR Solutions data for these vehicles? (y/n): ").strip().lower()

    paccar_retrieved = False
    if retrieve_paccar == "y":
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
        if paccar_retrieved:
            print("\n" + "="*70)
            paccar_filepath = save_results_to_csv(pending_df)
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
    filter_dates = input("Filter to vehicles with lastUpdated within past X hours? (y/n): ").strip().lower()

    filter_applied = False
    if filter_dates == "y":
        hours_input = input("Enter number of hours (default 24): ").strip()
        debug_filter = input("Enable debug output? (y/n): ").strip().lower() == "y"
        try:
            hours = int(hours_input) if hours_input else 24
            pending_df = filter_by_last_updated(pending_df, hours=hours, debug=debug_filter)
            filter_applied = True
        except ValueError:
            print(f"[ERROR] Invalid input '{hours_input}'. Using default 24 hours.")
            pending_df = filter_by_last_updated(pending_df, hours=24, debug=debug_filter)
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


if __name__ == "__main__":
    main()
