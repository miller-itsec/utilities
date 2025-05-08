# ==============================================================================
# MetaDefender Cloud API Malware Sample Downloader
# ==============================================================================
#
# Purpose:
# --------
# This script continuously fetches malware hash identifiers (SHA256) from the
# MetaDefender Cloud API v4 Hash Feed (`/feed/hashes`) and attempts to download
# the corresponding malware samples using the File Download API (`/file/{hash}/download`).
# It is designed for automated bulk acquisition of malware samples for research
# and analysis purposes.
#
# Key Features:
# -------------
#   - Parallel Downloading: Utilizes a ThreadPoolExecutor to download multiple
#     files concurrently, significantly speeding up the acquisition process.
#     (Configurable via --threads).
#   - Resilient Operations: Implements configurable retries with delays for
#     network errors or temporary API issues during hash feed fetching,
#     download link retrieval, and file downloading.
#   - State Persistence: Remembers previously processed hashes by logging them
#     to `processed_hashes.log`. On subsequent runs, hashes listed in this
#     log are skipped, preventing redundant work and API calls.
#   - Duplicate Download Prevention: Avoids re-downloading files that already
#     exist in the target download directory (checks by SHA256 filename).
#   - Accurate Download Limiting: Optionally accepts a `--maximum-download`
#     limit which precisely controls the number of download *attempts* initiated,
#     ensuring the number of *successful* downloads does not exceed the limit.
#     Uses a combination of pending and completed download counts for accuracy.
#     If no limit is specified, it runs until the API feed is exhausted.
#   - Comprehensive Logging: Logs detailed information about its progress,
#     downloads, skips, errors, and API interactions to both the console and
#     a file (`malware_downloader.log`). Includes timestamps and thread names.
#   - Summary Reporting: Generates a detailed report upon completion, summarizing
#     key statistics like total hashes seen, files downloaded, skipped counts,
#     failed attempts, total duration, download rates, HTTP status codes,
#     and encountered errors. The report is printed and saved to a file.
#   - Error Handling: Specifically handles 404 Not Found errors for download
#     links and files by not retrying unnecessarily. Attempts to clean up
#     partially downloaded files on error.
#
# Usage:
# ------
#   python download_samples.py [--maximum-download N] [--threads M]
#
#   Arguments:
#     --maximum-download N : (Optional) Stop after successfully downloading N samples.
#                            If omitted, runs indefinitely until the API feed ends.
#                            Set to 0 to run through setup and exit (useful for testing config).
#     --threads M          : (Optional) Number of parallel download threads. Default is 8.
#
# Configuration:
# --------------
#   - API_KEY: **MUST** be set to your valid MetaDefender Cloud API key.
#   - DOWNLOAD_DIR: Directory where downloaded samples will be saved (default: "downloaded_samples").
#   - PROCESSED_LOG_FILE: File to track processed hashes (default: "processed_hashes.log").
#   - LOG_FILE: File for detailed operational logs (default: "malware_downloader.log").
#   - MAX_RETRIES, RETRY_DELAY, TIMEOUTS: Constants controlling network resilience.
#
# Note on Reliability:
# --------------------
# This version incorporates robust state tracking and precise limit checking
# based on initiated download attempts, aiming for reliable operation and
# accurate adherence to the specified download limits, while maintaining
# parallel download performance.
#
# ==============================================================================
import requests
import json
import threading
import os
import time
import argparse
import logging
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor

# --- Configuration ---
API_KEY = ""
BASE_URL = "https://api.metadefender.com/v4"
HASH_FEED_ENDPOINT = f"{BASE_URL}/feed/hashes"
FILE_DOWNLOAD_ENDPOINT = f"{BASE_URL}/file"
DOWNLOAD_DIR = "downloaded_samples"
PROCESSED_LOG_FILE = "processed_hashes.log"
LOG_FILE = "malware_downloader.log" 
MAX_RETRIES = 3
RETRY_DELAY = 5
REQUEST_TIMEOUT = 15
DOWNLOAD_CHUNK_TIMEOUT = 30

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='a'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Argument Parsing ---
parser = argparse.ArgumentParser(description="MetaDefender malware downloader with advanced features.")
parser.add_argument("--maximum-download", type=int, default=None,
                    help="Maximum number of successful samples to acquire. If not set, downloads indefinitely. Set to 0 to download none.")
parser.add_argument("--threads", type=int, default=16, help="Number of threads for parallel downloading.")
args = parser.parse_args()

MAX_DOWNLOAD = args.maximum_download
NUM_THREADS = args.threads

# --- Global State Tracking ---
total_hashes_seen = 0
downloaded_count = 0                # Successfully downloaded files
pending_downloads_count = 0         # Tasks submitted that are expected to attempt a download
skipped_existing_count = 0
skipped_processed_count_from_log = 0
skipped_processed_in_session = 0
failed_count = 0                    # Downloads that failed after all retries
start_time = time.time() 
download_sizes = {}
download_rates = {}
http_status_counts = {}
error_counts = {}
processed_hashes = set()            # Hashes loaded from log + hashes for which a download attempt was initiated this session
download_count_lock = threading.Lock() # Protects downloaded_count, pending_downloads_count, should_stop
should_stop = False

# --- Helper Functions ---
def load_processed_hashes():
    global skipped_processed_count_from_log # Mark this global as it's modified
    loaded_set = set()
    if os.path.exists(PROCESSED_LOG_FILE):
        try:
            with open(PROCESSED_LOG_FILE, 'r') as f:
                loaded_set = set(line.strip() for line in f if line.strip())
            skipped_processed_count_from_log = len(loaded_set) # Count how many were loaded initially
        except IOError as e:
            logger.error(f"Failed to load processed hashes from {PROCESSED_LOG_FILE} - {e}")
    return loaded_set

def save_processed_hash(sha256_hash):
    try:
        with open(PROCESSED_LOG_FILE, 'a') as f:
            f.write(f"{sha256_hash}\n")
    except IOError as e:
        logger.error(f"Failed to save processed hash {sha256_hash} to {PROCESSED_LOG_FILE} - {e}")

def update_statistics(status_code=None, error_type=None, filename=None, file_size=None, download_time=None):
    global skipped_existing_count # Only this one is directly modified here from the global counters
    if status_code:
        http_status_counts[status_code] = http_status_counts.get(status_code, 0) + 1
    if error_type: # General error types
        error_counts[error_type] = error_counts.get(error_type, 0) + 1
    if filename and file_size is not None and download_time is not None and download_time > 0:
        download_sizes[filename] = file_size
        download_rates[filename] = file_size / download_time
    elif filename and file_size is not None:
        download_sizes[filename] = file_size
    elif error_type == "Skipped Existing":
        skipped_existing_count += 1

def fetch_hashes_page(page_number):
    headers = {'apikey': API_KEY}
    params = {'page': page_number}
    response = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            logger.info(f"Fetching hash feed page {page_number}, attempt {attempt+1}/{MAX_RETRIES+1}...")
            response = requests.get(HASH_FEED_ENDPOINT, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
            update_statistics(status_code=response.status_code)
            response.raise_for_status()
            try:
                hashes_data = response.json()
                if 'data' in hashes_data and isinstance(hashes_data['data'], list):
                    return hashes_data
                else:
                    logger.warning(f"Page {page_number} returned malformed data structure. Response: {response.text[:200]}")
                    return {'data': []} # Return structure with empty data to allow graceful handling
            except json.JSONDecodeError:
                logger.error(f"JSONDecodeError on hash feed page {page_number}. Response: {response.text[:200]}")
                update_statistics(error_type=f"JSON Decode Error (Hash Feed - Page {page_number})")
                return None # Error
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching hash feed page {page_number} (attempt {attempt+1}/{MAX_RETRIES+1}) - {e}")
            if response is not None: update_statistics(status_code=response.status_code)
            if attempt < MAX_RETRIES: time.sleep(RETRY_DELAY)
            else:
                update_statistics(error_type=f"API Request Failed (Hash Feed - Page {page_number})")
                return None # Final error
    return None

def get_download_link(sha256_hash):
    url = f"{FILE_DOWNLOAD_ENDPOINT}/{sha256_hash}/download"
    headers = {'apikey': API_KEY}
    response_obj = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            logger.debug(f"Getting download link for {sha256_hash}, attempt {attempt+1}/{MAX_RETRIES+1}...")
            response_obj = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            update_statistics(status_code=response_obj.status_code)
            if response_obj.status_code == 404:
                logger.warning(f"Download link not found (404) for {sha256_hash}.")
                return None
            response_obj.raise_for_status()
            data = response_obj.json()
            return data.get('file_path')
        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting download link for {sha256_hash} (attempt {attempt+1}/{MAX_RETRIES+1}) - {e}")
            if attempt < MAX_RETRIES: time.sleep(RETRY_DELAY)
            else:
                status_to_log = response_obj.status_code if response_obj is not None else None
                update_statistics(status_code=status_to_log, error_type=f"API Request Failed (Download Link - {sha256_hash})")
                return None
        except json.JSONDecodeError:
            logger.error(f"JSONDecodeError getting download link for {sha256_hash}. Response: {response_obj.text[:200] if response_obj else 'N/A'}")
            update_statistics(error_type=f"JSON Decode Error (Download Link - {sha256_hash})")
            return None
    return None

def download_file(download_url, original_filename, sha256_hash):
    global downloaded_count, failed_count # These are modified here

    filepath = os.path.join(DOWNLOAD_DIR, sha256_hash if sha256_hash else original_filename)

    if os.path.exists(filepath):
        logger.info(f"File {filepath} already exists. Skipping download.")
        update_statistics(error_type="Skipped Existing")
        return True # Considered "available"

    response_obj = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            logger.info(f"Starting download: {original_filename} (SHA256: {sha256_hash}) to {filepath}, attempt {attempt+1}/{MAX_RETRIES+1}...")
            dl_start_time = time.time()
            response_obj = requests.get(download_url, stream=True, timeout=(REQUEST_TIMEOUT, DOWNLOAD_CHUNK_TIMEOUT))
            update_statistics(status_code=response_obj.status_code)
            response_obj.raise_for_status()
            
            total_size_in_bytes = 0
            with open(filepath, 'wb') as file:
                for chunk in response_obj.iter_content(chunk_size=8192):
                    file.write(chunk)
                    total_size_in_bytes += len(chunk)
            dl_end_time = time.time()
            download_time = dl_end_time - dl_start_time
            
            update_statistics(filename=filepath, file_size=total_size_in_bytes, download_time=download_time)
            
            with download_count_lock: # Acquired for downloaded_count
                old_dl_count = downloaded_count
                downloaded_count += 1
                logger.info(f"DOWNLOADED {filepath} ({total_size_in_bytes} B) in {download_time:.2f}s. Total downloaded: {downloaded_count} (was {old_dl_count}).")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Download error for {original_filename} ({filepath}), attempt {attempt+1}/{MAX_RETRIES+1} - {e}")
            if os.path.exists(filepath): # Cleanup partial file
                try: os.remove(filepath); logger.debug(f"Removed partial file: {filepath}")
                except OSError as oe: logger.error(f"Error removing partial file {filepath}: {oe}")
            
            if attempt < MAX_RETRIES: time.sleep(RETRY_DELAY)
            else: # Final attempt failed
                logger.error(f"FINAL FAILED download for {original_filename} ({filepath}) after {MAX_RETRIES+1} attempts.")
                status_to_log = response_obj.status_code if response_obj is not None else None
                specific_error_key = f"Download Error Final ({original_filename})" # Group errors by filename
                error_counts[specific_error_key] = error_counts.get(specific_error_key, 0) + 1
                if status_to_log: http_status_counts[status_to_log] = http_status_counts.get(status_to_log, 0) + 1
                
                with download_count_lock: # Acquired for failed_count
                    failed_count += 1
                return False # Download ultimately failed
    return False # Should not be reached if loop runs correctly

def process_hash_entry(hash_entry):
    global pending_downloads_count # Modified in finally block

    sha256_hash = hash_entry.get('sha256')
    original_filename = "unknown_filename" # Default
    
    try:
        # Derive original_filename (best effort)
        download_key_value = hash_entry.get('download')
        if download_key_value:
            try:
                parsed_path = urlparse(download_key_value).path
                if parsed_path and os.path.basename(parsed_path):
                    original_filename = os.path.basename(parsed_path)
                elif parsed_path == '/': # Handle case like http://example.com/
                     original_filename = sha256_hash # Fallback if path is just "/"
                # else keep "unknown_filename" if path is empty or basename is empty
            except Exception as e:
                logger.debug(f"Could not parse original_filename from download key '{download_key_value}': {e}")
        
        if original_filename == "unknown_filename": # Further fallback
            original_filename = sha256_hash or hash_entry.get('sha1') or hash_entry.get('md5') or "unknown_entry"

        if not sha256_hash:
            logger.warning(f"Entry missing SHA256. Filename guess: '{original_filename}'. Entry: {str(hash_entry)[:100]}")
            error_counts["Missing SHA256 in feed"] = error_counts.get("Missing SHA256 in feed", 0) + 1
            return # pending_downloads_count was not incremented for this, so no decrement needed in finally

        logger.debug(f"Processing hash: {sha256_hash} ({original_filename})")
        download_link = get_download_link(sha256_hash)
        if download_link:
            if download_file(download_link, original_filename, sha256_hash):
                save_processed_hash(sha256_hash)
        else:
            logger.warning(f"No download link for {sha256_hash} ({original_filename}). Download attempt aborted.")
            # No specific error for statistics here, as get_download_link would have logged its own failure (e.g., 404)
            
    except Exception as e:
        logger.error(f"Unexpected error in process_hash_entry for {sha256_hash if sha256_hash else 'unknown hash'}: {e}", exc_info=True)
    finally:
        if sha256_hash: # Only decrement if pending_downloads_count was incremented for this valid hash
            with download_count_lock:
                logger.debug(f"Task for {sha256_hash} ending. Decrementing pending_downloads_count from {pending_downloads_count}.")
                if pending_downloads_count > 0:
                    pending_downloads_count -= 1
                else: # Should ideally not happen if logic is correct
                    logger.warning(f"Attempted to decrement pending_downloads_count for {sha256_hash} but it was already 0 or less.")
                logger.debug(f"Pending_downloads_count is now {pending_downloads_count} for {sha256_hash}.")


def fetch_and_process_hashes(start_page=1):
    global total_hashes_seen, should_stop, processed_hashes, skipped_processed_in_session, pending_downloads_count, downloaded_count

    page_number = start_page
    while True:
        with download_count_lock: # Check global should_stop under lock before fetching page
            if should_stop:
                logger.info("Stop signal active. Exiting hash fetching loop.")
                break
        
        logger.info(f"Requesting hash feed page {page_number}...")
        hashes_data_page = fetch_hashes_page(page_number)

        if hashes_data_page and 'data' in hashes_data_page:
            current_page_hashes_from_api = hashes_data_page['data']
            if not current_page_hashes_from_api:
                logger.info(f"Page {page_number} is valid but contains no hashes. Assuming end of relevant feed.")
                break 

            logger.info(f"Received {len(current_page_hashes_from_api)} hashes on page {page_number}.")
            
            page_hash_list_for_submission = []
            page_internal_duplicates_skipped = 0
            temp_page_seen_hashes = set() # To de-duplicate within the current fetched page

            for entry in current_page_hashes_from_api:
                total_hashes_seen +=1 
                sha256_for_check = entry.get('sha256')
                if not sha256_for_check: 
                    logger.debug(f"Entry missing SHA256 on page {page_number}, skipping: {str(entry)[:100]}")
                    continue

                if sha256_for_check in processed_hashes:
                    logger.debug(f"Hash {sha256_for_check} (page {page_number}) already in global processed_hashes set. Skipping.")
                    skipped_processed_in_session += 1
                    continue
                if sha256_for_check in temp_page_seen_hashes:
                    logger.debug(f"Hash {sha256_for_check} (page {page_number}) is a duplicate within this page batch. Skipping.")
                    page_internal_duplicates_skipped +=1
                    continue
                
                page_hash_list_for_submission.append(entry)
                temp_page_seen_hashes.add(sha256_for_check)
            
            if page_internal_duplicates_skipped > 0:
                logger.info(f"Skipped {page_internal_duplicates_skipped} duplicates from within page {page_number} itself.")

            if not page_hash_list_for_submission:
                logger.info(f"No new, unique hashes to process on page {page_number} after all filtering.")
                page_number += 1
                time.sleep(0.1) 
                continue

            logger.info(f"Attempting to submit {len(page_hash_list_for_submission)} unique, new hashes from page {page_number} for processing.")
            futures = []
            with ThreadPoolExecutor(max_workers=NUM_THREADS, thread_name_prefix='Downloader') as executor:
                for entry in page_hash_list_for_submission:
                    current_sha256 = entry.get('sha256') # Already validated this exists and is unique for submission
                    
                    submit_this_task = False
                    with download_count_lock:
                        # This state is for the decision to submit THIS task
                        state_msg = (f"MAX_DOWNLOAD={MAX_DOWNLOAD}, Downloaded={downloaded_count}, "
                                     f"Pending={pending_downloads_count}, Sum_Pending_Downloaded={downloaded_count + pending_downloads_count}, "
                                     f"ShouldStop={should_stop}")
                        logger.info(f"Lock for submission check of {current_sha256}. State: {state_msg}")

                        if should_stop:
                            logger.info(f"should_stop is True for {current_sha256}. Not submitting.")
                        elif MAX_DOWNLOAD is not None and (downloaded_count + pending_downloads_count >= MAX_DOWNLOAD):
                            logger.info(f"Submission limit would be exceeded for {current_sha256}. "
                                        f"(Sum: {downloaded_count + pending_downloads_count} >= MAX_DOWNLOAD: {MAX_DOWNLOAD}). "
                                        f"Setting should_stop=True.")
                            should_stop = True 
                        else:
                            pending_downloads_count += 1
                            submit_this_task = True
                            logger.debug(f"Approved submission for {current_sha256}. Incremented pending_downloads_count to {pending_downloads_count}.")
                    
                    if should_stop: # Check flag (now potentially updated) outside the lock to break the loop
                        logger.info(f"Stop signal now active after check for {current_sha256}. Halting further submissions for this page.")
                        break # Break from this 'for entry...' submission loop

                    if submit_this_task:
                        processed_hashes.add(current_sha256) # Add to global "attempted this session" set
                        futures.append(executor.submit(process_hash_entry, entry))
                    # else: if should_stop became true for THIS item, loop will break.

                logger.debug(f"Submitted {len(futures)} tasks for page {page_number}. Waiting for their completion...")
                for i, future in enumerate(futures):
                    try:
                        future.result() 
                    except Exception as e: # Should not happen if process_hash_entry catches its own exceptions
                        logger.error(f"A submitted task (idx {i}, page {page_number}) unexpectedly failed: {e}", exc_info=True)
            
            logger.info(f"All submitted tasks for page {page_number} have concluded.")

            with download_count_lock: # Check global should_stop again under lock after page processing
                if should_stop: 
                    logger.info("Stop signal confirmed active after processing page. Terminating further page fetching.")
                    break # Break from the main 'while True' page fetching loop
            
            page_number += 1
            time.sleep(0.5)
        
        elif hashes_data_page is None: # fetch_hashes_page had an error or no data after retries
            logger.error(f"Failed to fetch page {page_number} or API feed exhausted. Stopping.")
            break
        # else: hashes_data_page is not None but 'data' key might be missing - handled by initial check.

    logger.info("Hash feed processing complete or stop condition met.")


# --- Main Execution ---
if __name__ == "__main__":
    run_start_time = time.time() 
    
    if not API_KEY or API_KEY == "PUT_YOUR_METADEFENDER_API_KEY_HERE":
        logger.critical("CRITICAL: API_KEY is not set or is the placeholder. Edit the script and provide your MetaDefender API key.")
        exit(1)
        
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    initial_processed_set = load_processed_hashes()
    processed_hashes.update(initial_processed_set)
    logger.info(f"Loaded {len(initial_processed_set)} unique hashes from '{PROCESSED_LOG_FILE}'. These will be skipped.")
    
    effective_max_dl_msg = str(MAX_DOWNLOAD) if MAX_DOWNLOAD is not None else "Unlimited (until feed ends)"
    logger.info(f"Commencing malware sample acquisition. Effective MAX_DOWNLOAD (successful acquisitions): {effective_max_dl_msg}.")
    
    if MAX_DOWNLOAD == 0:
        logger.info("MAX_DOWNLOAD is 0. No samples will be acquired as per configuration.")
        with download_count_lock: # Ensure thread-safe modification
            should_stop = True 
    
    # Check should_stop before starting fetch_and_process_hashes
    start_fetching = True
    with download_count_lock:
        if should_stop:
            start_fetching = False
            
    if start_fetching:
        fetch_and_process_hashes()
    else:
        logger.info("Skipping fetch_and_process_hashes because should_stop is already true.")

    # --- Debriefing: Print Comprehensive Statistics ---
    run_end_time = time.time()
    total_duration = run_end_time - run_start_time # Use run_start_time from main
    logger.info("Acquisition phase concluded. Generating final report.")

    report = f"\n--- Acquisition Report ({time.strftime('%Y-%m-%d %H:%M:%S')}) ---\n"
    report += f"Total Operation Duration: {total_duration:.2f} seconds\n"
    report += f"Configured MAX_DOWNLOAD (Successful Acquisitions): {effective_max_dl_msg}\n" # effective_max_dl_msg defined earlier
    report += f"Threads Used: {NUM_THREADS}\n\n"
    
    report += f"Hash Feed Activity:\n"
    report += f"  Total Hashes Seen from API (raw count): {total_hashes_seen}\n"
    report += f"  Skipped (already in '{PROCESSED_LOG_FILE}' at start): {skipped_processed_count_from_log}\n"
    report += f"  Skipped (already processed/attempted earlier in this session or duplicate on page): {skipped_processed_in_session}\n"
    report += f"  Skipped (file already existed locally during download attempt): {skipped_existing_count}\n\n"

    report += f"Download Outcome:\n"
    report += f"  Successfully Acquired (new files downloaded): {downloaded_count}\n"
    # The pending_downloads_count at the end should be 0 if all tasks completed.
    # For initiated attempts, it's tricky to give an exact sum here without more complex tracking.
    # The number of tasks for which pending_downloads_count was incremented is what we limited.
    report += f"  Download Attempts Approved (based on limit): Up to {MAX_DOWNLOAD if MAX_DOWNLOAD is not None else 'All eligible'} apx.\n"
    report += f"  Download Attempts Failed (after all retries): {failed_count}\n"

    if downloaded_count > 0:
        total_bytes_downloaded = sum(download_sizes.values())
        
        actual_download_time_sum = 0
        files_with_rates = 0
        total_bytes_for_avg_rate = 0

        for filename_key, rate_val in download_rates.items():
            if rate_val > 0 and filename_key in download_sizes:
                size_val = download_sizes[filename_key]
                if size_val > 0:
                    actual_download_time_sum += size_val / rate_val # time = size / rate
                    total_bytes_for_avg_rate += size_val
                    files_with_rates += 1
        
        report += f"  Total Payload Acquired: {total_bytes_downloaded} bytes ({total_bytes_downloaded / (1024*1024):.2f} MB)\n"
        if actual_download_time_sum > 0:
            effective_dl_rate = total_bytes_for_avg_rate / actual_download_time_sum
            report += f"  Effective Aggregate Download Rate (for {files_with_rates} successfully downloaded files with rate data): {effective_dl_rate:.2f} bytes/second\n"
        else: # Fallback if actual_download_time_sum is 0 (e.g. all downloads were instantaneous, unlikely)
            overall_download_rate_total_duration = total_bytes_downloaded / total_duration if total_duration > 0 else 0
            report += f"  Overall Acquisition Rate (based on total script duration): {overall_download_rate_total_duration:.2f} bytes/second\n"


        if download_rates: # This dictionary itself contains filename -> rate
            report += "\n  --- Sample of Individual Download Rates (first 5) ---\n"
            for i, (filename_key, rate_val) in enumerate(list(download_rates.items())):
                if i >= 5: break
                size_for_this_file = download_sizes.get(filename_key, 0)
                report += f"    {os.path.basename(filename_key)}: {rate_val:.2f} bytes/second ({size_for_this_file / (1024*1024):.2f} MB)\n"
            if len(download_rates) > 5: report += "    ... and more.\n"
    else:
        report += "  No new files were downloaded in this session.\n"

    if http_status_counts:
        report += "\n--- HTTP Status Code Summary ---\n"
        for code, count in sorted(http_status_counts.items()):
            report += f"  HTTP {code}: {count} times\n"

    if error_counts:
        report += "\n--- Error/Issue Summary (first 10 unique) ---\n" # Clarified it's first 10 unique
        sorted_errors = sorted(error_counts.items())
        for i, (error, count) in enumerate(sorted_errors):
            if i >= 10: break
            error_display = error[:120] + "..." if len(error) > 120 else error
            report += f"  {error_display}: {count} times\n"
        if len(sorted_errors) > 10: report += "  ... and more unique errors.\n" # Clarified "unique errors"
            
    report += "\n--- End of Report ---\n"
    
    logger.info("Final report data:\n" + report) # Logging the report is good

    report_filename = f"acquisition_report_{time.strftime('%Y%m%d_%H%M%S')}.txt"
    try:
        with open(report_filename, "w") as r_file:
            r_file.write(report)
        logger.info(f"Full report also saved to {report_filename}")
    except IOError as e:
        logger.error(f"Could not write report to file {report_filename}: {e}")