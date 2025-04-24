import requests
from requests.exceptions import RequestException
import os
import time
from urllib.parse import quote, urlparse, urlencode, unquote
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import hashlib
import threading

# --- Configuration ---
SEARCH_URL_TEMPLATE = "https://web.archive.org/__wb/search/waybacksearch"
OUTPUT_DIR = "output_pdfs_by_term_optimized_paged" # New output dir
PAGE_FETCH_DELAY = 1.5
PDF_DOWNLOAD_DELAY = 0.1
MAX_DOWNLOAD_WORKERS = 8
MAX_CRAWL_WORKERS = 4
RESULTS_PER_PAGE = 100
# --- New Constant: Maximum API pages to fetch per search term ---
MAX_API_PAGES_PER_TERM = 100 # e.g., Check first 100 pages (100*100 = 10k results)
# ------------------------------------------------------------------
REQUEST_TIMEOUT = 20
HEAD_REQUEST_TIMEOUT = 10
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

# Search Terms (Same as before)
SEARCH_TERMS = [
    "CV", "Resume", "\"Curriculum Vitae\"", "Application", "Cover letter",
    "Job application", "Employment history", "Thesis", "Dissertation",
    "Research paper", "Published article", "Scholarship application",
    "Recommendation letter", "Meeting minutes", "Project proposal", "Budget report",
    "Strategic plan", "Business plan", "Internal report", "Developer CV",
    "Security researcher", "Whitepaper", "Conference paper", "IEEE member",
    "Lebenslauf", "Currículo", "履歴書 filetype:pdf", "CV Europass",
    "\"Passport Scan\"", "\"Passport Copy\"", "\"Invoice Confidential\"",
    "\"Purchase Order\"", "\"Salary Slip\"", "\"Payslip\"", "\"Bank Statement\"",
    "\"Tax Return\"", "\"W-9 Form\"", "\"Form 1040\"", "\"Academic Transcript\"",
    "\"Performance Review\"", "\"Employee List\"", "\"Staff List\"",
    "\"Meeting Agenda\" \"Private\"", "\"Contract Agreement\"", "\"NDA\"",
    "\"Non-Disclosure Agreement\"", "\"Network Diagram\"", "\"Internal Memo\"",
    "\"For Internal Use Only\"", "scan.pdf", "Rechnung", "Factura",
    "Passeport", "Gehaltsabrechnung", "\"Patient Record\"",
    "\"User Credentials\"", "\"Password List\"", "\"Account Details\"",
]


# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# --- Helper Functions (sanitize_directory_name, check_url_head, download_pdf_to_path) ---
# (Keep these functions the same as the previous 'optimized' version)
def sanitize_directory_name(name: str) -> str:
    name = name.replace('"', '').replace("'", "")
    name = re.sub(r'[\\/*?:"<>| ]', '_', name)
    name = re.sub(r'_+', '_', name)
    name = name.strip('_')
    max_len = 100
    if len(name) > max_len:
        name = name[:max_len]
    if not name:
        name = "default_term"
    return name

def check_url_head(pdf_url: str, session: requests.Session) -> bool:
    try:
        head_response = session.head(pdf_url, timeout=HEAD_REQUEST_TIMEOUT, allow_redirects=True)
        if head_response.status_code == 200:
            content_type = head_response.headers.get('Content-Type', '').lower()
            if 'application/pdf' in content_type:
                logging.debug(f"HEAD check OK for {pdf_url[:60]}...")
                return True
            else:
                logging.debug(f"HEAD check failed (Content-Type: {content_type}): {pdf_url[:60]}...")
                return False
        elif 400 <= head_response.status_code < 500:
             logging.debug(f"HEAD check failed (Status: {head_response.status_code}): {pdf_url[:60]}...")
             return False
        else:
             logging.warning(f"HEAD check failed (Status: {head_response.status_code}): {pdf_url[:60]}...")
             return False
    except RequestException as e:
        logging.debug(f"HEAD request failed ({type(e).__name__}): {pdf_url[:60]}...")
        return False
    except Exception as e:
        logging.warning(f"Unexpected error during HEAD check for {pdf_url[:60]}: {e}")
        return False

def download_pdf_to_path(pdf_url: str, filepath: str, session: requests.Session):
    time.sleep(PDF_DOWNLOAD_DELAY)
    logging.info(f"Attempting GET download: {os.path.basename(filepath)}...")
    try:
        response = session.get(pdf_url, timeout=REQUEST_TIMEOUT, stream=True, allow_redirects=True)
        response.raise_for_status()
        content_type = response.headers.get('Content-Type', '').lower()
        if 'application/pdf' not in content_type:
             logging.warning(f"GET Content-Type mismatch ({content_type}) for {os.path.basename(filepath)}, skipping save.")
             response.close()
             return
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logging.info(f"[+] Downloaded: {os.path.basename(filepath)}")
    except requests.exceptions.Timeout:
        logging.warning(f"[!] Timeout during GET download for {os.path.basename(filepath)}")
    except requests.exceptions.TooManyRedirects:
        logging.warning(f"[!] Too many redirects for {os.path.basename(filepath)}")
    except requests.exceptions.RequestException as e:
        logging.error(f"[!] Request Error during GET for {os.path.basename(filepath)}: {e}")
    except IOError as e:
        logging.error(f"[!] File Error saving {os.path.basename(filepath)}: {e}")
    except Exception as e:
        logging.error(f"[!] Unexpected Error downloading {pdf_url} to {os.path.basename(filepath)}: {e}", exc_info=False)


def crawl_and_submit_downloads(term: str, session: requests.Session, download_executor: ThreadPoolExecutor):
    """Crawls API for a term up to MAX_API_PAGES_PER_TERM, performs HEAD checks, and submits valid PDF download tasks."""
    logging.info(f"Starting crawl for term: '{term}' (max pages: {MAX_API_PAGES_PER_TERM})")
    page = 1
    total_hits = -1
    links_submitted_for_term = 0
    links_skipped_head_check = 0
    processed_api_pages = 0

    sanitized_term = sanitize_directory_name(term)
    term_output_dir = os.path.join(OUTPUT_DIR, sanitized_term)
    try:
        os.makedirs(term_output_dir, exist_ok=True)
    except OSError as e:
        logging.error(f"Could not create directory {term_output_dir}: {e}")
        return

    while True:
        # *** Check page limit ***
        if page > MAX_API_PAGES_PER_TERM:
            logging.info(f"Reached max API page limit ({MAX_API_PAGES_PER_TERM}) for '{term}'. Stopping crawl for this term.")
            break
        # ************************

        params = {
            'q': term, 'size': RESULTS_PER_PAGE, 'page': page,
            'filetype': 'pdf', 'collection': 'pdf'
        }
        search_url = f"{SEARCH_URL_TEMPLATE}?{urlencode(params, quote_via=quote)}"
        logging.debug(f"Querying API page {page}/{MAX_API_PAGES_PER_TERM} for '{term}'")

        try:
            response = session.get(search_url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            processed_api_pages += 1
            data = response.json()

            hits = data.get('hits', [])
            if total_hits == -1:
                 total_hits = data.get('total', 0)
                 logging.info(f"API reported ~{total_hits} total hits for '{term}'.")

            if not hits:
                logging.info(f"No more API results for '{term}' on page {page} (before limit).")
                break

            for hit in hits:
                if hit.get('content_type') == 'application/pdf':
                    pdf_url = hit.get('url')
                    if pdf_url:
                        try:
                            url_hash = hashlib.sha256(pdf_url.encode('utf-8')).hexdigest()
                            filename = f"{url_hash}.pdf"
                            filepath = os.path.join(term_output_dir, filename)
                            if not os.path.exists(filepath):
                                if check_url_head(pdf_url, session):
                                    download_executor.submit(download_pdf_to_path, pdf_url, filepath, session)
                                    links_submitted_for_term += 1
                                else:
                                    links_skipped_head_check += 1
                        except Exception as e:
                            logging.error(f"Error processing hit/submitting download for {pdf_url}: {e}")

            # Removed check based on total_hits as we now rely on MAX_API_PAGES_PER_TERM
            # if total_hits > 0 and (page * RESULTS_PER_PAGE >= total_hits):
            #      logging.info(f"Reached estimated end for '{term}' based on total hits.")
            #      break

            page += 1
            if page <= MAX_API_PAGES_PER_TERM and page % 20 == 0: # Log progress every 20 pages
                logging.info(f"Processed {page-1}/{MAX_API_PAGES_PER_TERM} API pages for '{term}'...")
            time.sleep(PAGE_FETCH_DELAY)

        except json.JSONDecodeError:
            logging.error(f"Failed to decode JSON response for '{term}' page {page}")
            time.sleep(PAGE_FETCH_DELAY * 2)
            continue
        except RequestException as e:
            logging.warning(f"API request failed for '{term}' page {page}: {e}")
            break
        except Exception as e:
            logging.error(f"Unexpected error crawling '{term}' page {page}: {e}", exc_info=True)
            break

    logging.info(f"Finished crawl for '{term}'. Submitted {links_submitted_for_term} downloads, skipped {links_skipped_head_check} after HEAD check. Processed {processed_api_pages} API pages (limit was {MAX_API_PAGES_PER_TERM}).")


# --- Main Execution ---
# (Keep the main function the same as the previous 'optimized' version)
def main():
    start_time = time.time()
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logging.info(f"Base output directory: {OUTPUT_DIR}")
    logging.info(f"Max Crawl Workers: {MAX_CRAWL_WORKERS}, Max Download Workers: {MAX_DOWNLOAD_WORKERS}")
    logging.info(f"Results Per Page: {RESULTS_PER_PAGE}, API Page Limit Per Term: {MAX_API_PAGES_PER_TERM}, API Page Delay: {PAGE_FETCH_DELAY}s")

    with requests.Session() as session:
        session.headers.update({'User-Agent': USER_AGENT})

        with ThreadPoolExecutor(max_workers=MAX_CRAWL_WORKERS, thread_name_prefix='Crawl_') as crawl_executor, \
             ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS, thread_name_prefix='Download_') as download_executor:

            crawl_futures = [crawl_executor.submit(crawl_and_submit_downloads, term, session, download_executor)
                             for term in SEARCH_TERMS]

            logging.info("All crawl tasks submitted. Waiting for crawlers to finish...")
            for future in as_completed(crawl_futures):
                try:
                    future.result()
                except Exception as exc:
                    logging.error(f"A crawl task generated an exception: {exc}", exc_info=True)

            logging.info("All crawl tasks finished. Waiting for pending downloads to complete...")

    end_time = time.time()
    logging.info(f"\n--- All tasks completed in {time.strftime('%H:%M:%S', time.gmtime(end_time - start_time))}. ---")


if __name__ == "__main__":
    main()