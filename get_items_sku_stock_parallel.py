import asyncio
import hashlib
import httpx
import traceback
import xml.etree.ElementTree as ET
from dropbox_uploader import upload_file_to_dropbox # Now takes 2 args
import os
from dotenv import load_dotenv
import datetime # Added for date calculation
import tempfile # Moved import higher

load_dotenv()

# --- Configuration ---
ACCOUNT_ID = os.getenv("ACCOUNT_ID")
AUTH_KEY = os.getenv("AUTH_KEY")
CONDITION = "1"

BASE_URL_ITEMS = "https://crossmall.jp/webapi2/get_item"
BASE_URL_SKU = "https://crossmall.jp/webapi2/get_item_sku"
BASE_URL_STOCK = "https://crossmall.jp/webapi2/get_stock"
# DROPBOX_PATH = "/Reports/stock/stock.xml" # Removed: Path will be generated dynamically

# JST timezone
JST = datetime.timezone(datetime.timedelta(hours=9))

# --- Utility functions ---
def generate_signature(query_string, auth_key):
    raw_string = f"{query_string}{auth_key}"
    return hashlib.md5(raw_string.encode()).hexdigest()

def fetch_all_items():
    item_code = ""
    items = []
    total_items = 0
    while True:
        query_string = f"account={ACCOUNT_ID}&condition={CONDITION}"
        if item_code:
            query_string += f"&item_code={item_code}"
        signature = generate_signature(query_string, AUTH_KEY)
        api_url = f"{BASE_URL_ITEMS}?{query_string}&signing={signature}"
        print(f"Fetching items with URL: {api_url}")
        # Consider adding retries/error handling like in the orders script
        response = httpx.get(api_url)
        if response.status_code != 200:
            print("Error fetching items:", response.status_code, response.text)
            break
        try:
            root = ET.fromstring(response.text)
        except ET.ParseError as e:
            print("Error parsing items response:", e)
            break
        results = root.findall(".//Result")
        if not results:
            print("No more items found.")
            break
        for result in results:
            item_elem = result
            item_code_elem = item_elem.find("item_code")
            if item_code_elem is not None and item_code_elem.text:
                # Use the last found item_code for pagination
                item_code = item_code_elem.text.strip()
            items.append(item_elem)
        total_items += len(results)
        print(f"Retrieved {len(results)} items. Last item_code for next page (if any): {item_code}")
        if len(results) < 100:
            print("Less than 100 items returned. Stopping retrieval.")
            break
    print(f"Total items retrieved: {total_items}")
    return items

async def fetch_sku_for_item(item_code, client, semaphore):
    query_string = f"account={ACCOUNT_ID}&item_code={item_code}"
    signature = generate_signature(query_string, AUTH_KEY)
    api_url = f"{BASE_URL_SKU}?{query_string}&signing={signature}"
    async with semaphore:
        # print(f"[DEBUG] Fetching SKU for item_code: {item_code}") # Can be noisy
        try:
            # Add timeout like in orders script
            response = await client.get(api_url, timeout=60.0)
            if response.status_code == 200:
                return response.text
            else:
                print(f"[ERROR] Failed SKU for {item_code}: Status code {response.status_code} - {response.text}")
                return None
        except Exception as e:
            print(f"[EXCEPTION] Exception fetching SKU for {item_code}: {repr(e)}")
            # traceback.print_exc() # Optional: Can be very verbose
            return None

async def fetch_stock_for_sku(sku_code, client, semaphore):
    """
    Asynchronously fetch stock data for a given sku_code.
    """
    query_string = f"account={ACCOUNT_ID}&sku_code={sku_code}"
    signature = generate_signature(query_string, AUTH_KEY)
    api_url = f"{BASE_URL_STOCK}?{query_string}&signing={signature}"
    async with semaphore:
        # print(f"[DEBUG] Fetching stock for sku_code: {sku_code}") # Can be noisy
        try:
            # Add timeout
            response = await client.get(api_url, timeout=60.0)
            if response.status_code == 200:
                return response.text
            else:
                print(f"[ERROR] Failed stock for {sku_code}: Status code {response.status_code} - {response.text}")
                return None
        except Exception as e:
            print(f"[EXCEPTION] Exception fetching stock for {sku_code}: {repr(e)}")
            # traceback.print_exc() # Optional
            return None

async def process_item(item_elem, client, sku_semaphore, stock_semaphore):
    item_code_elem = item_elem.find("item_code")
    if item_code_elem is None or not item_code_elem.text:
        # print("Item has no item_code, skipping SKU/Stock fetch.") # Can be noisy
        return item_elem
    item_code = item_code_elem.text.strip()
    sku_response_text = await fetch_sku_for_item(item_code, client, sku_semaphore)
    if sku_response_text is None:
        # Error logged in fetch_sku_for_item
        # print(f"No SKU response for item_code: {item_code}")
        return item_elem
    try:
        sku_root = ET.fromstring(sku_response_text)
    except ET.ParseError as e:
        print(f"Error parsing SKU response for item_code {item_code}: {e}")
        return item_elem

    skus_container = ET.Element("Skus")
    sku_results = sku_root.findall(".//Result")
    stock_tasks = []
    sku_elements = []

    # Collect all SKU elements and prepare stock fetch tasks
    for sku_result in sku_results:
        sku_elem = ET.Element("Sku")
        # Copy all children from the Result element to the Sku element
        for child in sku_result:
            sku_elem.append(child) # No need for deepcopy here unless modifying later

        sku_code = sku_result.findtext("item_sku_code")
        if sku_code and sku_code.strip():
            sku_code = sku_code.strip()
            # Schedule stock fetch only if SKU code exists
            stock_tasks.append(fetch_stock_for_sku(sku_code, client, stock_semaphore))
        else:
            # Add a placeholder None task if no SKU code, keeps lists aligned
            stock_tasks.append(asyncio.sleep(0, result=None)) # Coroutine that immediately returns None
        sku_elements.append(sku_elem) # Keep reference to the <Sku> element

    # Fetch stock data concurrently
    if stock_tasks:
        # Use return_exceptions=True to prevent one failure from stopping others
        stock_responses = await asyncio.gather(*stock_tasks, return_exceptions=True)

        # Process stock responses and attach to corresponding SKU elements
        for i, stock_response_or_exc in enumerate(stock_responses):
            sku_elem = sku_elements[i] # Get the corresponding <Sku> element

            if isinstance(stock_response_or_exc, Exception):
                sku_code_for_log = sku_elem.findtext('item_sku_code', 'N/A')
                print(f"[ERROR] Exception fetching stock for SKU {sku_code_for_log}: {stock_response_or_exc}")
                continue # Skip attaching stock data for this SKU

            stock_response_text = stock_response_or_exc
            if stock_response_text:
                try:
                    stock_root = ET.fromstring(stock_response_text)
                except ET.ParseError as e:
                    sku_code_for_log = sku_elem.findtext('item_sku_code', 'N/A')
                    print(f"Error parsing stock response for sku_code {sku_code_for_log}: {e}")
                    continue # Skip attaching stock data

                stock_container = ET.Element("Stock")
                stock_results = stock_root.findall(".//Result")
                if stock_results: # Only add <Stock> if there are results
                    for stock_result in stock_results:
                         # Append the <Result> element itself directly into <Stock>
                        stock_container.append(stock_result)
                    sku_elem.append(stock_container)
                # else: No stock results found for this SKU, don't add empty <Stock> tag

    # Append all processed SKU elements (with or without Stock) to the Skus container
    for sku_elem in sku_elements:
        skus_container.append(sku_elem)

    # Append the Skus container to the original item element
    item_elem.append(skus_container)
    return item_elem


async def process_all_items(items):
    # Adjust semaphore limits based on API limits and testing
    sku_semaphore = asyncio.Semaphore(15) # Concurrency for SKU fetches
    stock_semaphore = asyncio.Semaphore(15) # Concurrency for Stock fetches
    # Use higher client limits if needed, check API documentation/behavior
    limits = httpx.Limits(max_connections=100, max_keepalive_connections=20)
    async with httpx.AsyncClient(limits=limits, timeout=60.0) as client: # Use a single client
        tasks = [process_item(item, client, sku_semaphore, stock_semaphore) for item in items]
        print(f"Processing details for {len(tasks)} items concurrently...")
        # Use return_exceptions=True to handle individual item processing errors
        results = await asyncio.gather(*tasks, return_exceptions=True)
        print("Finished processing details.")

    # Filter out exceptions and None results if needed, or handle them
    processed_items = []
    errors = 0
    for i, res in enumerate(results):
        if isinstance(res, Exception):
            item_code_for_log = items[i].findtext('item_code', f'index {i}')
            print(f"[ERROR] Unhandled exception processing item {item_code_for_log}: {res}")
            errors += 1
        elif res is not None: # Should be an ET.Element
            processed_items.append(res)
        # else: Item processing returned None, potentially skipped or failed earlier

    if errors > 0:
        print(f"[WARNING] Encountered errors during processing of {errors} items.")

    return processed_items


def main():
    # --- Date Calculation (Today in JST) ---
    now_jst = datetime.datetime.now(JST)
    date_str = now_jst.strftime("%Y%m%d")

    # --- Filename Generation ---
    # Local temporary file name (doesn't need date) or persistent local file name
    temp_local_filename = "fetched_items_sku_stock_temp.xml"
    persistent_local_filename = f"stock_{date_str}.xml" # Optional: if WRITE_XML=true

    # Dropbox destination path
    dropbox_folder = "/Reports/stock" # Or read from env var
    dropbox_filename = f"stock_{date_str}.xml"
    dropbox_destination_path = f"{dropbox_folder}/{dropbox_filename}"
    print(f"Target Dropbox path: {dropbox_destination_path}")

    # --- Data Fetching and Processing ---
    print("Fetching base items...")
    items = fetch_all_items()
    if not items:
        print("No base items found. Exiting.")
        return

    print(f"Processing SKU and Stock details for {len(items)} items...")
    processed_items_list = asyncio.run(process_all_items(items))

    if not processed_items_list:
        print("No items were successfully processed. Exiting.")
        return

    # --- XML Generation ---
    print("Generating final XML tree...")
    root = ET.Element("Items")
    for item in processed_items_list:
        root.append(item)
    tree = ET.ElementTree(root)

    # --- File Handling and Upload ---
    upload_successful = False
    if os.getenv("WRITE_XML", "false").lower() == "true":
        # Write XML to a persistent local file and upload it.
        local_file_to_upload = persistent_local_filename
        try:
            print(f"Saving persistent local file to: {local_file_to_upload}")
            tree.write(local_file_to_upload, encoding="utf-8", xml_declaration=True)
            print(f"Uploading {local_file_to_upload} to {dropbox_destination_path}...")
            upload_file_to_dropbox(local_file_to_upload, dropbox_destination_path)
            upload_successful = True
            print("Persistent local file saved and uploaded.")
        except IOError as e:
            print(f"[ERROR] Failed to write persistent local file {local_file_to_upload}: {e}")
        except Exception as e:
            print(f"[ERROR] Failed to upload persistent file {local_file_to_upload}: {e}")
            traceback.print_exc() # Show more detail on upload error
    else:
        # Write XML to a temporary file, upload it, and then remove the file.
        local_file_to_upload = temp_local_filename # Use the generic temp name
        temp_file_path = None
        try:
            # Use NamedTemporaryFile for automatic cleanup on error if possible
            # Or manually manage as before
            with tempfile.NamedTemporaryFile(delete=False, suffix=".xml", mode='wb') as tmp:
                 tree.write(tmp, encoding="utf-8", xml_declaration=True)
                 temp_file_path = tmp.name
            print(f"Temporary file created at {temp_file_path} for upload.")
            print(f"Uploading {temp_file_path} to {dropbox_destination_path}...")
            upload_file_to_dropbox(temp_file_path, dropbox_destination_path)
            upload_successful = True
            print("Temporary file uploaded.")
        except IOError as e:
            print(f"[ERROR] Failed to write temporary file: {e}")
        except Exception as e:
            print(f"[ERROR] Failed to upload temporary file {temp_file_path}: {e}")
            traceback.print_exc()
        finally:
            # Clean up the temporary file if it exists
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                    print(f"Temporary file {temp_file_path} removed.")
                except OSError as e:
                    print(f"[ERROR] Failed to remove temporary file {temp_file_path}: {e}")

    if upload_successful:
        print("--- Stock data processing and upload completed successfully. ---")
    else:
        print("--- Stock data processing completed with upload errors. ---")


if __name__ == "__main__":
    if not ACCOUNT_ID or not AUTH_KEY:
        print("[ERROR] ACCOUNT_ID or AUTH_KEY environment variables not set. Exiting.")
    else:
        main()