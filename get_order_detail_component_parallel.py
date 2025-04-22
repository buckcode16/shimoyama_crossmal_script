import copy
import asyncio
import hashlib
import httpx
import xml.etree.ElementTree as ET
import os
import tempfile
from dropbox_uploader import upload_file_to_dropbox # Takes 2 args now
from dotenv import load_dotenv
import datetime
import traceback

load_dotenv()

# API Credentials and Endpoints
ACCOUNT_ID = os.getenv("ACCOUNT_ID")
AUTH_KEY = os.getenv("AUTH_KEY")
BASE_URLS = {
    "order": "https://crossmall.jp/webapi2/get_order",
    "order_detail": "https://crossmall.jp/webapi2/get_order_detail",
    "order_component": "https://crossmall.jp/webapi2/get_order_component"
}

# JST timezone
JST = datetime.timezone(datetime.timedelta(hours=9))

# Utility: Generate MD5 signature
def generate_signature(query_string, auth_key):
    raw_string = f"{query_string}{auth_key}"
    return hashlib.md5(raw_string.encode()).hexdigest()

# Asynchronous make_request function with retry logic (no changes needed)
async def async_make_request(endpoint, params, client, max_retries=3, delay=2.0):
    query_parts = []
    for key, value in params.items():
        if value is not None:
            if isinstance(value, list):
                for item in value:
                    query_parts.append(f"{key}={item}")
            else:
                query_parts.append(f"{key}={value}")
    query_string = "&".join(sorted(query_parts)) # Sort for consistent signature

    signature = generate_signature(query_string, AUTH_KEY)
    api_url = f"{BASE_URLS[endpoint]}?{query_string}&signing={signature}"
    attempts = 0
    log_context = ""
    if 'order_date_fr' in params and params['order_date_fr'] == params.get('order_date_to'):
         log_context = f"for order_date {params['order_date_fr']}"
    elif 'order_number' in params:
         log_context = f"for order {params.get('order_number')} line {params.get('line_no', 'N/A')}"

    while attempts < max_retries:
        try:
            response = await client.get(api_url, timeout=60.0) # Standard timeout
            response.raise_for_status() # Raises HTTPStatusError for 4xx/5xx
            return response.text
        except httpx.HTTPStatusError as e:
            # Log specific HTTP errors
            print(f"HTTP Error in {endpoint} {log_context} (attempt {attempts+1}/{max_retries}): Status {e.response.status_code} - Response: {e.response.text[:200]}...")
            # Optional: Add specific handling for rate limits (e.g., 429) or server errors (5xx)
            if e.response.status_code == 429 or e.response.status_code >= 500:
                 print("Server error or rate limit hit, retrying...")
            else:
                 print("Client error, likely won't succeed on retry.")
                 return None # Don't retry client errors like 400, 401, 403, 404
        except httpx.RequestError as e:
            # Network errors, timeouts, etc.
            print(f"Request Error in {endpoint} {log_context} (attempt {attempts+1}/{max_retries}): {e}")
        except Exception as e:
            # Catch other potential exceptions during the request
            print(f"General Exception during request in {endpoint} {log_context} (attempt {attempts+1}/{max_retries}): {e}")
            # traceback.print_exc() # Uncomment for detailed stack trace during debugging

        attempts += 1
        if attempts < max_retries:
            current_delay = delay * (2 ** (attempts - 1)) # Exponential backoff
            print(f"Retrying in {current_delay:.2f} seconds...")
            await asyncio.sleep(current_delay)
        else:
            print(f"Max retries ({max_retries}) reached for {endpoint} with parameters: {params}")
            return None # Failed after all retries

# Asynchronous function to fetch base order data sequentially for a SINGLE specific order date
async def async_fetch_base_orders_for_day(client, order_date):
    """Fetches base order data filtered for a single order_date."""
    all_orders_data = {}
    last_order_number = "" # For pagination
    page_count = 0
    total_fetched_this_date = 0

    print(f"[INFO] Date: {order_date} - Starting base order fetch...")

    while True:
        page_count += 1
        params = {
            "account": ACCOUNT_ID,
            "condition": "1",
            "creating_source_type[]": ["99"], # Filter for source type 99
            "order_date_fr": order_date, # Filter for the specific date
            "order_date_to": order_date
        }
        # Add order_number for pagination on subsequent requests
        if last_order_number:
            params["order_number"] = last_order_number

        # print(f"  DEBUG: Fetching page {page_count} with params: {params}") # Debug log
        response_text = await async_make_request("order", params, client)

        if not response_text:
            print(f"[ERROR] Date: {order_date} - Failed to fetch orders page {page_count} after retries.")
            # Decide if this should halt processing for the date or just log
            break # Stop fetching for this date if a page fails

        try:
            root = ET.fromstring(response_text)
        except ET.ParseError as e:
            print(f"[ERROR] Date: {order_date} - Failed to parse XML page {page_count}: {e}")
            print(f"  Response text snippet: {response_text[:500]}...")
            break # Stop fetching for this date if parsing fails

        results = root.findall(".//Result")
        current_page_count = len(results)
        total_fetched_this_date += current_page_count

        if not results:
            if page_count == 1:
                 print(f"[INFO] Date: {order_date} - No orders found matching criteria.")
            # else: # Normal end of pagination, no need to log verbosely unless debugging
                 # print(f"  Date: {order_date} - Page {page_count}: Found 0 orders (End of data).")
            break # Exit loop, no more data
        else:
            print(f"  Date: {order_date} - Page {page_count}: Found {current_page_count} orders...")

        new_last_order_number_on_page = None
        for res in results:
            order_number_el = res.find("order_number")
            if order_number_el is not None and order_number_el.text:
                order_number = order_number_el.text.strip()
                # Use deepcopy ONLY if modifying base_order_element later, otherwise shallow is fine
                all_orders_data[order_number] = copy.deepcopy(res) # Store the ET.Element
                new_last_order_number_on_page = order_number # Track the last valid order number on the page
            else:
                print(f"[WARNING] Date: {order_date} - Found a Result without an order_number on page {page_count}.")

        # Pagination logic: Stop if fewer than 100 results OR if no new last order number found
        if current_page_count < 100:
            # print(f"  Date: {order_date} - Page {page_count}: Found {current_page_count} orders (< 100). Assuming end of data.")
            break
        elif new_last_order_number_on_page:
             last_order_number = new_last_order_number_on_page
             # print(f"  DEBUG: Setting last_order_number for next page to {last_order_number}") # Debug log
        else:
             print(f"[WARNING] Date: {order_date} - Page {page_count} had results but no valid last order_number found. Stopping pagination.")
             break

    print(f"[INFO] Date: {order_date} - Finished base order fetch. Total found: {total_fetched_this_date} orders ({len(all_orders_data)} unique order numbers).")
    return all_orders_data


# Asynchronous function to fetch order detail
async def async_get_order_detail(order_number, client, semaphore):
    params = {"account": ACCOUNT_ID, "order_number": order_number}
    async with semaphore:
        # print(f"DEBUG: Fetching detail for {order_number}")
        detail_xml = await async_make_request("order_detail", params, client)
        if not detail_xml:
            # Error logged in async_make_request
            return None
        try:
            return ET.fromstring(detail_xml)
        except ET.ParseError as e:
            print(f"[ERROR] Order {order_number}: Failed to parse order_detail XML: {e}")
            return None

# Asynchronous function to fetch order component
async def async_get_order_component(order_number, line_no, client, semaphore):
    params = {"account": ACCOUNT_ID, "order_number": order_number, "line_no": line_no}
    async with semaphore:
        # print(f"DEBUG: Fetching component for {order_number} line {line_no}")
        comp_xml = await async_make_request("order_component", params, client)
        if not comp_xml:
             # Error logged in async_make_request
            return None
        try:
            return ET.fromstring(comp_xml)
        except ET.ParseError as e:
            print(f"[ERROR] Order {order_number}, Line {line_no}: Failed to parse order_component XML: {e}")
            return None

# Asynchronous function to process a single order
async def process_order(order_number, base_order_element, client, semaphore):
    """Builds the complete XML structure for a single order."""
    order_elem = ET.Element("Order", attrib={"order_number": order_number})
    order_header_elem = ET.Element("OrderHeader")
    # Copy base fields directly into header - consider if deepcopy is needed
    for child in base_order_element:
        order_header_elem.append(copy.deepcopy(child)) # Keep deepcopy for safety if base might be reused
    order_elem.append(order_header_elem)

    order_details_container = ET.Element("OrderDetails")
    component_tasks_to_run = [] # Stores tuples: (task_coroutine, order_detail_element_ref, line_no)

    # Fetch Order Detail data first
    detail_root = await async_get_order_detail(order_number, client, semaphore)

    if detail_root is not None:
        detail_results = detail_root.findall(".//Result")
        if detail_results:
            for detail_result in detail_results:
                order_detail_elem = ET.Element("OrderDetail")
                line_no = None
                has_jan_cd = False
                # Copy detail fields into <OrderDetail>
                for child in detail_result:
                    order_detail_elem.append(copy.deepcopy(child)) # Deepcopy for safety
                    # Check conditions for fetching component
                    if child.tag == "jan_cd" and child.text and child.text.strip():
                        has_jan_cd = True
                    if child.tag == "line_no" and child.text:
                        line_no = child.text.strip()

                # Schedule component fetch if needed for this detail line
                if not has_jan_cd and line_no:
                    # print(f"DEBUG: Order {order_number}, Line {line_no}: Scheduling component fetch (no jan_cd).")
                    comp_coro = async_get_order_component(order_number, line_no, client, semaphore)
                    component_tasks_to_run.append((comp_coro, order_detail_elem, line_no)) # Store coro, target element, and line_no

                # Append the <OrderDetail> element now, components will be added later if fetched
                order_details_container.append(order_detail_elem)
        else:
            print(f"[WARNING] Order {order_number}: Detail fetched but no <Result> elements found.")
    # else: Error logged in async_get_order_detail

    # Fetch and process Order Components concurrently if any were scheduled
    if component_tasks_to_run:
        # print(f"DEBUG: Order {order_number}: Gathering {len(component_tasks_to_run)} component results...")
        component_coroutines = [task[0] for task in component_tasks_to_run]
        target_detail_elements = [task[1] for task in component_tasks_to_run]
        target_line_nos = [task[2] for task in component_tasks_to_run] # For logging

        component_results_roots = await asyncio.gather(*component_coroutines, return_exceptions=True)

        for i, result_or_exc in enumerate(component_results_roots):
            target_detail_elem = target_detail_elements[i]
            line_no_for_log = target_line_nos[i]

            if isinstance(result_or_exc, Exception):
                print(f"[ERROR] Order {order_number}, Line {line_no_for_log}: Exception gathering component result: {result_or_exc}")
                continue # Skip processing component for this line

            comp_root = result_or_exc
            if comp_root is not None:
                component_results = comp_root.findall(".//Result")
                if component_results:
                    components_container = ET.Element("Components")
                    components_added = 0
                    for comp_result in component_results:
                         # Maybe add check: is component data meaningful?
                        component_node = ET.Element("Component")
                        for comp_child in comp_result:
                            component_node.append(copy.deepcopy(comp_child)) # Deepcopy for safety
                        components_container.append(component_node)
                        components_added += 1

                    if components_added > 0: # Only add <Components> if it contains <Component> nodes
                        target_detail_elem.append(components_container)
                        # print(f"DEBUG: Order {order_number}, Line {line_no_for_log}: Added {components_added} components.")
                # else: print(f"DEBUG: Order {order_number}, Line {line_no_for_log}: Component fetched but no <Result> found.")
            # else: Error logged in async_get_order_component

    # Append the <OrderDetails> container (potentially with nested <Components>)
    if len(order_details_container): # Only add if there's at least one <OrderDetail>
        order_elem.append(order_details_container)
    elif detail_root is None: # If detail fetch failed entirely
         print(f"[WARNING] Order {order_number}: No details could be fetched or added.")
         # Decide if an empty order node should still be returned or None
         # return None # Or return the order_elem with just the header

    return order_elem


# Asynchronously build the complete order tree for a given set of base orders
async def build_complete_order_tree(orders_data, client, order_date):
    """Builds the final XML tree containing integrated order data for a single day."""
    orders_root = ET.Element("Orders")
    # Adjust semaphore based on testing and API limits for detail/component calls
    semaphore = asyncio.Semaphore(15) # Concurrency limit for detail/component fetches
    # Use higher client limits if many concurrent requests are made
    limits = httpx.Limits(max_connections=100, max_keepalive_connections=20)
    # Create a new client or pass the existing one; ensure it's suitable for high concurrency
    # Using the passed 'client' assumes it was created with appropriate limits

    order_processing_tasks = [
        process_order(order_num, base_element, client, semaphore)
        for order_num, base_element in orders_data.items()
    ]
    if not order_processing_tasks:
         print(f"[INFO] Date: {order_date} - No orders to process for details/components.")
         return orders_root, 0 # Return empty root and 0 count

    print(f"[INFO] Date: {order_date} - Starting detail/component processing for {len(order_processing_tasks)} orders...")
    order_elements_results = await asyncio.gather(*order_processing_tasks, return_exceptions=True)
    print(f"[INFO] Date: {order_date} - Finished detail/component processing.")

    processed_count = 0
    failed_order_processing_count = 0
    for i, result_or_exc in enumerate(order_elements_results):
        # Get the order number corresponding to this result
        order_num = list(orders_data.keys())[i]
        if isinstance(result_or_exc, Exception):
            print(f"[ERROR] Date: {order_date}, Order {order_num}: Unhandled exception during process_order call: {result_or_exc}")
            # traceback.print_exc() # Uncomment for full trace
            failed_order_processing_count += 1
            continue
        elif result_or_exc is None:
             print(f"[WARNING] Date: {order_date}, Order {order_num}: Processing returned None (likely detail fetch failure).")
             failed_order_processing_count += 1 # Count as failed if no element produced
             continue


        order_elem = result_or_exc # Should be an ET.Element("Order")
        if order_elem is not None and len(order_elem.findall(".//OrderDetail")) > 0: # Check if it has details
            orders_root.append(order_elem)
            processed_count += 1
        elif order_elem is not None:
             # Order processed, but maybe no details were found or added
             print(f"[INFO] Date: {order_date}, Order {order_num}: Processed but resulted in an order element with no OrderDetail children. Skipping addition to final tree.")
             # Consider if these should still be counted or logged differently
             failed_order_processing_count +=1 # Count as 'failed' in terms of useful output

    print(f"[INFO] Date: {order_date} - Added {processed_count} orders with details to the XML tree.")
    if failed_order_processing_count > 0:
         print(f"[WARNING] Date: {order_date} - Failed or skipped detail/component processing for {failed_order_processing_count} orders.")

    return orders_root, processed_count


# Function to process a single order date completely
async def process_yesterday_order_date(order_date_str):
    """Fetches, processes, and saves/uploads order data for a single order date."""
    print(f"--- Starting processing for Order Date: {order_date_str} ---")
    try:
        date_obj = datetime.datetime.strptime(order_date_str, '%Y-%m-%d').date()
        date_nodash = date_obj.strftime('%Y%m%d')
    except ValueError:
        print(f"[ERROR] Invalid date format received: {order_date_str}. Expected YYYY-MM-DD.")
        return f"ERROR_INVALID_DATE_FORMAT:{order_date_str}"

    # --- Filename Generation (Using YYYYMMDD format from the processed date) ---
    output_filename_base = f"orders_{date_nodash}"
    persistent_local_filename = f"{output_filename_base}.xml"
    dropbox_folder = os.getenv("DROPBOX_ORDER_FOLDER", "/Reports/order") # Allow override via env var
    dropbox_target_path = f"{dropbox_folder}/{output_filename_base}.xml"

    orders_data_dict = {}
    orders_tree = None
    processed_count = 0
    final_xml_bytes = None

    # --- Client Setup ---
    # Configure client with appropriate limits for potentially high concurrency
    limits = httpx.Limits(max_connections=100, max_keepalive_connections=20)
    timeout = httpx.Timeout(60.0, connect=10.0) # General timeout, shorter connect timeout

    try:
        async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
            # Step 1: Fetch base order data
            orders_data_dict = await async_fetch_base_orders_for_day(client, order_date=order_date_str)

            if not orders_data_dict:
                # Logged "No orders found" inside fetch function if that was the case
                print(f"[INFO] Date: {order_date_str} - No base orders matched criteria. Processing complete for this date.")
                return f"SUCCESS_NO_ORDERS:{order_date_str}"

            # Step 2: Process orders (fetch details/components)
            orders_tree, processed_count = await build_complete_order_tree(orders_data_dict, client, order_date_str)

            if processed_count == 0 and len(orders_data_dict) > 0:
                 # Warnings/errors logged inside build_complete_order_tree
                 print(f"[INFO] Date: {order_date_str} - Base orders found, but none yielded details/components for the final XML tree.")
                 return f"SUCCESS_NO_DATA_PROCESSED:{order_date_str}" # Indicate success, but no useful data generated

            if orders_tree is None: # Should not happen if build_complete_order_tree returns root always
                 print(f"[ERROR] Date: {order_date_str} - Final order tree is None. This should not happen.")
                 return f"ERROR_NULL_TREE:{order_date_str}"

        # Step 3: Serialize XML (only if orders were processed successfully)
        if orders_tree is not None and processed_count > 0:
            try:
                # Ensure the root element has children before trying to stringify
                if len(orders_tree):
                    final_xml_bytes = ET.tostring(orders_tree, encoding="utf-8", method='xml', xml_declaration=True)
                    print(f"[INFO] Date: {order_date_str} - Successfully generated final XML bytes ({len(final_xml_bytes)} bytes).")
                else:
                    print(f"[INFO] Date: {order_date_str} - XML tree was generated but contained no processed orders. No file will be created/uploaded.")
                    # Treat this like SUCCESS_NO_DATA_PROCESSED?
                    return f"SUCCESS_NO_DATA_IN_TREE:{order_date_str}"

            except Exception as e:
                print(f"[ERROR] Date: {order_date_str} - Failed to serialize final XML tree: {e}")
                traceback.print_exc()
                return f"ERROR_SERIALIZE:{order_date_str}"
        # else: Handled by processed_count check and returns above

        # Step 4: Write to file and upload (only if XML bytes exist)
        if final_xml_bytes:
            write_xml_flag = os.getenv("WRITE_XML", "true").lower() == "true"
            upload_successful = False
            local_path_used_for_upload = None

            if write_xml_flag:
                local_path_used_for_upload = persistent_local_filename
                try:
                    print(f"[INFO] Date: {order_date_str} - Writing XML to persistent file: {local_path_used_for_upload}")
                    with open(local_path_used_for_upload, "wb") as f:
                        f.write(final_xml_bytes)
                    print(f"[INFO] Date: {order_date_str} - Uploading {local_path_used_for_upload} to Dropbox: {dropbox_target_path}")
                    # Call the updated uploader (takes 2 args)
                    upload_file_to_dropbox(local_path_used_for_upload, dropbox_target_path)
                    upload_successful = True
                    print(f"[INFO] Date: {order_date_str} - Persistent file saved and uploaded.")
                except IOError as e:
                    print(f"[ERROR] Date: {order_date_str} - Failed to write XML to {local_path_used_for_upload}: {e}")
                    return f"ERROR_WRITE_LOCAL:{order_date_str}"
                except Exception as e: # Catch potential upload errors
                    print(f"[ERROR] Date: {order_date_str} - Failed during Dropbox upload for {local_path_used_for_upload}: {e}")
                    traceback.print_exc() # More detail on upload failure
                    return f"ERROR_UPLOAD:{order_date_str}"
            else:
                # Use temporary file for upload
                temp_file_path = None
                try:
                    # Safely create and write to a temporary file
                    with tempfile.NamedTemporaryFile(delete=False, suffix=".xml", mode='wb', prefix=f"{output_filename_base}_") as tmp:
                        tmp.write(final_xml_bytes)
                        temp_file_path = tmp.name
                        local_path_used_for_upload = temp_file_path # Store path for logging/cleanup

                    print(f"[INFO] Date: {order_date_str} - XML written to temporary file: {local_path_used_for_upload}")
                    print(f"[INFO] Date: {order_date_str} - Uploading temp file to Dropbox: {dropbox_target_path}")
                     # Call the updated uploader (takes 2 args)
                    upload_file_to_dropbox(local_path_used_for_upload, dropbox_target_path)
                    upload_successful = True
                    print(f"[INFO] Date: {order_date_str} - Temporary file uploaded.")

                except IOError as e:
                    print(f"[ERROR] Date: {order_date_str} - Failed to write XML to temporary file: {e}")
                    return f"ERROR_WRITE_TEMP:{order_date_str}"
                except Exception as e:
                    print(f"[ERROR] Date: {order_date_str} - Failed during Dropbox upload from temporary file {local_path_used_for_upload}: {e}")
                    traceback.print_exc()
                    # Ensure temp file is cleaned up even if upload fails
                    # Return error code AFTER cleanup attempt
                    return f"ERROR_UPLOAD_TEMP:{order_date_str}"
                finally:
                    # Clean up the temporary file if it was created
                    if local_path_used_for_upload and local_path_used_for_upload == temp_file_path and os.path.exists(local_path_used_for_upload):
                        try:
                            os.remove(local_path_used_for_upload)
                            print(f"[INFO] Date: {order_date_str} - Removed temporary file: {local_path_used_for_upload}")
                        except OSError as e:
                            # Log error but don't necessarily fail the whole process just for cleanup failure
                            print(f"[ERROR] Date: {order_date_str} - Failed to remove temporary file {local_path_used_for_upload}: {e}")

            # Check upload status if XML bytes existed
            if upload_successful:
                print(f"--- Successfully completed processing and upload for Order Date: {order_date_str} ---")
                return f"SUCCESS:{order_date_str}"
            else:
                 # If final_xml_bytes existed but upload failed, errors were returned above
                 # This path shouldn't be reached if upload failed and returned an ERROR_* code.
                 print(f"[ERROR] Date: {order_date_str} - Reached end of block with final_xml_bytes but upload was not successful. Logic error?")
                 return f"ERROR_UNKNOWN_UPLOAD_STATE:{order_date_str}"

        else:
            # No final_xml_bytes generated (processed_count was 0 or serialization failed)
            # Appropriate SUCCESS_NO_DATA* or ERROR_SERIALIZE codes returned earlier
            print(f"--- Completed processing for Order Date: {order_date_str} (No data generated or uploaded) ---")
            # Return the code determined earlier (e.g., SUCCESS_NO_DATA_PROCESSED)
            # Find the actual return code based on previous steps if needed.
            # This path is likely redundant given earlier returns.
            # Re-evaluate the return logic carefully. Let's assume previous returns handled this.
            # If we reach here, it means processed_count was > 0 but final_xml_bytes was None/empty.
            if processed_count > 0 and not final_xml_bytes:
                 return f"SUCCESS_NO_DATA_IN_TREE:{order_date_str}" # Reiterate this state
            else: # Should have returned SUCCESS_NO_ORDERS or SUCCESS_NO_DATA_PROCESSED earlier
                 print(f"[WARNING] Reached end of process block unexpectedly for {order_date_str}. Review logic.")
                 return f"STATE_UNKNOWN:{order_date_str}"


    except Exception as e:
        print(f"[FATAL ERROR] Unhandled exception during processing for Order Date {order_date_str}: {e}")
        traceback.print_exc()
        return f"ERROR_UNHANDLED:{order_date_str}"

# Main execution function
async def main():
    # Calculate yesterday's date using JST
    try:
        now_jst = datetime.datetime.now(JST)
        today_jst = now_jst.date()
        yesterday_jst = today_jst - datetime.timedelta(days=1)
        yesterday_date_str = yesterday_jst.strftime('%Y-%m-%d') # Format YYYY-MM-DD for API
    except Exception as e:
         print(f"[ERROR] Could not calculate yesterday's date in JST: {e}")
         return # Cannot proceed without a valid date

    print(f"[INFO] Target date for processing (Yesterday in JST): {yesterday_date_str}")

    # Process only yesterday's date
    result_status = await process_yesterday_order_date(yesterday_date_str)

    # --- Summary Reporting ---
    print("\n--- Overall Processing Summary ---")
    success = False
    if isinstance(result_status, str):
        status_code, date_processed = result_status.split(':', 1) if ':' in result_status else (result_status, yesterday_date_str)

        if status_code == "SUCCESS":
            print(f"Date {date_processed}: Successfully processed and uploaded.")
            success = True
        elif status_code == "SUCCESS_NO_ORDERS":
            print(f"Date {date_processed}: Completed successfully (No orders found matching criteria).")
            success = True # Script ran correctly, no data is expected.
        elif status_code in ["SUCCESS_NO_DATA_PROCESSED", "SUCCESS_NO_DATA_IN_TREE"]:
            print(f"Date {date_processed}: Completed successfully (Orders found, but no data met criteria for final XML/upload).")
            success = True # Script ran correctly, but generated no output file.
        elif status_code.startswith("ERROR_"):
            print(f"Date {date_processed}: FAILED - {status_code}")
        else: # Handle unexpected status codes
            print(f"Date {date_processed}: Unknown status code received: {status_code}")
            print(f"  Full result string: {result_status}")

    else: # Should not happen if process_yesterday_order_date always returns string
        print(f"[SUMMARY-ERROR] An unexpected result type was returned for date {yesterday_date_str}: {type(result_status)} - {result_status}")

    print("---------------------------------")
    if success:
         print("Job completed.")
    else:
         print("Job FAILED. Check logs above for specific errors.")


if __name__ == "__main__":
    if not ACCOUNT_ID or not AUTH_KEY:
        print("[ERROR] ACCOUNT_ID or AUTH_KEY environment variables not set. Exiting.")
    else:
        # Run the main async function (no arguments needed)
        asyncio.run(main())