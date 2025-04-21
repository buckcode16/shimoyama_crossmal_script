import copy
import asyncio
import hashlib
import httpx
import xml.etree.ElementTree as ET
import os
import tempfile
from dropbox_uploader import upload_file_to_dropbox # Assuming this exists and works
from dotenv import load_dotenv
import datetime
import argparse # Added for command-line arguments
import traceback # Added for better error reporting

load_dotenv()

# API Credentials and Endpoints
ACCOUNT_ID = os.getenv("ACCOUNT_ID")
AUTH_KEY = os.getenv("AUTH_KEY")
BASE_URLS = {
    "order": "https://crossmall.jp/webapi2/get_order",
    "order_detail": "https://crossmall.jp/webapi2/get_order_detail",
    "order_component": "https://crossmall.jp/webapi2/get_order_component"
}

# Utility: Generate MD5 signature
def generate_signature(query_string, auth_key):
    raw_string = f"{query_string}{auth_key}"
    return hashlib.md5(raw_string.encode()).hexdigest()

# Asynchronous make_request function with retry logic
async def async_make_request(endpoint, params, client, max_retries=3, delay=2.0):
    query_parts = []
    for key, value in params.items():
        if value is not None:
            if isinstance(value, list):
                for item in value:
                    query_parts.append(f"{key}={item}")
            else:
                query_parts.append(f"{key}={value}")
    query_string = "&".join(query_parts)

    signature = generate_signature(query_string, AUTH_KEY)
    api_url = f"{BASE_URLS[endpoint]}?{query_string}&signing={signature}"
    attempts = 0
    # Determine context for logging
    log_context = ""
    if 'order_date_fr' in params and params['order_date_fr'] == params.get('order_date_to'):
         log_context = f"for order_date {params['order_date_fr']}"
    elif 'order_number' in params:
         log_context = f"for order {params.get('order_number')} line {params.get('line_no', 'N/A')}"

    while attempts < max_retries:
        try:
            response = await client.get(api_url, timeout=60.0)
            response.raise_for_status()
            return response.text
        except httpx.HTTPStatusError as e:
            print(f"HTTP Error in {endpoint} {log_context} (attempt {attempts+1}/{max_retries}): {e.response.status_code} - {e.response.text}")
        except httpx.RequestError as e:
            print(f"Request Error in {endpoint} {log_context} (attempt {attempts+1}/{max_retries}): {e}")
        except Exception as e:
            print(f"General Exception in {endpoint} {log_context} (attempt {attempts+1}/{max_retries}): {e}")

        attempts += 1
        if attempts < max_retries:
            await asyncio.sleep(delay * (2 ** (attempts - 1)))
        else:
            print(f"Max retries reached for {endpoint} with parameters: {params}")
            return None

# Asynchronous function to fetch base order data sequentially (pagination) for a SINGLE specific order date
async def async_fetch_base_orders_for_day(client, order_date): # Takes client and single order date
    """Fetches base order data filtered for a single order_date."""
    all_orders_data = {}
    last_order_number = ""
    page_count = 0

    print(f"[INFO] Date: {order_date} - Starting fetch...") # Log the date being fetched

    while True:
        page_count += 1
        params = {
            "account": ACCOUNT_ID,
            "condition": "1",
            # "cancel_flag": "0",
            "creating_source_type[]": ["99"],
            "order_date_fr": order_date, # Set both to the single date
            "order_date_to": order_date
        }
        if last_order_number:
            params["order_number"] = last_order_number

        response_text = await async_make_request("order", params, client)

        if not response_text:
            print(f"[ERROR] Date: {order_date} - Failed to fetch orders page {page_count}.")
            break

        try:
            root = ET.fromstring(response_text)
        except ET.ParseError as e:
            print(f"[ERROR] Date: {order_date} - Failed to parse XML page {page_count}: {e}")
            print(f"  Response text was: {response_text[:500]}...")
            break

        results = root.findall(".//Result")

        # Log pagination results clearly per date
        if not results:
            if page_count == 1:
                 print(f"[INFO] Date: {order_date} - No orders found.")
            # else: # Normal end of pagination, less verbose log
                 # print(f"  Date: {order_date} - Page {page_count}: Found 0 orders (End of data).")
            break
        else:
            # Use simpler log format for pagination within the date
            print(f"  Date: {order_date} - Page {page_count}: Found {len(results)} orders...")

        new_orders_found_on_page = False
        for res in results:
            order_number_el = res.find("order_number")
            if order_number_el is not None and order_number_el.text:
                order_number = order_number_el.text.strip()
                all_orders_data[order_number] = copy.deepcopy(res)
                new_orders_found_on_page = True # Mark true even if overwriting theoretically
            else:
                print(f"[WARNING] Date: {order_date} - Found a Result without an order_number on page {page_count}.")

        # Pagination logic based on last order number and results count
        if results:
            last_order_el = results[-1].find("order_number")
            if last_order_el is not None and last_order_el.text:
                 current_last_order = last_order_el.text.strip()
                 # Avoid complex check on repeated last_order_number unless proven necessary
                 last_order_number = current_last_order
            else:
                print(f"[WARNING] Date: {order_date} - Last Result on page {page_count} missing order_number. Stopping pagination.")
                break
        else: # Should be caught by the 'if not results' block above, but defensive check
             break

        # End pagination if fewer than 100 results received
        if len(results) < 100:
            # print(f"  Date: {order_date} - Page {page_count}: Found {len(results)} orders (< 100). Assuming end.")
            break

    print(f"[INFO] Date: {order_date} - Fetched total {len(all_orders_data)} base orders.")
    return all_orders_data

# Asynchronous function to fetch order detail (no changes needed)
async def async_get_order_detail(order_number, client, semaphore):
    params = {"account": ACCOUNT_ID, "order_number": order_number}
    async with semaphore:
        detail_xml = await async_make_request("order_detail", params, client)
        if not detail_xml:
            print(f"[ERROR] Order {order_number}: No order detail XML returned.")
            return None
        try:
            return ET.fromstring(detail_xml)
        except ET.ParseError as e:
            print(f"[ERROR] Order {order_number}: Failed to parse order_detail XML: {e}")
            return None

# Asynchronous function to fetch order component (no changes needed)
async def async_get_order_component(order_number, line_no, client, semaphore):
    params = {"account": ACCOUNT_ID, "order_number": order_number, "line_no": line_no}
    async with semaphore:
        comp_xml = await async_make_request("order_component", params, client)
        if not comp_xml:
            print(f"[ERROR] Order {order_number}, Line {line_no}: No order component XML returned.")
            return None
        try:
            return ET.fromstring(comp_xml)
        except ET.ParseError as e:
            print(f"[ERROR] Order {order_number}, Line {line_no}: Failed to parse order_component XML: {e}")
            return None

# Asynchronous function to process a single order, integrating all data (no major changes needed)
async def process_order(order_number, base_order_element, client, semaphore):
    """Builds the complete XML structure for a single order."""
    # print(f"Processing order: {order_number}") # Keep if needed

    # 1. Create the main <Order> element
    order_elem = ET.Element("Order", attrib={"order_number": order_number})

    # 2. Create <OrderHeader> and copy base order fields into it
    order_header_elem = ET.Element("OrderHeader")
    for child in base_order_element:
        order_header_elem.append(copy.deepcopy(child))
    order_elem.append(order_header_elem)

    # 3. Prepare for Order Details and potential Components
    order_details_container = ET.Element("OrderDetails")
    component_tasks_to_run = [] # Stores tuples: (task_coroutine, order_detail_element_ref)

    # 4. Fetch Order Detail data
    detail_root = await async_get_order_detail(order_number, client, semaphore)

    if detail_root is not None:
        detail_results = detail_root.findall(".//Result")
        if detail_results:
            for detail_result in detail_results:
                order_detail_elem = ET.Element("OrderDetail")
                line_no = None
                has_jan_cd = False
                for child in detail_result:
                    order_detail_elem.append(copy.deepcopy(child))
                    if child.tag == "jan_cd" and child.text and child.text.strip():
                        has_jan_cd = True
                    if child.tag == "line_no" and child.text:
                        line_no = child.text.strip()

                if not has_jan_cd and line_no:
                    comp_coro = async_get_order_component(order_number, line_no, client, semaphore)
                    component_tasks_to_run.append((comp_coro, order_detail_elem))
                order_details_container.append(order_detail_elem)

    # 5. Fetch and process Order Components concurrently if any were scheduled
    if component_tasks_to_run:
        component_coroutines = [task[0] for task in component_tasks_to_run]
        target_detail_elements = [task[1] for task in component_tasks_to_run]
        component_results_roots = await asyncio.gather(*component_coroutines, return_exceptions=True)

        for i, result_or_exc in enumerate(component_results_roots):
            target_detail_elem = target_detail_elements[i]
            line_no_for_log = target_detail_elem.findtext('line_no', 'N/A')

            if isinstance(result_or_exc, Exception):
                print(f"[ERROR] Order {order_number}, Line {line_no_for_log}: Exception fetching component: {result_or_exc}")
                continue

            comp_root = result_or_exc
            if comp_root is not None:
                component_results = comp_root.findall(".//Result")
                if component_results:
                    components_container = ET.Element("Components")
                    components_added = 0
                    for comp_result in component_results:
                        comp_jan_elem = comp_result.find("jan_cd")
                        if (comp_jan_elem is not None and comp_jan_elem.text and comp_jan_elem.text.strip()) or len(comp_result) > 0:
                            component_node = ET.Element("Component")
                            for comp_child in comp_result:
                                component_node.append(copy.deepcopy(comp_child))
                            components_container.append(component_node)
                            components_added += 1

                    if len(components_container):
                        target_detail_elem.append(components_container)

    # 6. Append the <OrderDetails> container to the main <Order>
    if len(order_details_container):
        order_elem.append(order_details_container)

    return order_elem

# Asynchronously build the complete order tree for a given set of base orders (for a single day)
async def build_complete_order_tree(orders_data, client, order_date): # Takes client and order_date for logging
    """Builds the final XML tree containing integrated order data for a single day."""
    orders_root = ET.Element("Orders")
    semaphore = asyncio.Semaphore(10) # Controls concurrency for details/components within this day's orders

    order_processing_tasks = [
        process_order(order_num, base_element, client, semaphore)
        for order_num, base_element in orders_data.items()
    ]
    # print(f"[INFO] Date: {order_date} - Starting detail/component processing for {len(order_processing_tasks)} orders...") # Included in next log
    order_elements_results = await asyncio.gather(*order_processing_tasks, return_exceptions=True)
    # print(f"[INFO] Date: {order_date} - Finished detail/component processing.") # Included in next log

    processed_count = 0
    failed_order_processing_count = 0
    for i, result_or_exc in enumerate(order_elements_results):
        order_num = list(orders_data.keys())[i]
        if isinstance(result_or_exc, Exception):
            print(f"[ERROR] Date: {order_date}, Order {order_num}: Exception during processing: {result_or_exc}")
            failed_order_processing_count += 1
            continue

        order_elem = result_or_exc
        if order_elem is not None:
            orders_root.append(order_elem)
            processed_count += 1

    print(f"[INFO] Date: {order_date} - Processed {processed_count} orders into XML tree.")
    if failed_order_processing_count > 0:
         print(f"[WARNING] Date: {order_date} - Failed detail/component processing for {failed_order_processing_count} orders.")

    return orders_root, processed_count

# Function to process a single order date completely
async def process_single_order_date(order_date_str):
    """Fetches, processes, and saves/uploads order data for a single order date."""
    print(f"--- Starting processing for Order Date: {order_date_str} ---")
    date_obj = datetime.datetime.strptime(order_date_str, '%Y-%m-%d').date()
    date_nodash = date_obj.strftime('%Y%m%d')
    # Naming convention specific to order date
    output_filename = f"orders_{date_nodash}.xml"
    dropbox_target_path = f"/Reports/order/orders_{date_nodash}.xml" # Adjust path as needed

    orders_data_dict = {}
    orders_tree = None
    processed_count = 0
    final_xml_bytes = None # Initialize

    try:
        # Create a client session per date task for simplicity, or pass one from main if preferred
        async with httpx.AsyncClient() as client:
            # Step 1: Fetch base order data for the specific order_date
            orders_data_dict = await async_fetch_base_orders_for_day(client, order_date=order_date_str)

            if not orders_data_dict:
                # Already logged "No orders found" inside the fetch function
                return f"SUCCESS_NO_ORDERS:{order_date_str}" # Indicate success but no data

            # Step 2: Process orders (fetch details/components)
            orders_tree, processed_count = await build_complete_order_tree(orders_data_dict, client, order_date_str)

            if processed_count == 0 and len(orders_data_dict) > 0:
                 # Already logged failures inside build_complete_order_tree
                 print(f"[WARNING] Date: {order_date_str} - Base orders fetched, but none successfully processed into the final tree.")
                 return f"SUCCESS_NO_DATA:{order_date_str}" # Success, but no output generated

            if orders_tree is None:
                 print(f"[ERROR] Date: {order_date_str} - Failed to build the final order tree structure.")
                 return f"ERROR_NULL_TREE:{order_date_str}" # Treat as error

        # Step 3: Serialize the final XML tree (only if orders were processed)
        if orders_tree is not None and processed_count > 0:
            try:
                final_xml_bytes = ET.tostring(orders_tree, encoding="utf-8", method='xml', xml_declaration=True)
                # print(f"[INFO] Date: {order_date_str} - Successfully generated final XML tree.") # Less verbose
            except Exception as e:
                print(f"[ERROR] Date: {order_date_str} - Failed to serialize final XML tree: {e}")
                return f"ERROR_SERIALIZE:{order_date_str}"
        # No else needed, handled by processed_count check above


        # Step 4: Write to dated local file and upload (only if XML bytes exist)
        if final_xml_bytes:
            write_xml_flag = os.getenv("WRITE_XML", "true").lower() == "true"

            if write_xml_flag:
                try:
                    with open(output_filename, "wb") as f:
                        f.write(final_xml_bytes)
                    print(f"[INFO] Date: {order_date_str} - Order data saved locally to {output_filename}")
                    upload_file_to_dropbox(output_filename, dropbox_target_path)
                    print(f"[INFO] Date: {order_date_str} - Uploaded {output_filename} to Dropbox path {dropbox_target_path}")
                except IOError as e:
                    print(f"[ERROR] Date: {order_date_str} - Failed to write XML to {output_filename}: {e}")
                    return f"ERROR_WRITE_LOCAL:{order_date_str}"
                except Exception as e:
                    print(f"[ERROR] Date: {order_date_str} - Failed during Dropbox upload for {output_filename}: {e}")
                    return f"ERROR_UPLOAD:{order_date_str}"
            else:
                # print(f"[INFO] Date: {order_date_str} - WRITE_XML not 'true'. Using temp file for upload.") # Less verbose
                temp_file_path = None
                try:
                    with tempfile.NamedTemporaryFile(delete=False, suffix=".xml", mode='wb') as tmp:
                        tmp.write(final_xml_bytes)
                        temp_file_path = tmp.name
                    upload_file_to_dropbox(temp_file_path, dropbox_target_path)
                    print(f"[INFO] Date: {order_date_str} - Uploaded temp file to Dropbox path {dropbox_target_path}")
                except IOError as e:
                    print(f"[ERROR] Date: {order_date_str} - Failed to write XML to temporary file: {e}")
                    return f"ERROR_WRITE_TEMP:{order_date_str}"
                except Exception as e:
                    print(f"[ERROR] Date: {order_date_str} - Failed during Dropbox upload from temporary file: {e}")
                    return f"ERROR_UPLOAD_TEMP:{order_date_str}"
                finally:
                    if temp_file_path and os.path.exists(temp_file_path):
                        try:
                            os.remove(temp_file_path)
                        except OSError as e:
                            print(f"[ERROR] Date: {order_date_str} - Failed to remove temp file {temp_file_path}: {e}")

        # If we reach here and final_xml_bytes were generated, it's a success.
        # If final_xml_bytes is None (because processed_count was 0), earlier returns handled it.
        if final_xml_bytes:
            print(f"--- Successfully completed processing for Order Date: {order_date_str} ---")
            return f"SUCCESS:{order_date_str}"
        else:
            # Should have been caught by SUCCESS_NO_DATA return earlier
            print(f"--- Completed processing for Order Date: {order_date_str} (No data generated) ---")
            return f"SUCCESS_NO_DATA:{order_date_str}"


    except Exception as e:
        print(f"[FATAL ERROR] Unhandled exception during processing for Order Date {order_date_str}: {e}")
        traceback.print_exc()
        return f"ERROR_UNHANDLED:{order_date_str}"


# Main execution function
async def main(start_date_str, end_date_str):
    try:
        start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').date()
    except ValueError:
        print("[ERROR] Invalid date format. Please use YYYY-MM-DD for both dates.")
        return

    if start_date > end_date:
        print("[ERROR] Start date cannot be after end date.")
        return

    # Generate list of dates to process
    dates_to_process = []
    current_date = start_date
    while current_date <= end_date:
        dates_to_process.append(current_date.strftime('%Y-%m-%d'))
        current_date += datetime.timedelta(days=1)

    if not dates_to_process:
        print("[INFO] No dates found in the specified range.")
        return

    print(f"[INFO] Preparing to process {len(dates_to_process)} order dates from {start_date_str} to {end_date_str} concurrently.")

    # Create concurrent tasks for each date
    tasks = [process_single_order_date(date_str) for date_str in dates_to_process]

    # Run tasks concurrently and gather results (including exceptions)
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Process results (same summary logic as before)
    print("\n--- Overall Processing Summary ---")
    success_count = 0
    no_orders_count = 0
    no_data_count = 0
    error_count = 0
    unhandled_exceptions = 0

    for result in results:
        if isinstance(result, Exception):
            # This catches exceptions raised *before* the task's own try/except could return an ERROR string
            print(f"[SUMMARY-ERROR] An unexpected framework exception occurred: {result}")
            unhandled_exceptions += 1
            error_count += 1
        elif isinstance(result, str):
            if result.startswith("SUCCESS:"):
                success_count += 1
                # Detailed success already logged within the task
            elif result.startswith("SUCCESS_NO_ORDERS:"):
                no_orders_count += 1
                print(f"[SUMMARY-INFO] No orders found for {result.split(':')[1]}")
            elif result.startswith("SUCCESS_NO_DATA:"):
                no_data_count += 1
                print(f"[SUMMARY-INFO] Orders found but no data generated for {result.split(':')[1]}")
            elif result.startswith("ERROR_"):
                error_count += 1
                print(f"[SUMMARY-ERROR] Failed processing for {result.split(':')[1]} ({result.split(':')[0]})")
            else:
                 print(f"[SUMMARY-WARNING] Unknown result string: {result}")
                 error_count +=1
        else:
             print(f"[SUMMARY-WARNING] Unknown result type: {type(result)} - {result}")
             error_count += 1


    print("\n--- Final Counts ---")
    print(f"Successfully processed dates with data: {success_count}")
    print(f"Dates processed with no orders found:  {no_orders_count}")
    print(f"Dates processed with no output data:   {no_data_count}")
    print(f"Dates with processing errors:         {error_count}")
    if unhandled_exceptions > 0:
        print(f"  (Including {unhandled_exceptions} unexpected framework exceptions)")
    print(f"Total dates attempted:                {len(dates_to_process)}")
    print("---------------------------------")


if __name__ == "__main__":
    if not ACCOUNT_ID or not AUTH_KEY:
        print("[ERROR] ACCOUNT_ID or AUTH_KEY environment variables not set. Exiting.")
    else:
        # --- Command Line Argument Parsing ---
        parser = argparse.ArgumentParser(description="Fetch and process Crossmall order data day-by-day for a given order date range.")
        # Changed arguments back to represent the range of individual dates to process
        parser.add_argument("start_date", help="Start date for order date filter (YYYY-MM-DD)")
        parser.add_argument("end_date", help="End date for order date filter (YYYY-MM-DD)")
        args = parser.parse_args()

        # Run the main async function with parsed arguments
        asyncio.run(main(args.start_date, args.end_date))