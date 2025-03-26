#!/usr/bin/env python3
import copy
import asyncio
import hashlib
import httpx
import xml.etree.ElementTree as ET
import os
import tempfile
from dropbox_uploader import upload_file_to_dropbox
from dotenv import load_dotenv
import datetime

load_dotenv()

# API Credentials and Endpoints
ACCOUNT_ID = os.getenv("ACCOUNT_ID")
AUTH_KEY = os.getenv("AUTH_KEY")
BASE_URLS = {
    "order": "https://crossmall.jp/webapi2/get_order",
    "order_detail": "https://crossmall.jp/webapi2/get_order_detail",
    "order_component": "https://crossmall.jp/webapi2/get_order_component"
}
# DELIVERY_DATE = "2025-03-18"
# ORDER_DATE_FR = "2025-03-20"
# ORDER_DATE_TO = "2025-03-20"

yesterday = datetime.date.today() - datetime.timedelta(days=1)
ORDER_DATE_FR = yesterday.strftime("%Y-%m-%d")
ORDER_DATE_TO = yesterday.strftime("%Y-%m-%d")

DROPBOX_ACCESS_TOKEN = os.getenv("DROPBOX_ACCESS_TOKEN")
DROPBOX_PATH = "/Reports/order.xml"  # Base path in Dropbox

# Utility: Generate MD5 signature from query string and auth key
def generate_signature(query_string, auth_key):
    raw_string = f"{query_string}{auth_key}"
    return hashlib.md5(raw_string.encode()).hexdigest()

# Asynchronous make_request function with retry logic
async def async_make_request(endpoint, params, client, max_retries=3, delay=2.0):
    query_string = "&".join([f"{key}={value}" for key, value in params.items() if value])
    signature = generate_signature(query_string, AUTH_KEY)
    api_url = f"{BASE_URLS[endpoint]}?{query_string}&signing={signature}"
    attempts = 0
    while attempts < max_retries:
        try:
            response = await client.get(api_url, timeout=60.0)
            if response.status_code == 200:
                return response.text
            else:
                print(f"HTTP Error in {endpoint} (attempt {attempts+1}/{max_retries}): {response.status_code} - {response.text}")
        except Exception as e:
            print(f"Exception in {endpoint} (attempt {attempts+1}/{max_retries}): {e}")
        attempts += 1
        if attempts < max_retries:
            await asyncio.sleep(delay)
    print(f"Max retries reached for {endpoint} with parameters: {params}")
    return None

# Asynchronous function to fetch orders sequentially (pagination)
async def async_fetch_orders(client, delivery_date=None, order_date_fr=None, order_date_to=None):
    all_order_numbers = []
    last_order_number = ""
    while True:
        params = {
            "account": ACCOUNT_ID,
            "condition": "1",
            "cancel_flag":"0",
            "creating_source_type[]": "99",
            "order_date_fr": order_date_fr,
            "order_date_to": order_date_to,
        }
            # "delivery_date": delivery_date
        if last_order_number:
            params["order_number"] = last_order_number
        print(f"[DEBUG] Fetching orders with params: {params}")
        response_text = await async_make_request("order", params, client)
        if not response_text:
            break
        root = ET.fromstring(response_text)
        results = root.findall(".//Result")
        if not results:
            print("No more orders found.")
            break
        for res in results:
            order_number_el = res.find("order_number")
            if order_number_el is not None and order_number_el.text:
                all_order_numbers.append(order_number_el.text.strip())
        last_order_el = results[-1].find("order_number")
        if last_order_el is not None and last_order_el.text:
            last_order_number = last_order_el.text.strip()
        if len(results) < 100:
            break
    return all_order_numbers

# Asynchronous function to fetch order detail for a given order number
async def async_get_order_detail(order_number, client, semaphore):
    params = {
        "account": ACCOUNT_ID,
        "order_number": order_number
    }
    async with semaphore:
        print(f"[DEBUG] Fetching order detail for order_number: {order_number}")
        detail_xml = await async_make_request("order_detail", params, client)
        if not detail_xml:
            print(f"[ERROR] No order detail for order_number: {order_number}")
        return detail_xml

# Asynchronous function to fetch order component for a given order number and line_no
async def async_get_order_component(order_number, line_no, client, semaphore):
    params = {
        "account": ACCOUNT_ID,
        "order_number": order_number,
        "line_no": line_no
    }
    async with semaphore:
        print(f"[DEBUG] Fetching order component for order_number: {order_number}, line_no: {line_no}")
        comp_xml = await async_make_request("order_component", params, client)
        if not comp_xml:
            print(f"[ERROR] No component for order_number: {order_number}, line_no: {line_no}")
        return comp_xml

# Asynchronous function to process a single order:
# Fetch order detail, and for any detail with an empty/missing <jan_cd>,
# fetch order components concurrently.
async def process_order(order_number, client, semaphore):
    print(f"Processing order: {order_number}")
    order_elem = ET.Element("Order", attrib={"order_number": order_number})
    detail_xml = await async_get_order_detail(order_number, client, semaphore)
    if not detail_xml:
        return order_elem
    detail_root = ET.fromstring(detail_xml)
    component_tasks = []  # List of tuples: (order_detail_elem, order_number, line_no)
    detail_elems = []
    for detail in detail_root.findall(".//Result"):
        order_detail_elem = ET.Element("OrderDetail")
        for child in detail:
            order_detail_elem.append(copy.deepcopy(child))
        jan_elem = detail.find("jan_cd")
        if jan_elem is None or not (jan_elem.text and jan_elem.text.strip()):
            line_no_elem = detail.find("line_no")
            if line_no_elem is not None and line_no_elem.text:
                print(f"  Detail with line_no {line_no_elem.text.strip()} has empty jan_cd. Fetching components...")
                component_tasks.append((order_detail_elem, order_number, line_no_elem.text.strip()))
        detail_elems.append(order_detail_elem)
    # Launch concurrent component requests for details that need them.
    comp_coroutines = [async_get_order_component(order_num, line_no, client, semaphore)
                       for (_, order_num, line_no) in component_tasks]
    comp_results = await asyncio.gather(*comp_coroutines)
    # Attach component results to the corresponding detail elements.
    for ((order_detail_elem, _, _), comp_xml) in zip(component_tasks, comp_results):
        if comp_xml:
            comp_root = ET.fromstring(comp_xml)
            component_results = comp_root.findall(".//Result")
            if component_results:
                components_container = ET.Element("Components")
                for comp in component_results:
                    comp_jan_elem = comp.find("jan_cd")
                    if comp_jan_elem is not None and comp_jan_elem.text and comp_jan_elem.text.strip():
                        component_node = ET.Element("Component")
                        for comp_child in comp:
                            component_node.append(copy.deepcopy(comp_child))
                        components_container.append(component_node)
                if len(components_container):
                    order_detail_elem.append(components_container)
    for de in detail_elems:
        order_elem.append(de)
    return order_elem

# Asynchronously build the order tree for all orders (details & components processed in parallel)
async def build_order_tree_parallel(order_numbers):
    orders_root = ET.Element("Orders")
    semaphore = asyncio.Semaphore(10)  # Limit concurrent API calls
    async with httpx.AsyncClient() as client:
        order_tasks = [process_order(order_num, client, semaphore) for order_num in order_numbers]
        order_elements = await asyncio.gather(*order_tasks)
    for order_elem in order_elements:
        orders_root.append(order_elem)
    return orders_root

async def main():
    output_filename = "fetched_order_details_components_parallel.xml"
    # Step 1: Fetch order numbers sequentially (due to pagination)
    async with httpx.AsyncClient() as client:
        # order_numbers = await async_fetch_orders(DELIVERY_DATE, client)
        order_numbers = await async_fetch_orders(client, order_date_fr=ORDER_DATE_FR, order_date_to=ORDER_DATE_TO)
    print(f"Total orders fetched: {len(order_numbers)}")
    # Step 2: Process each order in parallel (fetch details and, when needed, components)
    orders_tree = await build_order_tree_parallel(order_numbers)
    final_xml = ET.tostring(orders_tree, encoding="utf-8")
    
    # Check WRITE_XML environment variable (default true)
    if os.getenv("WRITE_XML", "true").lower() == "true":
        with open(output_filename, "wb") as f:
            f.write(final_xml)
        print(f"Fetched order details (with components) saved to {output_filename}")
        upload_file_to_dropbox(output_filename, DROPBOX_PATH, DROPBOX_ACCESS_TOKEN)
    else:
        print("WRITE_XML is not true, so skipping persistent local file write.")
        # Write to a temporary file, upload, then remove it.
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xml") as tmp:
            tmp.write(final_xml)
            temp_filename = tmp.name
        print(f"Temporary file created at {temp_filename} for upload.")
        upload_file_to_dropbox(temp_filename, DROPBOX_PATH, DROPBOX_ACCESS_TOKEN)
        os.remove(temp_filename)
        print("Temporary file removed after upload.")

if __name__ == "__main__":
    asyncio.run(main())
