import asyncio
import hashlib
import httpx
import traceback
import xml.etree.ElementTree as ET
from dropbox_uploader import upload_file_to_dropbox
import os
from dotenv import load_dotenv
load_dotenv()

# --- Configuration ---
ACCOUNT_ID = os.getenv("ACCOUNT_ID")
AUTH_KEY = os.getenv("AUTH_KEY")
CONDITION = "1"

BASE_URL_ITEMS = "https://crossmall.jp/webapi2/get_item"
BASE_URL_SKU = "https://crossmall.jp/webapi2/get_item_sku"
BASE_URL_STOCK = "https://crossmall.jp/webapi2/get_stock"
DROPBOX_PATH = "/Reports/stock.xml"



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
                item_code = item_code_elem.text.strip()
            items.append(item_elem)
        total_items += len(results)
        print(f"Retrieved {len(results)} items. Last item_code: {item_code}")
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
        print(f"[DEBUG] Fetching SKU for item_code: {item_code}")
        try:
            response = await client.get(api_url)
            if response.status_code == 200:
                return response.text
            else:
                print(f"[ERROR] Failed SKU for {item_code}: Status code {response.status_code}")
                return None
        except Exception as e:
            print(f"[EXCEPTION] Exception fetching SKU for {item_code}: {repr(e)}")
            traceback.print_exc()
            return None

async def fetch_stock_for_sku(sku_code, client, semaphore):
    """
    Asynchronously fetch stock data for a given sku_code.
    """
    query_string = f"account={ACCOUNT_ID}&sku_code={sku_code}"
    signature = generate_signature(query_string, AUTH_KEY)
    api_url = f"{BASE_URL_STOCK}?{query_string}&signing={signature}"
    async with semaphore:
        print(f"[DEBUG] Fetching stock for sku_code: {sku_code}")
        try:
            response = await client.get(api_url)
            if response.status_code == 200:
                return response.text
            else:
                print(f"[ERROR] Failed stock for {sku_code}: Status code {response.status_code}")
                return None
        except Exception as e:
            print(f"[EXCEPTION] Exception fetching stock for {sku_code}: {repr(e)}")
            traceback.print_exc()
            return None

async def process_item(item_elem, client, sku_semaphore, stock_semaphore):
    item_code_elem = item_elem.find("item_code")
    if item_code_elem is None or not item_code_elem.text:
        print("Item has no item_code, skipping SKU/Stock fetch.")
        return item_elem
    item_code = item_code_elem.text.strip()
    sku_response_text = await fetch_sku_for_item(item_code, client, sku_semaphore)
    if sku_response_text is None:
        print(f"No SKU response for item_code: {item_code}")
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
    for sku_result in sku_results:
        sku_elem = ET.Element("Sku")
        for child in sku_result:
            sku_elem.append(child)
        sku_code = sku_result.findtext("item_sku_code")
        if sku_code and sku_code.strip():
            sku_code = sku_code.strip()
            stock_tasks.append(asyncio.create_task(fetch_stock_for_sku(sku_code, client, stock_semaphore)))
        else:
            stock_tasks.append(asyncio.sleep(0, result=None))
        sku_elements.append(sku_elem)

    if stock_tasks:
        stock_responses = await asyncio.gather(*stock_tasks)
        for sku_elem, stock_response_text in zip(sku_elements, stock_responses):
            if stock_response_text:
                try:
                    stock_root = ET.fromstring(stock_response_text)
                except ET.ParseError as e:
                    print(f"Error parsing stock response for sku_code {sku_elem.findtext('item_sku_code')}: {e}")
                    continue
                stock_container = ET.Element("Stock")
                stock_results = stock_root.findall(".//Result")
                for stock_result in stock_results:
                    stock_container.append(stock_result)
                sku_elem.append(stock_container)

    for sku_elem in sku_elements:
        skus_container.append(sku_elem)
    item_elem.append(skus_container)
    return item_elem

async def process_all_items(items):
    sku_semaphore = asyncio.Semaphore(10)
    stock_semaphore = asyncio.Semaphore(10)
    async with httpx.AsyncClient() as client:
        tasks = [process_item(item, client, sku_semaphore, stock_semaphore) for item in items]
        results = await asyncio.gather(*tasks)
    return results

def main():
    print("Fetching items...")
    items = fetch_all_items()
    print(f"Total items to process: {len(items)}")
    processed_items = asyncio.run(process_all_items(items))
    root = ET.Element("Items")
    for item in processed_items:
        root.append(item)
    output_filename = "fetched_items_sku_stock_tree.xml"
    tree = ET.ElementTree(root)
    if os.getenv("WRITE_XML", "false").lower() == "true":
        # Write XML to a persistent file.
        tree.write(output_filename, encoding="utf-8", xml_declaration=True)
        print(f"Final hierarchical XML saved to {output_filename}")
        upload_file_to_dropbox(output_filename, DROPBOX_PATH)
    else:
        print("WRITE_XML is not true, so not saving a persistent local file.")
        # Write XML to a temporary file, upload it, and then remove the file.
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xml") as tmp:
            tree.write(tmp.name, encoding="utf-8", xml_declaration=True)
            temp_filename = tmp.name
        print(f"Temporary file created at {temp_filename} for upload.")
        upload_file_to_dropbox(temp_filename, DROPBOX_PATH)
        os.remove(temp_filename)
        print("Temporary file removed after upload.")

if __name__ == "__main__":
    main()
