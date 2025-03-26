import xml.etree.ElementTree as ET
import pandas as pd

def parse_stock_minimal(xml_path):
    tree = ET.parse(xml_path)
    root = tree.getroot()

    rows = []
    for result in root.findall("Result"):
        skus = result.find("Skus")
        if skus is None:
            continue

        for sku in skus.findall("Sku"):
            item_sku_code = sku.findtext("item_sku_code")
            jan_code = sku.findtext("jan_code")

            if not jan_code:
                continue  # Only include rows where JAN code exists

            stock_value = None
            stock_tag = sku.find("Stock")
            if stock_tag is not None:
                stock_result = stock_tag.find("Result")
                if stock_result is not None:
                    stock_value = stock_result.findtext("stock")

            rows.append({
                'item_sku_code': item_sku_code,
                'stock': stock_value,
                'jan_code': jan_code
            })

    return pd.DataFrame(rows)


def parse_order_items(xml_path):
    tree = ET.parse(xml_path)
    root = tree.getroot()

    order_items = []

    for order in root.findall("Order"):
        for detail in order.findall("OrderDetail"):
            order_number = detail.findtext("order_number")
            main_item_code = detail.findtext("item_code")
            jan_code = detail.findtext("jan_cd")
            amount = detail.findtext("amount")

            # main order line
            if jan_code:
                order_items.append({
                    'order_number': order_number,
                    'item_code': main_item_code,
                    'jan_code': jan_code,
                    'amount': amount
                })

            # components of set items
            components = detail.find("Components")
            if components is not None:
                for comp in components.findall("Component"):
                    comp_item_code = comp.findtext("item_code")
                    comp_jan = comp.findtext("jan_cd")
                    comp_count = comp.findtext("component_count")
                    if comp_jan:
                        order_items.append({
                            'order_number': order_number,
                            'item_code': comp_item_code,
                            'jan_code': comp_jan,
                            'amount': comp_count
                        })

    return pd.DataFrame(order_items)


if __name__ == "__main__":
    items_xml_path = "fetched_items_sku_stock_tree.xml"
    orders_xml_path = "fetched_order_details_components_parallel.xml"

    stock_df = parse_stock_minimal(items_xml_path)
    order_items_df = parse_order_items(orders_xml_path)

    # Filter out rows where stock is None or empty
    stock_df = stock_df[stock_df["stock"].notna() & (stock_df["stock"] != "")]

    stock_df.to_csv("stock.csv", index=False, encoding="utf-8-sig")
    order_items_df.to_csv("order_items.csv", index=False, encoding="utf-8-sig")

    print("stock.csv & order_items.csv created")

