#!/bin/bash
echo "Starting get_items_sku_stock_parallel.py..."
python get_items_sku_stock_parallel.py

echo "Starting get_order_detail_component_parallel.py..."
python get_order_detail_component_parallel.py

echo "All tasks completed."
