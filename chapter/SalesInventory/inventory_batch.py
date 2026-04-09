import pandas as pd
from datetime import datetime

# File paths
INVENTORY_FILE = ("/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/SalesInventory/inventory.csv")
SALES_FILE = "/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/SalesInventory/sales.csv"
OUTPUT_INVENTORY = "updated_inventory.csv"
LOG_FILE = "batch_log.txt"

def log(message):
    with open(LOG_FILE, "a") as f:
        f.write(f"{datetime.now()} - {message}\n")

def load_data():
    inventory = pd.read_csv(INVENTORY_FILE)
    sales = pd.read_csv(SALES_FILE)
    sales['sale_date'] = pd.to_datetime(sales['sale_date'])
    return inventory, sales

def apply_sales(inventory, sales):
    today = pd.Timestamp.today().normalize()

    today_sales = sales[sales['sale_date'] == today]

    daily_sales = today_sales.groupby('product_id')['quantity_sold'].sum().reset_index()

    updated_inventory = inventory.merge(daily_sales, on='product_id', how='left')
    updated_inventory['quantity_sold'] = updated_inventory['quantity_sold'].fillna(0)

    updated_inventory['stock_quantity'] -= updated_inventory['quantity_sold']

    # Prevent negative stock
    updated_inventory['stock_quantity'] = updated_inventory['stock_quantity'].clip(lower=0)

    return updated_inventory.drop(columns=['quantity_sold'])

def generate_reports(inventory, sales):
    today = pd.Timestamp.today().normalize()

    today_sales = sales[sales['sale_date'] == today]
    report = today_sales.merge(inventory, on='product_id')

    report['total_sale_value'] = report['quantity_sold'] * report['price']

    total_revenue = report['total_sale_value'].sum()

    low_stock = inventory[inventory['stock_quantity'] <= inventory['reorder_level']]

    print("\n--- Daily Report ---")
    print(report[['product_name', 'quantity_sold', 'total_sale_value']])
    print("Total Revenue:", total_revenue)

    print("\n--- Low Stock Alert ---")
    print(low_stock[['product_name', 'stock_quantity']])

def save_inventory(inventory):
    inventory.to_csv(OUTPUT_INVENTORY, index=False)

def main():
    try:
        log("Batch job started")

        inventory, sales = load_data()
        inventory = apply_sales(inventory, sales)
        generate_reports(inventory, sales)
        save_inventory(inventory)

        log("Batch job completed successfully")

    except Exception as e:
        log(f"ERROR: {str(e)}")

if __name__ == "__main__":
    main()