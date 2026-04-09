import pandas as pd
from datetime import datetime


class InventorySystem:

    def __init__(self, inventory_file, sales_file, restock_file):
        self.inventory_file = inventory_file
        self.sales_file = sales_file
        self.restock_file = restock_file

        self.inventory = pd.read_csv(inventory_file)
        self.sales = pd.read_csv(sales_file)
        self.restock = pd.read_csv(restock_file)

        # Convert date columns
        self.sales['date'] = pd.to_datetime(self.sales['date'])
        self.restock['date'] = pd.to_datetime(self.restock['date'])

    # ----------------------------
    # Process daily inventory
    # ----------------------------
    def process_daily_inventory(self):
        all_dates = sorted(
            set(self.sales['date']).union(set(self.restock['date']))
        )

        daily_records = []

        current_inventory = self.inventory.copy()

        for date in all_dates:
            print(f"\nProcessing Date: {date.date()}")

            # --- Add Restock ---
            restock_today = self.restock[self.restock['date'] == date]

            for _, row in restock_today.iterrows():
                product_id = row['product_id']
                qty = row['quantity']

                current_inventory.loc[
                    current_inventory['product_id'] == product_id, 'stock'
                ] += qty

            # --- Subtract Sales ---
            sales_today = self.sales[self.sales['date'] == date]

            for _, row in sales_today.iterrows():
                product_id = row['product_id']
                qty = row['quantity']

                current_inventory.loc[
                    current_inventory['product_id'] == product_id, 'stock'
                ] -= qty

            # Prevent negative stock
            current_inventory['stock'] = current_inventory['stock'].clip(lower=0)

            # Save snapshot
            snapshot = current_inventory.copy()
            snapshot['date'] = date
            daily_records.append(snapshot)

        self.daily_inventory = pd.concat(daily_records)

    # ----------------------------
    # Add new sale
    # ----------------------------
    def add_sale(self, product_id, quantity):
        new_sale = pd.DataFrame([{
            'date': datetime.today(),
            'product_id': product_id,
            'quantity': quantity
        }])

        self.sales = pd.concat([self.sales, new_sale], ignore_index=True)
        self.sales.to_csv(self.sales_file, index=False)

    # ----------------------------
    # Add restock
    # ----------------------------
    def add_restock(self, product_id, quantity):
        new_stock = pd.DataFrame([{
            'date': datetime.today(),
            'product_id': product_id,
            'quantity': quantity
        }])

        self.restock = pd.concat([self.restock, new_stock], ignore_index=True)
        self.restock.to_csv(self.restock_file, index=False)

    # ----------------------------
    # Generate report
    # ----------------------------
    def generate_report(self):
        print("\n=== Current Inventory ===")
        print(self.inventory)

        print("\n=== Daily Inventory Levels ===")
        print(self.daily_inventory)

        print("\n=== Low Stock Alert (< 20) ===")
        low_stock = self.daily_inventory[self.daily_inventory['stock'] < 20]
        print(low_stock)

    # ----------------------------
    # Save updated inventory
    # ----------------------------
    def save_inventory(self):
        latest = self.daily_inventory.sort_values('date').groupby('product_id').last().reset_index()
        latest[['product_id', 'product_name', 'stock']].to_csv(self.inventory_file, index=False)


# ----------------------------
# Run the system
# ----------------------------
if __name__ == "__main__":
    system = InventorySystem(
        "/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/InventorySystemFull/inventory.csv",
        "/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/InventorySystemFull/sales.csv",
        "/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/InventorySystemFull/restock.csv"
    )

    # Process all transactions
    system.process_daily_inventory()

    # Add new sale example
    # system.add_sale(product_id=1, quantity=7)

    # Add restock example
    # system.add_restock(product_id=2, quantity=40)

    # Generate reports
    system.generate_report()

    # Save updated inventory
    system.save_inventory()
