from pymongo import MongoClient
from pathlib import Path
import xml.etree.ElementTree as ET
from datetime import datetime
import re

# MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["bndey_db"]
collection = db["tally_daily_sales"]

# XML File
xml_file = "/Users/mehermeka/PycharmProjects/PythonProjectSelenium/BnDeyShopSolutions/BnDeyTallySolutions/Sales_6.xml"

# Read UTF-16 Tally XML
xml_text = Path(xml_file).read_bytes().decode(
    "utf-16le",
    errors="ignore"
)

# Remove invalid Tally character references
xml_text = xml_text.replace("&#4;", "")

root = ET.fromstring(xml_text)

records = []

for voucher in root.findall(".//VOUCHER"):

    voucher_date = voucher.findtext("DATE", "")
    voucher_number = voucher.findtext("VOUCHERNUMBER", "")
    guid = voucher.findtext("GUID", "")
    party_name = voucher.findtext("PARTYNAME", "")

    try:
        voucher_date_iso = datetime.strptime(
            voucher_date,
            "%Y%m%d"
        ).strftime("%Y-%m-%d")
    except:
        voucher_date_iso = voucher_date

    for item in voucher.findall(".//ALLINVENTORYENTRIES.LIST"):

        stock_item_name = item.findtext("STOCKITEMNAME", "").strip()

        qty_text = item.findtext("ACTUALQTY", "")
        rate_text = item.findtext("RATE", "")
        amount_text = item.findtext("AMOUNT", "")

        # Quantity
        qty_match = re.search(r"([\d.]+)", qty_text)
        quantity = float(qty_match.group(1)) if qty_match else 0

        # Rate
        rate_match = re.search(r"([\d.]+)", rate_text)
        rate = float(rate_match.group(1)) if rate_match else 0

        # Amount
        try:
            amount = float(amount_text)
        except:
            amount = 0

        # Size ML
        size_match = re.search(r"(\d+)\s*-\s*ML", stock_item_name.upper())
        size_ml = int(size_match.group(1)) if size_match else None

        # Brand
        brand = re.sub(
            r"\s*\d+\s*-\s*ML.*$",
            "",
            stock_item_name,
            flags=re.IGNORECASE
        ).strip()

        record = {
            "voucher_date": voucher_date_iso,
            "voucher_number": voucher_number,
            "guid": guid,
            "party_name": party_name,
            "stock_item_name": stock_item_name,
            "brand": brand,
            "size_ml": size_ml,
            "quantity": quantity,
            "rate": rate,
            "amount": amount,
            "unit": "NOS",
            "source": "Tally Sales XML",
            "uploaded_at": datetime.utcnow()
        }

        records.append(record)

print(f"Found {len(records)} sales lines")

# Deduplicate using GUID + Stock Item
inserted = 0

for record in records:

    exists = collection.find_one({
        "guid": record["guid"],
        "stock_item_name": record["stock_item_name"]
    })

    if not exists:
        collection.insert_one(record)
        inserted += 1

print(f"Inserted {inserted} records")
print("Import Complete")