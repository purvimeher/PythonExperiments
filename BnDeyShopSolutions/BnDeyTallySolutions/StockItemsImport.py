import re
import xml.etree.ElementTree as ET
from pymongo import MongoClient, UpdateOne
from datetime import datetime

XML_FILE = "/Users/mehermeka/PycharmProjects/PythonProjectSelenium/BnDeyShopSolutions/BnDeyTallySolutions/OutStockstck.xml"

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bndey_db"
COLLECTION_NAME = "tally_stock_items"


def parse_number(value):
    if not value:
        return 0
    value = value.replace(",", "").replace("NOS", "").strip()
    return float(value) if "." in value else int(value)


def extract_size_ml(stock_name):
    match = re.search(r"(\d+)\s*-\s*ML", stock_name.upper())
    return int(match.group(1)) if match else None


def extract_brand(stock_name):
    return re.sub(r"\s+\d+\s*-\s*ML$", "", stock_name, flags=re.I).strip()


def read_tally_xml(xml_file):
    # Your XML is UTF-16 LE from Tally
    with open(xml_file, "rb") as f:
        xml_text = f.read().decode("utf-16le", errors="ignore")

    root = ET.fromstring(xml_text)

    stock_items = []

    children = list(root)

    for i in range(0, len(children), 2):
        name_node = children[i].find("DSPDISPNAME")
        stock_node = children[i + 1].find("DSPSTKCL")

        if name_node is None or stock_node is None:
            continue

        stock_name = name_node.text.strip()

        qty = stock_node.findtext("DSPCLQTY", default="0")
        rate = stock_node.findtext("DSPCLRATE", default="0")
        amount = stock_node.findtext("DSPCLAMTA", default="0")

        item = {
            "stock_item_name": stock_name,
            "brand": extract_brand(stock_name),
            "size_ml": extract_size_ml(stock_name),
            "quantity": parse_number(qty),
            "unit": "NOS",
            "rate": parse_number(rate),
            "amount": parse_number(amount),
            "source": "Tally XML Stock Summary",
            "uploaded_at": datetime.now()
        }

        stock_items.append(item)

    return stock_items


def upload_to_mongodb(stock_items):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    operations = []

    for item in stock_items:
        operations.append(
            UpdateOne(
                {
                    "stock_item_name": item["stock_item_name"],
                    "size_ml": item["size_ml"]
                },
                {"$set": item},
                upsert=True
            )
        )

    if operations:
        result = collection.bulk_write(operations)
        print("Upload completed")
        print("Inserted:", result.upserted_count)
        print("Updated:", result.modified_count)
        print("Matched:", result.matched_count)

    client.close()


if __name__ == "__main__":
    stock_items = read_tally_xml(XML_FILE)

    print("Total stock items found:", len(stock_items))

    upload_to_mongodb(stock_items)