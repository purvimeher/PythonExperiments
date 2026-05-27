import re
import xml.etree.ElementTree as ET
from datetime import datetime
from pymongo import MongoClient, UpdateOne

XML_FILE = "/Users/mehermeka/PycharmProjects/PythonProjectSelenium/BnDeyShopSolutions/BnDeyTallySolutions/lateststock_items.xml"

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bndey_db"
COLLECTION_NAME = "tally_stock_items"


def parse_number(value):
    if not value:
        return 0

    value = (
        value.replace(",", "")
        .replace("NOS", "")
        .strip()
    )

    try:
        return float(value) if "." in value else int(value)
    except:
        return 0


def extract_size_ml(stock_name):
    match = re.search(r"(\d+)\s*-\s*ML", stock_name.upper())
    return int(match.group(1)) if match else None


def extract_brand(stock_name):
    return re.sub(
        r"\s+\d+\s*-\s*ML$",
        "",
        stock_name,
        flags=re.I
    ).strip()


def read_tally_stock_xml(xml_file):
    with open(xml_file, "rb") as f:
        xml_text = f.read().decode("utf-16", errors="ignore")

    root = ET.fromstring(xml_text)

    stock_items = []

    dspaccnames = root.findall("DSPACCNAME")
    dspstkinfo = root.findall("DSPSTKINFO")

    for name_node, stock_node in zip(dspaccnames, dspstkinfo):

        stock_name = name_node.findtext("DSPDISPNAME", default="").strip()

        if not stock_name:
            continue

        dspstop = stock_node.find("DSPSTKOP")

        if dspstop is None:
            continue

        qty = dspstop.findtext("DSPOPQTY", default="0")
        rate = dspstop.findtext("DSPOPRATE", default="0")
        amount = dspstop.findtext("DSPOPAMTA", default="0")

        item = {
            "stock_item_name": stock_name,
            "brand": extract_brand(stock_name),
            "size_ml": extract_size_ml(stock_name),
            "quantity": parse_number(qty),
            "rate": parse_number(rate),
            "amount": abs(parse_number(amount)),
            "unit": "NOS",
            "source": "Tally XML Stock Summary",
            "uploaded_at": datetime.now()
        }

        stock_items.append(item)

    return stock_items


def upload_stock_items(stock_items):
    client = MongoClient(MONGO_URI)

    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # Remove old wrong indexes if any
    try:
        collection.drop_index("Brand_1_Size_ML_1")
    except:
        pass

    # Create correct unique index
    collection.create_index(
        [
            ("stock_item_name", 1),
            ("size_ml", 1)
        ],
        unique=True,
        name="unique_stock_item",
        partialFilterExpression={
            "stock_item_name": {
                "$exists": True,
                "$type": "string"
            },
            "size_ml": {
                "$exists": True
            }
        }
    )

    operations = []

    for item in stock_items:

        operations.append(
            UpdateOne(
                {
                    "stock_item_name": item["stock_item_name"],
                    "size_ml": item["size_ml"]
                },
                {
                    "$set": item
                },
                upsert=True
            )
        )

    if operations:
        result = collection.bulk_write(
            operations,
            ordered=False
        )

        print("Upload completed")
        print("Total XML stock items:", len(stock_items))
        print("Inserted:", result.upserted_count)
        print("Updated:", result.modified_count)
        print("Matched:", result.matched_count)

    client.close()


if __name__ == "__main__":

    stock_items = read_tally_stock_xml(XML_FILE)

    print("Total stock items found:", len(stock_items))

    upload_stock_items(stock_items)