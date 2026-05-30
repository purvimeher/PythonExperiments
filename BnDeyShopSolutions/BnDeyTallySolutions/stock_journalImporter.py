import re
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from pymongo import MongoClient, UpdateOne

XML_FILE = "/Users/mehermeka/PycharmProjects/PythonProjectSelenium/BnDeyShopSolutions/BnDeyTallySolutions/Stock Journal_5.xml"

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bndey_db"
COLLECTION_NAME = "tally_stock_journal"


def clean_invalid_xml_chars(xml_text):
    return re.sub(r"&#(0?[0-8]|1[0-9]|2[0-9]|3[0-1]);", "", xml_text)


def get_text(parent, tag, default=None):
    node = parent.find(tag) if parent is not None else None
    return node.text.strip() if node is not None and node.text else default


def parse_rate(rate_text):
    if not rate_text:
        return None
    match = re.search(r"-?\d+(\.\d+)?", rate_text)
    return float(match.group()) if match else None


def parse_quantity(qty_text):
    if not qty_text:
        return 0
    match = re.search(r"-?\d+(\.\d+)?", qty_text)
    return int(float(match.group())) if match else 0


def parse_size_ml(stock_item_name):
    match = re.search(r"(\d+)\s*-\s*ML|\b(\d+)\s*ML\b", stock_item_name, re.IGNORECASE)
    if match:
        return int(match.group(1) or match.group(2))
    return None


def parse_tally_date(date_text):
    return datetime.strptime(date_text, "%Y%m%d")


def import_stock_journal():
    raw = Path(XML_FILE).read_bytes()

    # Tally XML is often UTF-16
    xml_text = raw.decode("utf-16")
    xml_text = clean_invalid_xml_chars(xml_text)

    root = ET.fromstring(xml_text)
    voucher = root.find(".//VOUCHER")

    if voucher is None:
        raise Exception("No VOUCHER found in XML")

    voucher_date_text = get_text(voucher, "DATE")
    voucher_date = parse_tally_date(voucher_date_text)

    voucher_number = get_text(voucher, "VOUCHERNUMBER")
    voucher_type = get_text(voucher, "VOUCHERTYPENAME")
    guid = get_text(voucher, "GUID")
    destination_godown = get_text(voucher, "DESTINATIONGODOWN")

    docs = []

    for entry in voucher.findall("INVENTORYENTRIESIN.LIST"):
        batch = entry.find("BATCHALLOCATIONS.LIST")

        stock_item_name = get_text(entry, "STOCKITEMNAME")
        rate = parse_rate(get_text(entry, "RATE"))
        quantity = parse_quantity(get_text(entry, "ACTUALQTY"))
        billed_quantity = parse_quantity(get_text(entry, "BILLEDQTY"))
        amount = float(get_text(entry, "AMOUNT", "0"))

        godown = get_text(batch, "GODOWNNAME")
        batch_name = get_text(batch, "BATCHNAME")

        doc = {
            "voucher_number": voucher_number,
            "voucher_type": voucher_type,
            "voucher_date": voucher_date,
            "voucher_date_text": voucher_date_text,
            "guid": guid,
            "destination_godown": destination_godown,

            "stock_item_name": stock_item_name,
            "size_ml": parse_size_ml(stock_item_name),
            "rate": rate,
            "quantity": quantity,
            "billed_quantity": billed_quantity,
            "unit": "NOS",
            "amount": amount,

            "godown": godown,
            "batch_name": batch_name,

            "source": "Tally XML Stock Journal",
            "uploaded_at": datetime.now()
        }

        docs.append(doc)

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # Prevent duplicate import of same voucher/item/godown/qty/amount
    operations = [
        UpdateOne(
            {
                "guid": doc["guid"],
                "voucher_number": doc["voucher_number"],
                "stock_item_name": doc["stock_item_name"],
                "godown": doc["godown"],
                "quantity": doc["quantity"],
                "rate": doc["rate"],
                "amount": doc["amount"],
            },
            {"$set": doc},
            upsert=True
        )
        for doc in docs
    ]

    if operations:
        result = collection.bulk_write(operations)
        print("Import completed")
        print("Records found in XML:", len(docs))
        print("Inserted:", result.upserted_count)
        print("Updated:", result.modified_count)
    else:
        print("No records found")


if __name__ == "__main__":
    import_stock_journal()