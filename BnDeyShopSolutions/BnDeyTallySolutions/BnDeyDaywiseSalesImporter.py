from pymongo import MongoClient, ASCENDING
from pathlib import Path
import xml.etree.ElementTree as ET
from datetime import datetime
import re

# -----------------------------
# MongoDB Config
# -----------------------------
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bndey_db"
COLLECTION_NAME = "tally_daywise_sales"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# -----------------------------
# XML File
# -----------------------------
xml_file = "/Users/mehermeka/PycharmProjects/PythonProjectSelenium/BnDeyShopSolutions/BnDeyTallySolutions/DayBook.xml"

# -----------------------------
# Helpers
# -----------------------------
def clean_xml_text(path):
    raw = Path(path).read_bytes()

    try:
        text = raw.decode("utf-16le", errors="ignore")
    except:
        text = raw.decode("utf-8", errors="ignore")

    text = text.replace("&#4;", "")
    text = text.replace("\ufeff", "")
    return text


def parse_date(value):
    try:
        return datetime.strptime(value, "%Y%m%d").strftime("%Y-%m-%d")
    except:
        return value


def parse_number(value):
    if not value:
        return 0

    value = value.replace(",", "")
    match = re.search(r"-?\d+(\.\d+)?", value)

    if match:
        number = float(match.group())

        if number.is_integer():
            return int(number)

        return number

    return 0


def extract_size_ml(stock_item_name):
    match = re.search(
        r"(\d+)\s*-\s*ML",
        stock_item_name.upper()
    )
    return int(match.group(1)) if match else None


def extract_brand(stock_item_name):
    return re.sub(
        r"\s*\d+\s*-\s*ML.*$",
        "",
        stock_item_name,
        flags=re.IGNORECASE
    ).strip()


def get_batch_value(item, tag):
    batch = item.find(".//BATCHALLOCATIONS.LIST")
    if batch is not None:
        value = batch.findtext(tag)
        return value.strip() if value else ""
    return ""


# -----------------------------
# Read XML
# -----------------------------
xml_text = clean_xml_text(xml_file)
root = ET.fromstring(xml_text)

records = []

# -----------------------------
# Extract Sales Vouchers
# -----------------------------
for voucher in root.findall(".//VOUCHER"):

    voucher_type = voucher.findtext("VOUCHERTYPENAME", "").strip()

    # Import only Sales vouchers from DayBook
    if voucher_type.lower() != "sales":
        continue

    voucher_date_raw = voucher.findtext("DATE", "").strip()
    voucher_date = parse_date(voucher_date_raw)

    voucher_number = voucher.findtext("VOUCHERNUMBER", "").strip()
    guid = voucher.findtext("GUID", "").strip()
    party_name = voucher.findtext("PARTYNAME", "").strip()
    party_ledger_name = voucher.findtext("PARTYLEDGERNAME", "").strip()
    price_level = voucher.findtext("PRICELEVEL", "").strip()

    for item in voucher.findall(".//ALLINVENTORYENTRIES.LIST"):

        stock_item_name = item.findtext("STOCKITEMNAME", "").strip()

        if not stock_item_name:
            continue

        quantity = parse_number(item.findtext("ACTUALQTY", ""))
        billed_quantity = parse_number(item.findtext("BILLEDQTY", ""))
        rate = parse_number(item.findtext("RATE", ""))
        amount = parse_number(item.findtext("AMOUNT", ""))

        brand = extract_brand(stock_item_name)
        size_ml = extract_size_ml(stock_item_name)

        godown = get_batch_value(item, "GODOWNNAME")
        batch_name = get_batch_value(item, "BATCHNAME")
        destination_godown = get_batch_value(item, "DESTINATIONGODOWNNAME")

        record = {
            "voucher_date": voucher_date,
            "voucher_number": voucher_number,
            "guid": guid,
            "voucher_type": voucher_type,
            "party_name": party_name,
            "party_ledger_name": party_ledger_name,
            "price_level": price_level,
            "stock_item_name": stock_item_name,
            "brand": brand,
            "size_ml": size_ml,
            "quantity": quantity,
            "billed_quantity": billed_quantity,
            "rate": rate,
            "amount": amount,
            "unit": "PCS",
            "godown": godown,
            "destination_godown": destination_godown,
            "batch_name": batch_name,
            "source": "Tally DayBook XML",
            "uploaded_at": datetime.utcnow()
        }

        records.append(record)

print(f"Found {len(records)} daywise sales lines")

# -----------------------------
# Create Unique Index
# -----------------------------
collection.create_index(
    [
        ("guid", ASCENDING),
        ("stock_item_name", ASCENDING),
        ("rate", ASCENDING),
        ("quantity", ASCENDING),
        ("amount", ASCENDING)
    ],
    unique=True
)

# -----------------------------
# Insert Without Duplicates
# -----------------------------
inserted = 0
skipped = 0

for record in records:
    exists = collection.find_one({
        "guid": record["guid"],
        "stock_item_name": record["stock_item_name"],
        "rate": record["rate"],
        "quantity": record["quantity"],
        "amount": record["amount"]
    })

    if exists:
        skipped += 1
        continue

    collection.insert_one(record)
    inserted += 1

print(f"Inserted: {inserted}")
print(f"Skipped duplicates: {skipped}")
print("Import completed successfully")