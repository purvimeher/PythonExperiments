import re
import xml.etree.ElementTree as ET
from datetime import datetime
from pymongo import MongoClient, UpdateOne


XML_FILE = "/Users/mehermeka/PycharmProjects/PythonProjectSelenium/BnDeyShopSolutions/BnDeyTallySolutions/uptodatePricelist.xml"

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bndey_db"
COLLECTION_NAME = "stock_prices_tally"


def extract_size_ml(stock_name):
    match = re.search(r"(\d+)\s*-\s*ML", stock_name.upper())
    return int(match.group(1)) if match else None


def extract_brand(stock_name):
    return re.sub(r"\s+\d+\s*-\s*ML$", "", stock_name, flags=re.I).strip()


def parse_rate(rate_text):
    """
    Example:
    2114.00/NOS -> rate=2114.00, unit=NOS
    """
    if not rate_text:
        return 0, "NOS"

    parts = rate_text.strip().split("/")

    rate = float(parts[0].replace(",", "").strip())

    unit = parts[1].strip() if len(parts) > 1 else "NOS"

    return rate, unit


def parse_tally_date(date_text):
    """
    Example:
    1-Apr-26 -> datetime object
    """
    if not date_text:
        return None

    try:
        return datetime.strptime(date_text.strip(), "%d-%b-%y")
    except ValueError:
        return date_text


def read_stock_prices_from_xml(xml_file):
    with open(xml_file, "rb") as f:
        xml_text = f.read().decode("utf-16", errors="ignore")

    root = ET.fromstring(xml_text)

    stock_prices = []

    for stock_item in root.findall(".//STOCKITEM"):
        stock_name = stock_item.attrib.get("NAME", "").strip()

        if not stock_name:
            continue

        for price_list in stock_item.findall("FULLPRICELIST"):
            price_level = price_list.findtext("PRICELEVEL", default="").strip()
            price_date_text = price_list.findtext("DATE", default="").strip()

            price_level_list = price_list.find("PRICELEVELLIST")

            if price_level_list is None:
                continue

            rate_text = price_level_list.findtext("RATE", default="").strip()
            discount_text = price_level_list.findtext("DISCOUNT", default="").strip()
            starting_from = price_level_list.findtext("STARTINGFROM", default="").strip()
            ending_at = price_level_list.findtext("ENDINGAT", default="").strip()

            rate, unit = parse_rate(rate_text)

            item = {
                "stock_item_name": stock_name,
                "brand": extract_brand(stock_name),
                "size_ml": extract_size_ml(stock_name),
                "price_level": price_level,
                "rate": rate,
                "unit": unit,
                "price_date": parse_tally_date(price_date_text),
                "price_date_text": price_date_text,
                "starting_from": starting_from,
                "ending_at": ending_at,
                "discount": discount_text,
                "source": "Tally XML Price List",
                "uploaded_at": datetime.now()
            }

            stock_prices.append(item)

    return stock_prices


def upload_stock_prices_to_mongodb(stock_prices):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # Remove old wrong index if it exists
    try:
        collection.drop_index("Brand_1_Size_ML_1")
    except Exception:
        pass

    # Create correct unique index
    collection.create_index(
        [
            ("stock_item_name", 1),
            ("size_ml", 1),
            ("price_level", 1)
        ],
        unique=True,
        name="unique_stock_price"
    )

    operations = []

    for item in stock_prices:
        operations.append(
            UpdateOne(
                {
                    "stock_item_name": item["stock_item_name"],
                    "size_ml": item["size_ml"],
                    "price_level": item["price_level"]
                },
                {"$set": item},
                upsert=True
            )
        )

    if operations:
        result = collection.bulk_write(operations, ordered=False)

        print("Upload completed")
        print("Total XML prices:", len(stock_prices))
        print("Inserted:", result.upserted_count)
        print("Updated:", result.modified_count)
        print("Matched:", result.matched_count)

    client.close()

if __name__ == "__main__":
    prices = read_stock_prices_from_xml(XML_FILE)

    print("Total stock prices found:", len(prices))

    upload_stock_prices_to_mongodb(prices)