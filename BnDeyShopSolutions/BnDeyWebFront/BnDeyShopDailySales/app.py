from flask import Flask, request, render_template, jsonify
from pymongo import MongoClient

app = Flask(__name__)

# MongoDB connection
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bndey_db"
COLLECTION_NAME = "stock_prices"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]


def serialize_mongo_doc(doc):
    return {
        # "_id": str(doc.get("_id", "")),
        "Sl_No": doc.get("Sl_No", ""),
        "Brand_Category": doc.get("Brand_Category", ""),
        "Brand": doc.get("Brand", ""),
        "Size_ML": doc.get("Size_ML", ""),
        # "LookColumn": doc.get("LookColumn", ""),
        "Maximum_Retail_Price_per_bottle": doc.get("Maximum_Retail_Price_per_bottle", 0),
        # "Maximum_Retail_Price_per_bottle_OLD": doc.get("Maximum_Retail_Price_per_bottle_OLD", 0),
        "Maximum_Retail_Price_per_case": doc.get("Maximum_Retail_Price_per_case", 0),
    }
    """
    Convert MongoDB document into JSON/html-friendly format.
    """


@app.route("/")
def home():
    return """
    <h2>Stock Prices Application</h2>
    <p><a href='/stock-prices'>View Stock Prices in HTML Table</a></p>
    <p><a href='/api/stock-prices'>View Stock Prices JSON API</a></p>
    """


@app.route("/stock-prices")
def stock_prices_html():
    records = list(collection.find())
    stock_prices = [serialize_mongo_doc(doc) for doc in records]
    return render_template("stock_prices.html", stock_prices=stock_prices)


@app.route("/api/stock-prices")
def stock_prices_api():
    records = list(collection.find())
    stock_prices = [serialize_mongo_doc(doc) for doc in records]

    return jsonify({
        "status": "success",
        "count": len(stock_prices),
        "data": stock_prices
    })

@app.route("/stock-prices-search")
def stock_prices_search_html():
    brand = request.args.get("brand", "").strip()

    query = {}
    if brand:
        query["Brand"] = {"$regex": brand, "$options": "i"}   # case-insensitive contains

    records = list(collection.find(query).sort("Sl_No", 1))
    stock_prices = [serialize_mongo_doc_search(doc) for doc in records]

    return render_template(
        "stock_prices_search.html",
        stock_prices=stock_prices,
        selected_brand=brand
    )

def serialize_mongo_doc_search(doc):
    return {
        "_id": str(doc.get("_id", "")),
        "Sl_No": doc.get("Sl_No", ""),
        "Brand_Category": doc.get("Brand_Category", ""),
        "Brand": doc.get("Brand", ""),
        "Size_ML": doc.get("Size_ML", ""),
        "LookColumn": doc.get("LookColumn", ""),
        "Maximum_Retail_Price_per_bottle": doc.get("Maximum_Retail_Price_per_bottle", 0),
        "Maximum_Retail_Price_per_bottle_OLD": doc.get("Maximum_Retail_Price_per_bottle_OLD", 0),
        "Maximum_Retail_Price_per_case": doc.get("Maximum_Retail_Price_per_case", 0),
    }

@app.route("/api/stock-prices-search")
def stock_prices_search_api():
    brand = request.args.get("brand", "").strip()

    query = {}
    if brand:
        query["Brand"] = {"$regex": brand, "$options": "i"}   # case-insensitive contains

    records = list(collection.find(query).sort("Sl_No", 1))
    stock_prices = [serialize_mongo_doc_search(doc) for doc in records]

    return jsonify({
        "status": "success",
        "count": len(stock_prices),
        "filters": {
            "brand": brand
        },
        "data": stock_prices
    })

if __name__ == '__main__':
    app.run(debug=True)