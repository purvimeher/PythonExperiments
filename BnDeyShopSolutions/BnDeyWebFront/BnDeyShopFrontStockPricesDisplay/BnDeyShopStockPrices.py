from flask import Flask, render_template, request, jsonify
from pymongo import MongoClient, ASCENDING

app = Flask(__name__)

# MongoDB config
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bndey_db_Experimental"
COLLECTION_NAME = "stock_prices"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]


def build_exact_match_query(args):
    query = {}

    brand = args.get("brand", "").strip()
    brand_category = args.get("brand_category", "").strip()
    size_ml = args.get("size_ml", "").strip()
    # price = args.get("price", "").strip()
    # old_price = args.get("old_price", "").strip()
    # case_price = args.get("case_price", "").strip()

    if brand:
        query["Brand"] = brand

    if brand_category:
        query["Brand_Category"] = brand_category

    if size_ml:
        try:
            query["Size_ML"] = int(size_ml)
        except ValueError:
            raise ValueError("size_ml must be an integer")

    # if price:
    #     try:
    #         query["Maximum_Retail_Price_per_bottle"] = int(price)
    #     except ValueError:
    #         raise ValueError("price must be a number")
    #
    # if old_price:
    #     try:
    #         query["Maximum_Retail_Price_per_bottle_OLD"] = int(old_price)
    #     except ValueError:
    #         raise ValueError("old_price must be a number")
    #
    # if case_price:
    #     try:
    #         query["Maximum_Retail_Price_per_case"] = int(case_price)
    #     except ValueError:
    #         raise ValueError("case_price must be a number")

    return query


def get_all_dropdown_values():
    brands = sorted([x for x in collection.distinct("Brand") if x is not None])
    brand_categories = sorted([x for x in collection.distinct("Brand_Category") if x is not None])
    size_mls = sorted([x for x in collection.distinct("Size_ML") if x is not None])
    # prices = sorted([x for x in collection.distinct("Maximum_Retail_Price_per_bottle") if x is not None])
    # old_prices = sorted([x for x in collection.distinct("Maximum_Retail_Price_per_bottle_OLD") if x is not None])
    # case_prices = sorted([x for x in collection.distinct("Maximum_Retail_Price_per_case") if x is not None])

    return {
        "brands": brands,
        "brand_categories": brand_categories,
        "size_mls": size_mls
        # "prices": prices,
        # "old_prices": old_prices,
        # "case_prices": case_prices
    }


def get_filtered_brands_by_category(brand_category):
    mongo_query = {}
    if brand_category:
        mongo_query["Brand_Category"] = brand_category

    brands = sorted([x for x in collection.distinct("Brand", mongo_query) if x is not None])

    return brands


def get_filtered_sizes(brand_category=None, brand=None):
    mongo_query = {}

    if brand_category:
        mongo_query["Brand_Category"] = brand_category

    if brand:
        mongo_query["Brand"] = brand

    sizes = sorted([x for x in collection.distinct("Size_ML", mongo_query) if x is not None])
    return sizes


@app.route("/")
def show_stock_prices():
    try:
        query = build_exact_match_query(request.args)
        records = list(collection.find(query))

        for record in records:
            record["_id"] = str(record["_id"])
        # Sort by serial number

        filters = {
            "brand": request.args.get("brand", ""),
            "brand_category": request.args.get("brand_category", ""),
            "size_ml": request.args.get("size_ml", "")
            # "price": request.args.get("price", ""),
            # "old_price": request.args.get("old_price", ""),
            # "case_price": request.args.get("case_price", "")
        }

        dropdowns = get_all_dropdown_values()

        # preload dependent dropdowns for already-selected values
        selected_category = filters["brand_category"]
        selected_brand = filters["brand"]

        if selected_category:
            dropdowns["brands"] = get_filtered_brands_by_category(selected_category)

        if selected_category or selected_brand:
            dropdowns["size_mls"] = get_filtered_sizes(
                brand_category=selected_category if selected_category else None,
                brand=selected_brand if selected_brand else None
            )

        return render_template(
            "stock_prices.html",
            records=records,
            filters=filters,
            dropdowns=dropdowns
        )

    except Exception as e:
        return f"Error fetching stock prices: {str(e)}", 500


@app.route("/api/stock-prices", methods=["GET"])
def stock_prices_api():
    try:
        query = build_exact_match_query(request.args)
        records = list(collection.find(query))

        for record in records:
            record["_id"] = str(record["_id"])

        return jsonify({
            "status": "success",
            "count": len(records),
            "filters_applied": query,
            "data": records
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@app.route("/api/filter-options", methods=["GET"])
def filter_options_api():
    """
    Dynamic dropdown API:
    - category selected => return matching brands
    - brand selected => return matching sizes
    - category + brand selected => return matching sizes
    """
    try:
        brand_category = request.args.get("brand_category", "").strip()
        brand = request.args.get("brand", "").strip()

        brands = get_filtered_brands_by_category(brand_category if brand_category else None)
        sizes = get_filtered_sizes(
            brand_category=brand_category if brand_category else None,
            brand=brand if brand else None
        )

        return jsonify({
            "status": "success",
            "brands": brands,
            "sizes": sizes
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


if __name__ == "__main__":
    app.run()