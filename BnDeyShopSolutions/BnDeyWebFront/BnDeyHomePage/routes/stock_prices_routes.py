from flask import Blueprint, render_template, request, jsonify
from services.mongo_service import get_collection

stock_prices_bp = Blueprint(
    "stock_prices",
    __name__,
    url_prefix="/stock-prices"
)

collection = get_collection("stock_prices")


def build_exact_match_query(args):
    query = {}

    brand = args.get("brand", "").strip()
    brand_category = args.get("brand_category", "").strip()
    size_ml = args.get("size_ml", "").strip()

    if brand:
        query["Brand"] = brand

    if brand_category:
        query["Brand_Category"] = brand_category

    if size_ml:
        try:
            query["Size_ML"] = int(size_ml)
        except ValueError:
            raise ValueError("size_ml must be an integer")

    return query


def get_all_dropdown_values():
    brands = sorted([x for x in collection.distinct("Brand") if x is not None])
    brand_categories = sorted([x for x in collection.distinct("Brand_Category") if x is not None])
    size_mls = sorted([x for x in collection.distinct("Size_ML") if x is not None])

    return {
        "brands": brands,
        "brand_categories": brand_categories,
        "size_mls": size_mls
    }


def get_filtered_brands_by_category(brand_category):
    mongo_query = {}

    if brand_category:
        mongo_query["Brand_Category"] = brand_category

    return sorted([x for x in collection.distinct("Brand", mongo_query) if x is not None])


def get_filtered_sizes(brand_category=None, brand=None):
    mongo_query = {}

    if brand_category:
        mongo_query["Brand_Category"] = brand_category

    if brand:
        mongo_query["Brand"] = brand

    return sorted([x for x in collection.distinct("Size_ML", mongo_query) if x is not None])


@stock_prices_bp.route("/")
def show_stock_prices():
    try:
        query = build_exact_match_query(request.args)
        records = list(collection.find(query).sort("Sl_No", 1))

        for record in records:
            record["_id"] = str(record["_id"])

        filters = {
            "brand": request.args.get("brand", ""),
            "brand_category": request.args.get("brand_category", ""),
            "size_ml": request.args.get("size_ml", "")
        }

        dropdowns = get_all_dropdown_values()

        selected_category = filters["brand_category"]
        selected_brand = filters["brand"]

        if selected_category:
            dropdowns["brands"] = get_filtered_brands_by_category(selected_category)

        if selected_category or selected_brand:
            dropdowns["size_mls"] = get_filtered_sizes(
                brand_category=selected_category or None,
                brand=selected_brand or None
            )

        return render_template(
            "stock_prices.html",
            records=records,
            filters=filters,
            dropdowns=dropdowns
        )

    except Exception as e:
        return f"Error fetching stock prices: {str(e)}", 500


@stock_prices_bp.route("/api", methods=["GET"])
def stock_prices_api():
    try:
        query = build_exact_match_query(request.args)
        records = list(collection.find(query).sort("Sl_No", 1))

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


@stock_prices_bp.route("/api/filter-options", methods=["GET"])
def filter_options_api():
    try:
        brand_category = request.args.get("brand_category", "").strip()
        brand = request.args.get("brand", "").strip()

        brands = get_filtered_brands_by_category(brand_category or None)
        sizes = get_filtered_sizes(
            brand_category=brand_category or None,
            brand=brand or None
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