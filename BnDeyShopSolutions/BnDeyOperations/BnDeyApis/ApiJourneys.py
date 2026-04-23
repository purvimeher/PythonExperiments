import csv
import os
from datetime import datetime
import re
from typing import Optional

import pymongo
from fastapi import Query
from flasgger import Swagger
from flask import Flask, jsonify, request, send_file


connection_url = "mongodb://localhost:27017"
db_name = "bndey_db"
stock_prices_collection = "stock_prices"
daily_sales_collection = "daily_sales"
current_inventory_collection = "current_inventory"
app = Flask(__name__)
client = pymongo.MongoClient(connection_url)
# Swagger config
app.config["SWAGGER"] = {
    "title": "Stock In Hand API",
    "uiversion": 3
}

swagger_template = {
    "swagger": "2.0",
    "info": {
        "title": "Stock In Hand API",
        "description": "API to fetch stock in hand from current_inventory and export CSV files",
        "version": "1.0.0"
    },
    "basePath": "/"
}

Swagger(app, template=swagger_template)


# Database
Database = client.get_database(db_name)
# Table
stock_prices_collection_db = Database[stock_prices_collection]
daily_sales_collection_db = Database[daily_sales_collection]
current_inventory= Database[current_inventory_collection]
OUTPUT_FOLDER = '/Users/mehermeka/PycharmProjects/PythonProjectSelenium/BnDeyShopSolutions/BnDeyOperations/output/Total_Stock/'
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# http://127.0.0.1:5000/stock-prices/all
@app.route('/stock-prices/all', methods=['GET'])
def get_all_stock_prices():
    query = stock_prices_collection_db.find()
    output = {}
    i = 0
    for x in query:
        output[i] = x
        output[i].pop('_id')
        i += 1
    return jsonify(output)


# http://127.0.0.1:5000/sales/all/
@app.route('/sales/all', methods=['GET'])
def get_all_daily_sales():
    query = daily_sales_collection_db.find()
    output = {}
    i = 0
    for x in query:
        output[i] = x
        output[i].pop('_id')
        i += 1
    return jsonify(output)


# http://127.0.0.1:5000/sales/by-date?date=07/02/2026
@app.route('/sales/by-date', methods=['GET'])
def get_daily_sales_by_date():
    try:
        date = request.args.get("date")
        if not date:
            return jsonify({
                "status": "error",
                "message": "date is required"
            }), 400
        query = {
            "Date": date
        }
        result = list(daily_sales_collection_db.find(query, {"_id": 0}))

        if not result:
            return jsonify({
                "status": "error",
                "message": "Sale record/s not found"
            }), 404

        return jsonify({
            "status": "success",
            "data": result
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


# http://127.0.0.1:5000/stock-prices/by-size?size_ml=750&brand=OFFICERS%20CHOICE%20PRESTIGE%20WHISKY&Brand_category=Deluxe%20Prestige%20Brand
@app.route("/stock-prices", methods=["GET"])
def get_stock_price_by_brand_by_size_by_category():
    try:
        brand = request.args.get("brand")
        size_ml = request.args.get("size_ml")
        brand_category = request.args.get("brand_category")

        if not brand or not size_ml or not brand_category:
            return jsonify({
                "status": "error",
                "message": "brand, size_ml, and brand_category are required"
            }), 400

        query = {
            "Brand": brand,
            "Size_ML": int(size_ml),
            "Brand_Category": brand_category
        }

        result = stock_prices_collection_db.find_one(query, {"_id": 0})

        if not result:
            return jsonify({
                "status": "error",
                "message": "Stock price record not found"
            }), 404

        return jsonify({
            "status": "success",
            "data": result
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


# http://127.0.0.1:5000/stock-prices/by-size?size_ml=750
@app.route("/stock-prices/by-size", methods=["GET"])
def get_stock_prices_by_size():
    try:

        size_ml = request.args.get("size_ml")

        if not size_ml:
            return jsonify({
                "status": "error",
                "message": "size_ml is required"
            }), 400

        query = {"Size_ML": int(size_ml)}
        records = list(stock_prices_collection_db.find(query, {"_id": 0}))

        if not records:
            return jsonify({
                "status": "error",
                "message": "No records found"
            }), 404

        return jsonify({
            "status": "success",
            "count": len(records),
            "data": records
        })

    except Exception as e:

        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

# GET http://localhost:5000/stock-prices/search?brand=OFFICERS
@app.route("/stock-prices/search", methods=["GET"])
def search_by_brand():
    """
    Search stock prices where Brand contains value.
    Example:
    /stock-prices/search?brand=OFFICERS
    """

    try:
        brand_value = request.args.get("brand")

        if not brand_value:
            return jsonify({
                "status": "error",
                "message": "brand parameter is required"
            }), 400

        # Case-insensitive partial match
        query = {
            "Brand": {
                "$regex": re.compile(brand_value, re.IGNORECASE)
            }
        }

        results = list(stock_prices_collection_db.find(query, {"_id": 0}))

        return jsonify({
            "status": "success",
            "count": len(results),
            "data": results
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


def parse_date(date_str):
    return datetime.strptime(date_str, "%d/%m/%Y")


def build_match_filter(start_date=None, end_date=None, brand=None, brand_category=None, size_ml=None):
    match_filter = {}

    if start_date or end_date:
        date_filter = {}

        if start_date:
            parse_date(start_date)  # validate format
            date_filter["$gte"] = start_date

        if end_date:
            parse_date(end_date)  # validate format
            date_filter["$lte"] = end_date

        match_filter["Date"] = date_filter

    if brand:
        match_filter["Brand"] = brand

    if brand_category:
        match_filter["Brand_Category"] = brand_category

    if size_ml:
        try:
            match_filter["Size_ML"] = int(size_ml)
        except ValueError:
            raise ValueError("size_ml must be an integer")

    return match_filter

# http://127.0.0.1:5000/api/sales-summary?start_date=09/04/2026&end_date=11/04/2026
@app.route("/api/sales-summary", methods=["GET"])
def get_sales_summary():
    try:
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")
        brand = request.args.get("brand")
        brand_category = request.args.get("brand_category")
        size_ml = request.args.get("size_ml")

        if not start_date and not end_date:
            return jsonify({
                "status": "error",
                "message": "Please provide start_date and/or end_date in DD/MM/YYYY format"
            }), 400

        match_filter = build_match_filter(
            start_date=start_date,
            end_date=end_date,
            brand=brand,
            brand_category=brand_category,
            size_ml=size_ml
        )

        pipeline = [
            {"$match": match_filter},
            {
                "$lookup": {
                    "from": "stock_prices",
                    "let": {
                        "brand": "$Brand",
                        "brand_category": "$Brand_Category",
                        "size_ml": "$Size_ML"
                    },
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {"$eq": ["$Brand", "$$brand"]},
                                        {"$eq": ["$Brand_Category", "$$brand_category"]},
                                        {"$eq": ["$Size_ML", "$$size_ml"]}
                                    ]
                                }
                            }
                        },
                        {
                            "$project": {
                                "_id": 0,
                                "Maximum_Retail_Price_per_bottle": 1,
                                "Maximum_Retail_Price_per_case": 1,
                                "Maximum_Retail_Price_per_bottle_OLD": 1
                            }
                        }
                    ],
                    "as": "price_info"
                }
            },
            {
                "$unwind": {
                    "path": "$price_info",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$group": {
                    "_id": {
                        "Date": "$Date",
                        "Brand_Category": "$Brand_Category",
                        "Brand": "$Brand",
                        "Size_ML": "$Size_ML"
                    },
                    "total_quantity_sold": {"$sum": "$Qty"},
                    "price_per_bottle": {"$first": "$price_info.Maximum_Retail_Price_per_bottle"},
                    "price_per_case": {"$first": "$price_info.Maximum_Retail_Price_per_case"},
                    "old_price_per_bottle": {"$first": "$price_info.Maximum_Retail_Price_per_bottle_OLD"}
                }
            },
            {
                "$addFields": {
                    "total_sales": {
                        "$multiply": [
                            "$total_quantity_sold",
                            {"$ifNull": ["$price_per_bottle", 0]}
                        ]
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "Date": "$_id.Date",
                    "Brand_Category": "$_id.Brand_Category",
                    "Brand": "$_id.Brand",
                    "Size_ML": "$_id.Size_ML",
                    "Qty": "$total_quantity_sold",
                    "price_per_bottle": {"$ifNull": ["$price_per_bottle", 0]},
                    # "price_per_case": {"$ifNull": ["$price_per_case", 0]},
                    # "old_price_per_bottle": {"$ifNull": ["$old_price_per_bottle", 0]},
                    "total_sales": 1
                }
            },
            {
                "$sort": {
                    "Brand_Category": 1,
                    "Brand": 1,
                    "Size_ML": 1,
                    "Date": 1
                }
            }
        ]

        detail_rows = list(daily_sales_collection_db.aggregate(pipeline))

        subtotal_map = {}
        for row in detail_rows:
            key = (
                row["Brand_Category"],
                row["Brand"],
                row["Size_ML"]
            )

            if key not in subtotal_map:
                subtotal_map[key] = {
                    "Brand_Category": row["Brand_Category"],
                    "Brand": row["Brand"],
                    "Size_ML": row["Size_ML"],
                    "subtotal_quantity_sold": 0,
                    "price_per_bottle": row.get("price_per_bottle", 0),
                    # "price_per_case": row.get("price_per_case", 0),
                    # "old_price_per_bottle": row.get("old_price_per_bottle", 0),
                    "subtotal_sales": 0
                }

            subtotal_map[key]["subtotal_quantity_sold"] += row.get("Qty", 0)
            subtotal_map[key]["subtotal_sales"] += row.get("total_sales", 0)

        subtotals = sorted(
            list(subtotal_map.values()),
            key=lambda x: (x["Brand_Category"], x["Brand"], x["Size_ML"])
        )

        grand_total_qty = sum(item.get("Qty", 0) for item in detail_rows)
        grand_total_sales = sum(item.get("total_sales", 0) for item in detail_rows)

        return jsonify({
            "status": "success",
            "filters": {
                "start_date": start_date,
                "end_date": end_date,
                "brand": brand,
                "brand_category": brand_category,
                "size_ml": size_ml
            },
            "grand_totals": {
                "grand_total_quantity_sold": grand_total_qty,
                "grand_total_sales": grand_total_sales
            },
            "subtotals_by_brand_brand_category_size_ml": subtotals,
            "details_by_date": detail_rows
        }), 200

    except ValueError as ve:
        return jsonify({
            "status": "error",
            "message": str(ve)
        }), 400

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


# http://127.0.0.1:5000/api/sales-summary-csv?start_date=01/04/2026&end_date=09/04/2026
@app.route("/api/sales-summary-csv", methods=["GET"])
def generate_sales_csv():
    """
    Generate sales summary CSV by date range
    ---
    tags:
      - Sales Reports
    produces:
      - text/csv
      - application/json
    parameters:
      - name: start_date
        in: query
        type: string
        required: true
        description: Start date in DD/MM/YYYY format
        example: 01/04/2026
      - name: end_date
        in: query
        type: string
        required: true
        description: End date in DD/MM/YYYY format
        example: 09/04/2026
      - name: brand
        in: query
        type: string
        required: false
        description: Optional brand filter
        example: OFFICERS CHOICE PRESTIGE WHISKY
      - name: brand_category
        in: query
        type: string
        required: false
        description: Optional brand category filter
        example: Deluxe Prestige Brand
      - name: size_ml
        in: query
        type: integer
        required: false
        description: Optional size filter
        example: 1000
    responses:
      200:
        description: CSV file generated successfully
        schema:
          type: file
      400:
        description: Invalid request parameters
        schema:
          type: object
          properties:
            status:
              type: string
              example: error
            message:
              type: string
              example: start_date and end_date required
      500:
        description: Internal server error
        schema:
          type: object
          properties:
            status:
              type: string
              example: error
            message:
              type: string
              example: Some unexpected error occurred
    """
    try:

        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")

        if not start_date or not end_date:
            return jsonify({
                "status": "error",
                "message": "start_date and end_date required"
            }), 400

        start_dt = parse_date(start_date)
        end_dt = parse_date(end_date)

        pipeline = [

            {
                "$addFields": {
                    "parsed_date": {
                        "$dateFromString": {
                            "dateString": "$Date",
                            "format": "%d/%m/%Y"
                        }
                    }
                }
            },

            {
                "$match": {
                    "parsed_date": {
                        "$gte": start_dt,
                        "$lte": end_dt
                    }
                }
            },

            {
                "$lookup": {
                    "from": "stock_prices",
                    "let": {
                        "brand": "$Brand",
                        "brand_category": "$Brand_Category",
                        "size_ml": "$Size_ML"
                    },
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {"$eq": ["$Brand", "$$brand"]},
                                        {"$eq": ["$Brand_Category", "$$brand_category"]},
                                        {"$eq": ["$Size_ML", "$$size_ml"]}
                                    ]
                                }
                            }
                        }
                    ],
                    "as": "price_info"
                }
            },

            {
                "$unwind": {
                    "path": "$price_info",
                    "preserveNullAndEmptyArrays": True
                }
            },

            {
                "$group": {
                    "_id": {
                        "Date": "$Date",
                        "Brand_Category": "$Brand_Category",
                        "Brand": "$Brand",
                        "Size_ML": "$Size_ML"
                    },
                    "Qty": {"$sum": "$Qty"},
                    "price_per_bottle": {
                        "$first": "$price_info.Maximum_Retail_Price_per_bottle"
                    },
                    "price_per_case": {
                        "$first": "$price_info.Maximum_Retail_Price_per_case"
                    },
                    "old_price_per_bottle": {
                        "$first": "$price_info.Maximum_Retail_Price_per_bottle_OLD"
                    }
                }
            },

            {
                "$addFields": {
                    "total_sales": {
                        "$multiply": [
                            "$Qty",
                            {"$ifNull": ["$price_per_bottle", 0]}
                        ]
                    }
                }
            }

        ]

        results = list(daily_sales_collection_db.aggregate(pipeline))

        # -----------------------
        # Prepare CSV
        # -----------------------

        filename = f"sales_summary_{start_date.replace('/','-')}_{end_date.replace('/','-')}.csv"
        filepath = os.path.join(OUTPUT_FOLDER, filename)

        subtotal_map = {}

        grand_total_qty = 0
        grand_total_sales = 0

        with open(filepath, mode="w", newline="") as file:

            writer = csv.writer(file)

            writer.writerow([
                "Date",
                "Brand_Category",
                "Brand",
                "Size_ML",
                "Qty",
                "Price_per_bottle",
                "Price_per_case",
                "Old_price",
                "Total_sales"
            ])

            for row in results:

                writer.writerow([
                    row["_id"]["Date"],
                    row["_id"]["Brand_Category"],
                    row["_id"]["Brand"],
                    row["_id"]["Size_ML"],
                    row["Qty"],
                    row.get("price_per_bottle", 0),
                    row.get("price_per_case", 0),
                    row.get("old_price_per_bottle", 0),
                    row["total_sales"]
                ])

                key = (
                    row["_id"]["Brand_Category"],
                    row["_id"]["Brand"],
                    row["_id"]["Size_ML"]
                )

                subtotal_map.setdefault(key, {
                    "qty": 0,
                    "sales": 0
                })

                subtotal_map[key]["qty"] += row["Qty"]
                subtotal_map[key]["sales"] += row["total_sales"]

                grand_total_qty += row["Qty"]
                grand_total_sales += row["total_sales"]

            writer.writerow([])
            writer.writerow(["SUBTOTALS"])

            for key, val in subtotal_map.items():

                writer.writerow([
                    "",
                    key[0],
                    key[1],
                    key[2],
                    val["qty"],
                    "",
                    "",
                    "",
                    val["sales"]
                ])

            writer.writerow([])
            writer.writerow([
                "GRAND TOTAL",
                "",
                "",
                "",
                grand_total_qty,
                "",
                "",
                "",
                grand_total_sales
            ])

        return send_file(
            filepath,
            as_attachment=True
        )

    except Exception as e:

        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500



def build_match_filter_current_Inventory(date: Optional[str] = None, brand: Optional[str] = None, brand_category: Optional[str] = None, size_ml: Optional[int] = None):
    match_filter = {}

    if date:
        match_filter["Date"] = date

    if brand:
        match_filter["Brand"] = {"$regex": brand, "$options": "i"}

    if brand_category:
        match_filter["Brand_Category"] = {"$regex": brand_category, "$options": "i"}

    if size_ml is not None:
        match_filter["Size_ML"] = int(size_ml)

    return match_filter
@app.get("/stock-in-hand")
def get_stock_in_hand():
    """
    Get stock in hand
    ---
    tags:
      - Stock In Hand
    parameters:
      - name: date
        in: query
        type: string
        required: false
        description: Filter by exact date in DD/MM/YYYY
      - name: from_date
        in: query
        type: string
        required: false
        description: Start date in DD/MM/YYYY
      - name: to_date
        in: query
        type: string
        required: false
        description: End date in DD/MM/YYYY
      - name: brand
        in: query
        type: string
        required: false
        description: Filter by brand name
      - name: brand_category
        in: query
        type: string
        required: false
        description: Filter by brand category
      - name: size_ml
        in: query
        type: integer
        required: false
        description: Filter by bottle size in ML
    responses:
      200:
        description: Stock in hand fetched successfully
        schema:
          type: object
          properties:
            status:
              type: string
              example: success
            total_groups:
              type: integer
              example: 1
            grand_total_stock_in_hand:
              type: integer
              example: 10
            data:
              type: array
              items:
                type: object
                properties:
                  Date:
                    type: string
                    example: 09/04/2026
                  Brand_Category:
                    type: string
                    example: Deluxe Prestige Brand
                  Brand:
                    type: string
                    example: OFFICERS CHOICE PRESTIGE WHISKY
                  Size_ML:
                    type: integer
                    example: 1000
                  stock_in_hand:
                    type: integer
                    example: 10
    """
    try:
        date = request.args.get("date")
        brand = request.args.get("brand")
        brand_category = request.args.get("brand_category")
        size_ml = request.args.get("size_ml")


        match_filter = build_match_filter_current_Inventory(
            date=date,
            brand=brand,
            brand_category=brand_category,
            size_ml=size_ml
        )

        pipeline = []

        if match_filter:
            pipeline.append({"$match": match_filter})

        pipeline.extend([
            {
                "$group": {
                    "_id": {
                        "Date": "$Date",
                        "Brand_Category": "$Brand_Category",
                        "Brand": "$Brand",
                        "Size_ML": "$Size_ML"
                    },
                    "stock_in_hand": {"$sum": "$Qty"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "Date": "$_id.Date",
                    "Brand_Category": "$_id.Brand_Category",
                    "Brand": "$_id.Brand",
                    "Size_ML": "$_id.Size_ML",
                    "stock_in_hand": 1
                }
            },
            {
                "$sort": {
                    "Date": 1,
                    "Brand_Category": 1,
                    "Brand": 1,
                    "Size_ML": 1
                }
            }
        ])

        results = list(current_inventory.aggregate(pipeline))
        grand_total_stock = sum(item["stock_in_hand"] for item in results)

        return {
            "status": "success",
            "message": "Stock in hand calculated successfully",
            "filters": {
                "date": date,
                "brand": brand,
                "brand_category": brand_category,
                "size_ml": size_ml
            },
            "total_groups": len(results),
            "grand_total_stock_in_hand": grand_total_stock,
            "data": results
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


# -----------------------------
# Aggregation
# -----------------------------
def get_stock_data():

    match_filter = build_match_filter()

    pipeline = []

    if match_filter:
        pipeline.append({
            "$match": match_filter
        })

    pipeline.extend([

        {
            "$group": {
                "_id": {
                    "Date": "$Date",
                    "Brand_Category": "$Brand_Category",
                    "Brand": "$Brand",
                    "Size_ML": "$Size_ML"
                },
                "stock_in_hand": {
                    "$sum": "$Qty"
                }
            }
        },

        {
            "$project": {
                "_id": 0,
                "Date": "$_id.Date",
                "Brand_Category": "$_id.Brand_Category",
                "Brand": "$_id.Brand",
                "Size_ML": "$_id.Size_ML",
                "stock_in_hand": 1
            }
        },

        {
            "$sort": {
                "Date": 1,
                "Brand_Category": 1,
                "Brand": 1
            }
        }

    ])

    results = list(
        current_inventory.aggregate(pipeline)
    )

    grand_total = sum(
        row["stock_in_hand"]
        for row in results
    )

    return results, grand_total



# -----------------------------
# JSON API
# -----------------------------http://127.0.0.1:5000/stock-in-hand/date-range?from_date=01/01/2026&to_date=09/04/2026
# http://127.0.0.1:5000/stock-in-hand/date-range?from_date=01/01/2026&to_date=09/04/2026
@app.route("/stock-in-hand/date-range", methods=["GET"])
def stock_date_range():

    try:

        results, grand_total = get_stock_data()

        return jsonify({

            "status": "success",

            "total_records": len(results),

            "grand_total_stock_in_hand": grand_total,

            "data": results

        })

    except Exception as e:

        return jsonify({

            "status": "error",

            "message": str(e)

        })


# -----------------------------
# CSV Export API
# -----------------------------
# http://127.0.0.1:5000/stock-in-hand/date-range/csv?from_date=01/01/2026&to_date=09/04/2026
@app.route("/stock-in-hand/date-range/csv", methods=["GET"])
def stock_date_range_csv():
    """
    Export stock in hand CSV
    ---
    tags:
      - Stock In Hand
    parameters:
      - name: date
        in: query
        type: string
        required: false
        description: Filter by exact date in DD/MM/YYYY
      - name: from_date
        in: query
        type: string
        required: false
        description: Start date in DD/MM/YYYY
      - name: to_date
        in: query
        type: string
        required: false
        description: End date in DD/MM/YYYY
      - name: brand
        in: query
        type: string
        required: false
        description: Filter by brand name
      - name: brand_category
        in: query
        type: string
        required: false
        description: Filter by brand category
      - name: size_ml
        in: query
        type: integer
        required: false
        description: Filter by bottle size in ML
    responses:
      200:
        description: CSV file download
        schema:
          type: string
          format: binary
    """
    try:

        results, grand_total = get_stock_data()

        from_date = request.args.get("from_date")
        to_date = request.args.get("to_date")

        filename = f"stock_{from_date}_to_{to_date}.csv"
        filename = filename.replace("/", "_")

        file_path = os.path.join(
            OUTPUT_FOLDER,
            filename
        )

        with open(
            file_path,
            "w",
            newline="",
            encoding="utf-8"
        ) as file:

            writer = csv.writer(file)

            writer.writerow([
                "Date",
                "Brand_Category",
                "Brand",
                "Size_ML",
                "Stock_In_Hand"
            ])

            for row in results:

                writer.writerow([
                    row["Date"],
                    row["Brand_Category"],
                    row["Brand"],
                    row["Size_ML"],
                    row["stock_in_hand"]
                ])

            writer.writerow([])
            writer.writerow([
                "Grand Total",
                grand_total
            ])

        return send_file(
            file_path,
            as_attachment=True
        )

    except Exception as e:

        return jsonify({

            "status": "error",

            "message": str(e)

        })


if __name__ == '__main__':
    # app.run(host='127.0.0.1', port=5000, debug=True)
    app.run()
