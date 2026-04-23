from flasgger import Swagger
from flask import Flask, jsonify, request
from pymongo import MongoClient
from datetime import datetime

app = Flask(__name__)
# ---------------------------------
# Swagger Configuration
# ---------------------------------
app.config["SWAGGER"] = {
    "title": "Sales Report API",
    "uiversion": 3
}

swagger_template = {
    "swagger": "2.0",
    "info": {
        "title": "Sales Report API",
        "description": "API for weekly and monthly liquor sales reports using daily_sales and stock_prices collections",
        "version": "1.0.0"
    },
    "basePath": "/",
    "schemes": [
        "http",
        "https"
    ]
}

swagger = Swagger(app, template=swagger_template)

# -----------------------------
# MongoDB Configuration
# -----------------------------
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bndey_db"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

daily_sales_collection = db["daily_sales"]


# -----------------------------
# Helpers
# -----------------------------
def parse_date(date_str):
    return datetime.strptime(date_str, "%d/%m/%Y")


def build_common_pipeline():
    """
    Builds common aggregation stages:
    - convert Date string to actual date
    - apply stock_prices lookup
    - compute unit_price and total_sale_price
    """

    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    brand = request.args.get("brand")
    brand_category = request.args.get("brand_category")
    size_ml = request.args.get("size_ml")

    pipeline = []

    # Convert string date to real date
    pipeline.append({
        "$addFields": {
            "parsed_date": {
                "$dateFromString": {
                    "dateString": "$Date",
                    "format": "%d/%m/%Y"
                }
            }
        }
    })

    match_conditions = {}

    # Date filters
    if start_date or end_date:
        date_filter = {}
        if start_date:
            date_filter["$gte"] = parse_date(start_date)
        if end_date:
            date_filter["$lte"] = parse_date(end_date)
        match_conditions["parsed_date"] = date_filter

    # Other filters
    if brand:
        match_conditions["Brand"] = brand

    if brand_category:
        match_conditions["Brand_Category"] = brand_category

    if size_ml:
        match_conditions["Size_ML"] = int(size_ml)

    if match_conditions:
        pipeline.append({"$match": match_conditions})

    # Join with stock_prices
    pipeline.append({
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
    })

    pipeline.append({
        "$unwind": {
            "path": "$price_info",
            "preserveNullAndEmptyArrays": True
        }
    })

    pipeline.append({
        "$addFields": {
            "unit_price": {
                "$ifNull": ["$price_info.Maximum_Retail_Price_per_bottle", 0]
            },
            "total_sale_price": {
                "$multiply": [
                    {"$ifNull": ["$Qty", 0]},
                    {"$ifNull": ["$price_info.Maximum_Retail_Price_per_bottle", 0]}
                ]
            }
        }
    })

    return pipeline


# -----------------------------
# Weekly Report Endpoint
# -----------------------------
@app.route("/api/sales/weekly", methods=["GET"])
def weekly_sales_report():
    """
    Weekly sales summary
    ---
    tags:
      - Weekly Reports
    parameters:
      - name: start_date
        in: query
        type: string
        required: false
        example: 01/04/2026
        description: Start date in DD/MM/YYYY format
      - name: end_date
        in: query
        type: string
        required: false
        example: 30/04/2026
        description: End date in DD/MM/YYYY format
      - name: brand
        in: query
        type: string
        required: false
        example: OFFICERS CHOICE PRESTIGE WHISKY
      - name: brand_category
        in: query
        type: string
        required: false
        example: Deluxe Prestige Brand
      - name: size_ml
        in: query
        type: integer
        required: false
        example: 1000
    responses:
      200:
        description: Weekly sales report
        schema:
          type: object
          properties:
            status:
              type: string
              example: success
            report_type:
              type: string
              example: weekly
            data:
              type: array
              items:
                type: object
                properties:
                  year:
                    type: integer
                    example: 2026
                  week:
                    type: integer
                    example: 15
                  total_quantity_sold:
                    type: integer
                    example: 20
                  total_sales_amount:
                    type: number
                    example: 7520
      400:
        description: Invalid date or input
      500:
        description: Server error
    """
    try:
        pipeline = build_common_pipeline()

        pipeline.extend([
            {
                "$addFields": {
                    "week_number": {"$isoWeek": "$parsed_date"},
                    "week_year": {"$isoWeekYear": "$parsed_date"}
                }
            },
            {
                "$group": {
                    "_id": {
                        "year": "$week_year",
                        "week": "$week_number"
                    },
                    "total_quantity_sold": {"$sum": "$Qty"},
                    "total_sales_amount": {"$sum": "$total_sale_price"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "year": "$_id.year",
                    "week": "$_id.week",
                    "total_quantity_sold": 1,
                    "total_sales_amount": 1
                }
            },
            {
                "$sort": {
                    "year": 1,
                    "week": 1
                }
            }
        ])

        result = list(daily_sales_collection.aggregate(pipeline))

        return jsonify({
            "status": "success",
            "report_type": "weekly",
            "filters": {
                "start_date": request.args.get("start_date"),
                "end_date": request.args.get("end_date"),
                "brand": request.args.get("brand"),
                "brand_category": request.args.get("brand_category"),
                "size_ml": request.args.get("size_ml")
            },
            "data": result
        }), 200

    except ValueError:
        return jsonify({
            "status": "error",
            "message": "Invalid date or size_ml format. Use DD/MM/YYYY and numeric size_ml."
        }), 400

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


# -----------------------------
# Monthly Report Endpoint
# -----------------------------
@app.route("/api/sales/monthly", methods=["GET"])
def monthly_sales_report():
    """
    Monthly sales summary
    ---
    tags:
      - Monthly Reports
    parameters:
      - name: start_date
        in: query
        type: string
        required: false
        example: 01/04/2026
      - name: end_date
        in: query
        type: string
        required: false
        example: 30/04/2026
      - name: brand
        in: query
        type: string
        required: false
      - name: brand_category
        in: query
        type: string
        required: false
      - name: size_ml
        in: query
        type: integer
        required: false
    responses:
      200:
        description: Monthly sales report
        schema:
          type: object
          properties:
            status:
              type: string
              example: success
            report_type:
              type: string
              example: monthly
            data:
              type: array
              items:
                type: object
                properties:
                  year:
                    type: integer
                    example: 2026
                  month:
                    type: integer
                    example: 4
                  total_quantity_sold:
                    type: integer
                    example: 100
                  total_sales_amount:
                    type: number
                    example: 37600
      400:
        description: Invalid date or input
      500:
        description: Server error
    """
    try:
        pipeline = build_common_pipeline()

        pipeline.extend([
            {
                "$addFields": {
                    "month_number": {"$month": "$parsed_date"},
                    "month_year": {"$year": "$parsed_date"}
                }
            },
            {
                "$group": {
                    "_id": {
                        "year": "$month_year",
                        "month": "$month_number"
                    },
                    "total_quantity_sold": {"$sum": "$Qty"},
                    "total_sales_amount": {"$sum": "$total_sale_price"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "year": "$_id.year",
                    "month": "$_id.month",
                    "total_quantity_sold": 1,
                    "total_sales_amount": 1
                }
            },
            {
                "$sort": {
                    "year": 1,
                    "month": 1
                }
            }
        ])

        result = list(daily_sales_collection.aggregate(pipeline))

        return jsonify({
            "status": "success",
            "report_type": "monthly",
            "filters": {
                "start_date": request.args.get("start_date"),
                "end_date": request.args.get("end_date"),
                "brand": request.args.get("brand"),
                "brand_category": request.args.get("brand_category"),
                "size_ml": request.args.get("size_ml")
            },
            "data": result
        }), 200

    except ValueError:
        return jsonify({
            "status": "error",
            "message": "Invalid date or size_ml format. Use DD/MM/YYYY and numeric size_ml."
        }), 400

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


# -----------------------------
# Weekly grouped by product
# -----------------------------
@app.route("/api/sales/weekly/by-product", methods=["GET"])
def weekly_sales_by_product():
    """
    Weekly sales grouped by product
    ---
    tags:
      - Weekly Reports
    parameters:
      - name: start_date
        in: query
        type: string
        required: false
      - name: end_date
        in: query
        type: string
        required: false
      - name: brand
        in: query
        type: string
        required: false
      - name: brand_category
        in: query
        type: string
        required: false
      - name: size_ml
        in: query
        type: integer
        required: false
    responses:
      200:
        description: Weekly grouped sales report
      500:
        description: Server error
    """
    try:
        pipeline = build_common_pipeline()

        pipeline.extend([
            {
                "$addFields": {
                    "week_number": {"$isoWeek": "$parsed_date"},
                    "week_year": {"$isoWeekYear": "$parsed_date"}
                }
            },
            {
                "$group": {
                    "_id": {
                        "year": "$week_year",
                        "week": "$week_number",
                        "Brand": "$Brand",
                        "Brand_Category": "$Brand_Category",
                        "Size_ML": "$Size_ML"
                    },
                    "total_quantity_sold": {"$sum": "$Qty"},
                    "total_sale_price": {"$sum": "$total_sale_price"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "year": "$_id.year",
                    "week": "$_id.week",
                    "Brand": "$_id.Brand",
                    "Brand_Category": "$_id.Brand_Category",
                    "Size_ML": "$_id.Size_ML",
                    "total_quantity_sold": 1,
                    "total_sale_price": 1
                }
            },
            {
                "$sort": {
                    "year": 1,
                    "week": 1,
                    "Brand_Category": 1,
                    "Brand": 1,
                    "Size_ML": 1
                }
            }
        ])

        result = list(daily_sales_collection.aggregate(pipeline))

        return jsonify({
            "status": "success",
            "report_type": "weekly_by_product",
            "data": result
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


# -----------------------------
# Monthly grouped by product
# -----------------------------
@app.route("/api/sales/monthly/by-product", methods=["GET"])
def monthly_sales_by_product():
    """
    Monthly sales grouped by product
    ---
    tags:
      - Monthly Reports
    parameters:
      - name: start_date
        in: query
        type: string
        required: false
      - name: end_date
        in: query
        type: string
        required: false
      - name: brand
        in: query
        type: string
        required: false
      - name: brand_category
        in: query
        type: string
        required: false
      - name: size_ml
        in: query
        type: integer
        required: false
    responses:
      200:
        description: Monthly grouped sales report
      500:
        description: Server error
    """
    try:
        pipeline = build_common_pipeline()

        pipeline.extend([
            {
                "$addFields": {
                    "month_number": {"$month": "$parsed_date"},
                    "month_year": {"$year": "$parsed_date"}
                }
            },
            {
                "$group": {
                    "_id": {
                        "year": "$month_year",
                        "month": "$month_number",
                        "Brand": "$Brand",
                        "Brand_Category": "$Brand_Category",
                        "Size_ML": "$Size_ML"
                    },
                    "total_quantity_sold": {"$sum": "$Qty"},
                    "total_sale_price": {"$sum": "$total_sale_price"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "year": "$_id.year",
                    "month": "$_id.month",
                    "Brand": "$_id.Brand",
                    "Brand_Category": "$_id.Brand_Category",
                    "Size_ML": "$_id.Size_ML",
                    "total_quantity_sold": 1,
                    "total_sale_price": 1
                }
            },
            {
                "$sort": {
                    "year": 1,
                    "month": 1,
                    "Brand_Category": 1,
                    "Brand": 1,
                    "Size_ML": 1
                }
            }
        ])

        result = list(daily_sales_collection.aggregate(pipeline))

        return jsonify({
            "status": "success",
            "report_type": "monthly_by_product",
            "data": result
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


if __name__ == "__main__":
    app.run()