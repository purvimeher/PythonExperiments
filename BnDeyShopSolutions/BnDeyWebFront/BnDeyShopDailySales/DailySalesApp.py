from flask import Flask, render_template, request, Response
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from datetime import datetime
import csv
import io

app = Flask(__name__)

# -----------------------------
# MongoDB configuration
# -----------------------------
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bndey_db_Experimental"

DAILY_SALES_COLLECTION = "daily_sales"
STOCK_PRICES_COLLECTION = "stock_prices"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

daily_sales_col = db[DAILY_SALES_COLLECTION]
stock_prices_col = db[STOCK_PRICES_COLLECTION]


# -----------------------------
# Helpers
# -----------------------------
def parse_ddmmyyyy(date_str):
    """Convert DD/MM/YYYY string to datetime.date safely."""
    try:
        return datetime.strptime(date_str, "%d/%m/%Y")
    except (TypeError, ValueError):
        return None


def get_sorted_available_dates():
    """Get distinct dates sorted descending by actual date value."""
    raw_dates = daily_sales_col.distinct("Date")
    valid_dates = [d for d in raw_dates if parse_ddmmyyyy(d)]
    valid_dates.sort(key=lambda x: parse_ddmmyyyy(x), reverse=True)
    return valid_dates


def filter_dates_by_range(all_dates, from_date=None, to_date=None):
    """Filter existing string dates using parsed date range."""
    filtered = []
    from_dt = parse_ddmmyyyy(from_date) if from_date else None
    to_dt = parse_ddmmyyyy(to_date) if to_date else None

    for d in all_dates:
        current_dt = parse_ddmmyyyy(d)
        if not current_dt:
            continue

        if from_dt and current_dt < from_dt:
            continue
        if to_dt and current_dt > to_dt:
            continue
        filtered.append(d)

    return filtered


def build_lookup_and_amount_stages():
    """
    Reusable aggregation stages to:
    - lookup matching stock price
    - convert qty to int
    - compute line total
    """
    return [
        {
            "$lookup": {
                "from": STOCK_PRICES_COLLECTION,
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
                            "Maximum_Retail_Price_per_bottle": 1
                        }
                    }
                ],
                "as": "price_data"
            }
        },
        {
            "$unwind": {
                "path": "$price_data",
                "preserveNullAndEmptyArrays": True
            }
        },
        {
            "$addFields": {
                "qty_int": {
                    "$convert": {
                        "input": "$Qty",
                        "to": "int",
                        "onError": 0,
                        "onNull": 0
                    }
                },
                "unit_price": {
                    "$convert": {
                        "input": "$price_data.Maximum_Retail_Price_per_bottle",
                        "to": "double",
                        "onError": 0,
                        "onNull": 0
                    }
                }
            }
        },
        {
            "$addFields": {
                "line_total": {
                    "$multiply": ["$qty_int", "$unit_price"]
                }
            }
        }
    ]


def get_summary_all_dates(from_date=None, to_date=None):
    """
    Per-day summary across all dates within optional date range.
    Since Date is stored as string DD/MM/YYYY, range filtering is applied
    using Python after fetching available dates.
    """
    available_dates = get_sorted_available_dates()
    filtered_dates = filter_dates_by_range(available_dates, from_date, to_date)

    if not filtered_dates:
        return []

    pipeline = [
        {"$match": {"Date": {"$in": filtered_dates}}},
        *build_lookup_and_amount_stages(),
        {
            "$group": {
                "_id": "$Date",
                "total_qty": {"$sum": "$qty_int"},
                "total_amount": {"$sum": "$line_total"}
            }
        }
    ]

    raw_result = list(daily_sales_col.aggregate(pipeline))

    summary = []
    for doc in raw_result:
        summary.append({
            "date": doc["_id"],
            "total_qty": doc.get("total_qty", 0),
            "total_amount": round(doc.get("total_amount", 0), 2)
        })

    summary.sort(key=lambda x: parse_ddmmyyyy(x["date"]), reverse=True)
    return summary


def get_selected_date_totals(selected_date):
    """Grand totals for one selected date."""
    if not selected_date:
        return {"total_qty": 0, "total_amount": 0.0}

    pipeline = [
        {"$match": {"Date": selected_date}},
        *build_lookup_and_amount_stages(),
        {
            "$group": {
                "_id": None,
                "total_qty": {"$sum": "$qty_int"},
                "total_amount": {"$sum": "$line_total"}
            }
        }
    ]

    result = list(daily_sales_col.aggregate(pipeline))
    if not result:
        return {"total_qty": 0, "total_amount": 0.0}

    return {
        "total_qty": result[0].get("total_qty", 0),
        "total_amount": round(result[0].get("total_amount", 0), 2)
    }


def get_subtotal_by_category(selected_date):
    if not selected_date:
        return []

    pipeline = [
        {"$match": {"Date": selected_date}},
        *build_lookup_and_amount_stages(),
        {
            "$group": {
                "_id": "$Brand_Category",
                "total_qty": {"$sum": "$qty_int"},
                "total_amount": {"$sum": "$line_total"}
            }
        },
        {"$sort": {"_id": 1}}
    ]

    result = list(daily_sales_col.aggregate(pipeline))
    return [
        {
            "brand_category": doc["_id"] if doc["_id"] else "N/A",
            "total_qty": doc.get("total_qty", 0),
            "total_amount": round(doc.get("total_amount", 0), 2)
        }
        for doc in result
    ]


def get_subtotal_by_brand(selected_date):
    if not selected_date:
        return []

    pipeline = [
        {"$match": {"Date": selected_date}},
        *build_lookup_and_amount_stages(),
        {
            "$group": {
                "_id": "$Brand",
                "total_qty": {"$sum": "$qty_int"},
                "total_amount": {"$sum": "$line_total"}
            }
        },
        {"$sort": {"_id": 1}}
    ]

    result = list(daily_sales_col.aggregate(pipeline))
    return [
        {
            "brand": doc["_id"] if doc["_id"] else "N/A",
            "total_qty": doc.get("total_qty", 0),
            "total_amount": round(doc.get("total_amount", 0), 2)
        }
        for doc in result
    ]


def get_daily_rows(selected_date):
    if not selected_date:
        return []

    pipeline = [
        {"$match": {"Date": selected_date}},
        *build_lookup_and_amount_stages(),
        {
            "$project": {
                "_id": 0,
                "Date": 1,
                "Brand_Category": 1,
                "Brand": 1,
                "Size_ML": 1,
                "Qty": "$qty_int",
                "Unit_Price": "$unit_price",
                "Line_Total": "$line_total"
            }
        },
        {
            "$sort": {
                "Brand_Category": 1,
                "Brand": 1,
                "Size_ML": 1
            }
        }
    ]

    result = list(daily_sales_col.aggregate(pipeline))
    return [
        {
            "date": doc.get("Date", ""),
            "brand_category": doc.get("Brand_Category", ""),
            "brand": doc.get("Brand", ""),
            "size_ml": doc.get("Size_ML", ""),
            "qty": doc.get("Qty", 0),
            "unit_price": round(doc.get("Unit_Price", 0), 2),
            "line_total": round(doc.get("Line_Total", 0), 2)
        }
        for doc in result
    ]


def calculate_footer_totals(summary_rows):
    total_qty = sum(row.get("total_qty", 0) for row in summary_rows)
    total_amount = round(sum(row.get("total_amount", 0) for row in summary_rows), 2)
    return {
        "total_qty": total_qty,
        "total_amount": total_amount
    }


def generate_summary_csv(summary_rows):
    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow(["Date", "Total_Qty", "Total_Sales_Amount"])
    for row in summary_rows:
        writer.writerow([
            row["date"],
            row["total_qty"],
            row["total_amount"]
        ])

    return output.getvalue()


# -----------------------------
# Routes
# -----------------------------
@app.route("/", methods=["GET"])
def daily_sales_report():
    try:
        all_dates = get_sorted_available_dates()

        from_date = request.args.get("from_date", "").strip()
        to_date = request.args.get("to_date", "").strip()
        selected_date = request.args.get("date", "").strip()

        summary_all_dates = get_summary_all_dates(from_date, to_date)
        filtered_dates = [row["date"] for row in summary_all_dates]

        if not selected_date and filtered_dates:
            selected_date = filtered_dates[0]

        # If selected date not in current filtered range, reset to first available in range
        if selected_date and filtered_dates and selected_date not in filtered_dates:
            selected_date = filtered_dates[0]

        selected_totals = get_selected_date_totals(selected_date) if selected_date else {
            "total_qty": 0,
            "total_amount": 0.0
        }

        subtotal_by_category = get_subtotal_by_category(selected_date)
        subtotal_by_brand = get_subtotal_by_brand(selected_date)
        daily_rows = get_daily_rows(selected_date)

        footer_totals = calculate_footer_totals(summary_all_dates)

        chart_labels = [row["date"] for row in summary_all_dates]
        chart_amounts = [row["total_amount"] for row in summary_all_dates]

        return render_template(
            "daily_sales.html",
            all_dates=all_dates,
            filtered_dates=filtered_dates,
            selected_date=selected_date,
            from_date=from_date,
            to_date=to_date,
            summary_all_dates=summary_all_dates,
            selected_totals=selected_totals,
            subtotal_by_category=subtotal_by_category,
            subtotal_by_brand=subtotal_by_brand,
            daily_rows=daily_rows,
            footer_totals=footer_totals,
            chart_labels=chart_labels,
            chart_amounts=chart_amounts,
            error_message=None
        )

    except PyMongoError as exc:
        return render_template(
            "daily_sales.html",
            all_dates=[],
            filtered_dates=[],
            selected_date="",
            from_date="",
            to_date="",
            summary_all_dates=[],
            selected_totals={"total_qty": 0, "total_amount": 0.0},
            subtotal_by_category=[],
            subtotal_by_brand=[],
            daily_rows=[],
            footer_totals={"total_qty": 0, "total_amount": 0.0},
            chart_labels=[],
            chart_amounts=[],
            error_message=f"Database error: {str(exc)}"
        )
    except Exception as exc:
        return render_template(
            "daily_sales.html",
            all_dates=[],
            filtered_dates=[],
            selected_date="",
            from_date="",
            to_date="",
            summary_all_dates=[],
            selected_totals={"total_qty": 0, "total_amount": 0.0},
            subtotal_by_category=[],
            subtotal_by_brand=[],
            daily_rows=[],
            footer_totals={"total_qty": 0, "total_amount": 0.0},
            chart_labels=[],
            chart_amounts=[],
            error_message=f"Unexpected error: {str(exc)}"
        )


@app.route("/export-csv", methods=["GET"])
def export_csv():
    try:
        from_date = request.args.get("from_date", "").strip()
        to_date = request.args.get("to_date", "").strip()

        summary_rows = get_summary_all_dates(from_date, to_date)
        csv_data = generate_summary_csv(summary_rows)

        filename = "daily_sales_summary.csv"
        if from_date or to_date:
            filename = f"daily_sales_summary_{from_date or 'start'}_to_{to_date or 'end'}.csv"
            filename = filename.replace("/", "-")

        return Response(
            csv_data,
            mimetype="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )

    except Exception as exc:
        return Response(
            f"Error generating CSV: {str(exc)}",
            mimetype="text/plain",
            status=500
        )


if __name__ == "__main__":
    app.run(debug=True)