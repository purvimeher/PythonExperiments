from flask import Blueprint, render_template
from services.mongo_service import get_collection

daily_sales_bp = Blueprint(
    "daily_sales",
    __name__,
    url_prefix="/daily-sales"
)


@daily_sales_bp.route("/")
def show_daily_sales():
    collection = get_collection("daily_sales")
    records = list(collection.find().sort("Date", -1))

    for record in records:
        record["_id"] = str(record["_id"])

    return render_template("daily_sales.html", records=records)