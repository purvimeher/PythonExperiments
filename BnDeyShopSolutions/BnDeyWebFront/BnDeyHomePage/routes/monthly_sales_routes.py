from flask import Blueprint, render_template
from services.mongo_service import get_collection

monthly_sales_bp = Blueprint(
    "monthly_sales",
    __name__,
    url_prefix="/monthly-sales"
)


@monthly_sales_bp.route("/")
def show_monthly_sales():
    collection = get_collection("monthly_sales")
    records = list(collection.find().sort("YearMonth", -1))

    for record in records:
        record["_id"] = str(record["_id"])

    return render_template("monthly_sales.html", records=records)