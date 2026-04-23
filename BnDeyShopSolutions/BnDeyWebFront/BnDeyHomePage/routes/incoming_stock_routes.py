from flask import Blueprint, render_template
from services.mongo_service import get_collection

incoming_stock_bp = Blueprint(
    "incoming_stock",
    __name__,
    url_prefix="/incoming-stock"
)


@incoming_stock_bp.route("/")
def show_incoming_stock():
    collection = get_collection("incoming_stock")
    records = list(collection.find().sort("Brand_Category", 1))

    for record in records:
        record["_id"] = str(record["_id"])

    return render_template("incoming_stock.html", records=records)