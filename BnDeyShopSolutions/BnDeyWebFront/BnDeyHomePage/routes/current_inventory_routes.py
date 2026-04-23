from flask import Blueprint, render_template
from services.mongo_service import get_collection

current_inventory_bp = Blueprint(
    "current_inventory",
    __name__,
    url_prefix="/current-inventory"
)


@current_inventory_bp.route("/")
def show_current_inventory():
    collection = get_collection("current_inventory")
    records = list(collection.find().sort("Brand_Category", 1))

    for record in records:
        record["_id"] = str(record["_id"])

    return render_template("current_inventory.html", records=records)