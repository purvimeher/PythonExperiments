from flask import (
    Flask, render_template, request, redirect,
    url_for, flash, send_file, jsonify
)
from pymongo import MongoClient, ASCENDING, UpdateOne
from bson import ObjectId
from io import BytesIO
from datetime import datetime
import pandas as pd

app = Flask(__name__)
app.secret_key = "change-this-secret-key"

# =========================
# MongoDB Configuration
# =========================
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bndey_db_Experimental"
COLLECTION_NAME = "current_inventory"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Helpful index for filtering/upsert
collection.create_index(
    [("Date", ASCENDING), ("Brand_Category", ASCENDING), ("Brand", ASCENDING), ("Size_ML", ASCENDING)],
    unique=True
)


# =========================
# Helper functions
# =========================
def safe_int(value, default=0):
    try:
        if pd.isna(value):
            return default
        return int(float(value))
    except Exception:
        return default


def dt_to_str(value):
    if isinstance(value, datetime):
        return value.strftime("%d/%m/%Y %H:%M:%S")
    return value if value is not None else ""


def serialize_doc(doc):
    return {
        "_id": str(doc.get("_id")),
        "Date": doc.get("Date", ""),
        "Brand_Category": doc.get("Brand_Category", ""),
        "Brand": doc.get("Brand", ""),
        "Size_ML": doc.get("Size_ML", ""),
        "Qty": doc.get("Qty", 0),
        "Last_Updated": dt_to_str(doc.get("Last_Updated")),
        "updated_at": dt_to_str(doc.get("updated_at"))
    }


def build_filter_query(args):
    query = {}

    brand_category = args.get("brand_category", "").strip()
    brand = args.get("brand", "").strip()
    size_ml = args.get("size_ml", "").strip()

    if brand_category:
        query["Brand_Category"] = brand_category
    if brand:
        query["Brand"] = brand
    if size_ml:
        try:
            query["Size_ML"] = int(size_ml)
        except ValueError:
            pass

    return query


def get_all_dropdowns():
    brand_categories = sorted([x for x in collection.distinct("Brand_Category") if x])
    brands = sorted([x for x in collection.distinct("Brand") if x])
    sizes = sorted([x for x in collection.distinct("Size_ML") if x is not None])
    return brand_categories, brands, sizes


def get_filtered_dropdowns(brand_category=None, brand=None):
    brand_query = {}
    size_query = {}

    if brand_category:
        brand_query["Brand_Category"] = brand_category
        size_query["Brand_Category"] = brand_category

    if brand:
        size_query["Brand"] = brand

    brands = sorted([x for x in collection.distinct("Brand", brand_query) if x])
    sizes = sorted([x for x in collection.distinct("Size_ML", size_query) if x is not None])

    return brands, sizes


# =========================
# Routes
# =========================
@app.route("/")
def index():
    query = build_filter_query(request.args)

    docs = list(
        collection.find(query).sort([
            ("Brand_Category", ASCENDING),
            ("Brand", ASCENDING),
            ("Size_ML", ASCENDING),
            ("Date", ASCENDING)
        ])
    )
    records = [serialize_doc(doc) for doc in docs]

    summary_pipeline = [
        {"$match": query},
        {
            "$group": {
                "_id": {
                    "Brand_Category": "$Brand_Category",
                    "Brand": "$Brand"
                },
                "total_qty": {"$sum": {"$ifNull": ["$Qty", 0]}}
            }
        },
        {
            "$sort": {
                "_id.Brand_Category": 1,
                "_id.Brand": 1
            }
        }
    ]

    summary_data = list(collection.aggregate(summary_pipeline))
    summary_rows = [
        {
            "Brand_Category": row["_id"].get("Brand_Category", ""),
            "Brand": row["_id"].get("Brand", ""),
            "total_qty": row.get("total_qty", 0)
        }
        for row in summary_data
    ]

    grand_total = sum(safe_int(r["Qty"]) for r in records)

    all_brand_categories, all_brands, all_sizes = get_all_dropdowns()

    selected_brand_category = request.args.get("brand_category", "").strip()
    selected_brand = request.args.get("brand", "").strip()
    selected_size_ml = request.args.get("size_ml", "").strip()

    filtered_brands, filtered_sizes = get_filtered_dropdowns(
        selected_brand_category, selected_brand
    )
    print(grand_total)
    return render_template(
        "index.html",
        records=records,
        summary_rows=summary_rows,
        grand_total=grand_total,
        brand_categories=all_brand_categories,
        brands=filtered_brands if selected_brand_category else all_brands,
        sizes=filtered_sizes if (selected_brand_category or selected_brand) else all_sizes,
        selected_brand_category=selected_brand_category,
        selected_brand=selected_brand,
        selected_size_ml=selected_size_ml
    )


@app.route("/dropdown-data")
def dropdown_data():
    brand_category = request.args.get("brand_category", "").strip()
    brand = request.args.get("brand", "").strip()
    brands, sizes = get_filtered_dropdowns(brand_category, brand)
    return jsonify({"brands": brands, "sizes": sizes})


@app.route("/create", methods=["GET", "POST"])
def create_record():
    if request.method == "POST":
        date_val = request.form.get("Date", "").strip()
        brand_category = request.form.get("Brand_Category", "").strip()
        brand = request.form.get("Brand", "").strip()
        size_ml = safe_int(request.form.get("Size_ML", 0))
        qty = safe_int(request.form.get("Qty", 0))

        if not all([date_val, brand_category, brand, size_ml]):
            flash("Date, Brand Category, Brand and Size_ML are required.", "danger")
            return redirect(url_for("create_record"))

        now = datetime.utcnow()

        filter_query = {
            "Date": date_val,
            "Brand_Category": brand_category,
            "Brand": brand,
            "Size_ML": size_ml
        }

        update_doc = {
            "$set": {
                "Qty": qty,
                "Last_Updated": now,
                "updated_at": now
            },
            "$setOnInsert": {
                "Date": date_val,
                "Brand_Category": brand_category,
                "Brand": brand,
                "Size_ML": size_ml
            }
        }

        collection.update_one(filter_query, update_doc, upsert=True)
        flash("Record saved successfully.", "success")
        return redirect(url_for("index"))

    return render_template("form.html", mode="create", record={})


@app.route("/edit/<record_id>", methods=["GET", "POST"])
def edit_record(record_id):
    doc = collection.find_one({"_id": ObjectId(record_id)})
    if not doc:
        flash("Record not found.", "danger")
        return redirect(url_for("index"))

    if request.method == "POST":
        date_val = request.form.get("Date", "").strip()
        brand_category = request.form.get("Brand_Category", "").strip()
        brand = request.form.get("Brand", "").strip()
        size_ml = safe_int(request.form.get("Size_ML", 0))
        qty = safe_int(request.form.get("Qty", 0))

        if not all([date_val, brand_category, brand, size_ml]):
            flash("Date, Brand Category, Brand and Size_ML are required.", "danger")
            return redirect(url_for("edit_record", record_id=record_id))

        now = datetime.utcnow()

        collection.update_one(
            {"_id": ObjectId(record_id)},
            {
                "$set": {
                    "Date": date_val,
                    "Brand_Category": brand_category,
                    "Brand": brand,
                    "Size_ML": size_ml,
                    "Qty": qty,
                    "Last_Updated": now,
                    "updated_at": now
                }
            }
        )

        flash("Record updated successfully.", "success")
        return redirect(url_for("index"))

    return render_template("form.html", mode="edit", record=serialize_doc(doc))


@app.route("/delete/<record_id>", methods=["POST"])
def delete_record(record_id):
    result = collection.delete_one({"_id": ObjectId(record_id)})
    if result.deleted_count:
        flash("Record deleted successfully.", "success")
    else:
        flash("Record not found.", "danger")
    return redirect(url_for("index"))


@app.route("/import", methods=["GET", "POST"])
def import_csv():
    if request.method == "POST":
        if "file" not in request.files:
            flash("No file selected.", "danger")
            return redirect(url_for("import_csv"))

        file = request.files["file"]
        if file.filename == "":
            flash("Please choose a CSV file.", "danger")
            return redirect(url_for("import_csv"))

        try:
            df = pd.read_csv(file)

            required_columns = ["Date", "Brand_Category", "Brand", "Size_ML", "Qty"]
            missing = [col for col in required_columns if col not in df.columns]
            if missing:
                flash(f"Missing columns: {', '.join(missing)}", "danger")
                return redirect(url_for("import_csv"))

            ops = []
            now = datetime.utcnow()

            for _, row in df.iterrows():
                date_val = str(row.get("Date", "")).strip()
                brand_category = str(row.get("Brand_Category", "")).strip()
                brand = str(row.get("Brand", "")).strip()
                size_ml = safe_int(row.get("Size_ML", 0))
                qty = safe_int(row.get("Qty", 0))

                if not all([date_val, brand_category, brand, size_ml]):
                    continue

                filter_query = {
                    "Date": date_val,
                    "Brand_Category": brand_category,
                    "Brand": brand,
                    "Size_ML": size_ml
                }

                update_doc = {
                    "$set": {
                        "Qty": qty,
                        "Last_Updated": now,
                        "updated_at": now
                    },
                    "$setOnInsert": {
                        "Date": date_val,
                        "Brand_Category": brand_category,
                        "Brand": brand,
                        "Size_ML": size_ml
                    }
                }

                ops.append(UpdateOne(filter_query, update_doc, upsert=True))

            if ops:
                collection.bulk_write(ops, ordered=False)
                flash("CSV imported successfully.", "success")
            else:
                flash("No valid rows found in CSV.", "warning")

        except Exception as e:
            flash(f"Import failed: {str(e)}", "danger")

        return redirect(url_for("index"))

    return render_template("import.html")


@app.route("/export")
def export_csv():
    query = build_filter_query(request.args)

    docs = list(
        collection.find(query).sort([
            ("Brand_Category", ASCENDING),
            ("Brand", ASCENDING),
            ("Size_ML", ASCENDING),
            ("Date", ASCENDING)
        ])
    )

    rows = []
    for doc in docs:
        rows.append({
            "Date": doc.get("Date", ""),
            "Brand_Category": doc.get("Brand_Category", ""),
            "Brand": doc.get("Brand", ""),
            "Size_ML": doc.get("Size_ML", ""),
            "Qty": doc.get("Qty", 0),
            "Last_Updated": dt_to_str(doc.get("Last_Updated")),
            "updated_at": dt_to_str(doc.get("updated_at"))
        })

    df = pd.DataFrame(rows)
    output = BytesIO()
    df.to_csv(output, index=False)
    output.seek(0)

    return send_file(
        output,
        mimetype="text/csv",
        as_attachment=True,
        download_name="current_inventory_export.csv"
    )


if __name__ == "__main__":
    app.run()