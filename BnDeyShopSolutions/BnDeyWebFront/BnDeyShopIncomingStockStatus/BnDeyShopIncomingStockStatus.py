import csv
import io
from datetime import datetime, time

from bson import ObjectId
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    jsonify,
    send_file,
)
from pymongo import MongoClient, ASCENDING, UpdateOne

app = Flask(__name__)
app.secret_key = "your_secret_key_here"

# -----------------------------------
# MongoDB config
# -----------------------------------
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bndey_db_Experimental"
COLLECTION_NAME = "incoming_stock"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# -----------------------------------
# Indexes
# -----------------------------------
collection.create_index(
    [("Brand_Category", ASCENDING), ("Brand", ASCENDING), ("Size_ML", ASCENDING), ("Date", ASCENDING)]
)

# Optional unique index for duplicate handling
# Uncomment if you want strict uniqueness
# collection.create_index(
#     [("Brand_Category", 1), ("Brand", 1), ("Size_ML", 1), ("Date", 1)],
#     unique=True
# )


# -----------------------------------
# Helpers
# -----------------------------------
def safe_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def parse_ddmmyyyy(date_str):
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str.strip(), "%d/%m/%Y")
    except ValueError:
        return None


def mongo_date_range_query(from_date_str, to_date_str):
    """
    Date field is stored as string like dd/mm/yyyy.
    We convert range filtering into matching strings by fetching candidate data in Python.
    If you want high performance date filtering, store a real ISO date field in Mongo.
    """
    from_dt = parse_ddmmyyyy(from_date_str) if from_date_str else None
    to_dt = parse_ddmmyyyy(to_date_str) if to_date_str else None
    return from_dt, to_dt


def record_matches_date_range(record, from_dt, to_dt):
    record_dt = parse_ddmmyyyy(record.get("Date", ""))
    if not record_dt:
        return False
    if from_dt and record_dt < from_dt:
        return False
    if to_dt and record_dt > to_dt:
        return False
    return True


def serialize_doc(doc):
    return {
        "_id": str(doc.get("_id")),
        "Brand": doc.get("Brand", ""),
        "Size_ML": doc.get("Size_ML", ""),
        "Brand_Category": doc.get("Brand_Category", ""),
        "Date": doc.get("Date", ""),
        "Qty": doc.get("Qty", 0),
        "created_at": doc.get("created_at", ""),
        "updated_at": doc.get("updated_at", ""),
    }


def build_base_query(brand_category="", brand="", size_ml=""):
    query = {}
    if brand_category:
        query["Brand_Category"] = brand_category
    if brand:
        query["Brand"] = brand
    if size_ml:
        query["Size_ML"] = safe_int(size_ml)
    return query


def get_filtered_records(brand_category="", brand="", size_ml="", from_date="", to_date=""):
    query = build_base_query(brand_category, brand, size_ml)
    docs = list(
        collection.find(query).sort(
            [
                ("Brand_Category", ASCENDING),
                ("Brand", ASCENDING),
                ("Size_ML", ASCENDING),
                ("Date", ASCENDING),
            ]
        )
    )

    from_dt, to_dt = mongo_date_range_query(from_date, to_date)
    if from_dt or to_dt:
        docs = [doc for doc in docs if record_matches_date_range(doc, from_dt, to_dt)]

    return docs


def build_summary(docs):
    summary = {}
    grand_total = 0

    for doc in docs:
        category = doc.get("Brand_Category", "Unknown")
        brand = doc.get("Brand", "Unknown")
        size_ml = doc.get("Size_ML", "Unknown")
        qty = safe_int(doc.get("Qty", 0))

        if category not in summary:
            summary[category] = {
                "category_total": 0,
                "brands": {}
            }

        if brand not in summary[category]["brands"]:
            summary[category]["brands"][brand] = {
                "brand_total": 0,
                "sizes": {}
            }

        if size_ml not in summary[category]["brands"][brand]["sizes"]:
            summary[category]["brands"][brand]["sizes"][size_ml] = 0

        summary[category]["brands"][brand]["sizes"][size_ml] += qty
        summary[category]["brands"][brand]["brand_total"] += qty
        summary[category]["category_total"] += qty
        grand_total += qty

    return summary, grand_total


def get_filter_options():
    categories = sorted([x for x in collection.distinct("Brand_Category") if x])
    brands = sorted([x for x in collection.distinct("Brand") if x])
    sizes = sorted(
        [
            int(x) for x in collection.distinct("Size_ML")
            if str(x).strip().isdigit()
        ]
    )
    return categories, brands, sizes


# -----------------------------------
# Routes
# -----------------------------------
@app.route("/", methods=["GET"])
def dashboard():
    selected_category = request.args.get("brand_category", "").strip()
    selected_brand = request.args.get("brand", "").strip()
    selected_size = request.args.get("size_ml", "").strip()
    from_date = request.args.get("from_date", "").strip()
    to_date = request.args.get("to_date", "").strip()

    # Filtered docs
    docs = get_filtered_records(
        brand_category=selected_category,
        brand=selected_brand,
        size_ml=selected_size,
        from_date=from_date,
        to_date=to_date
    )

    records = [serialize_doc(doc) for doc in docs]
    summary, grand_total = build_summary(docs)

    # Overall docs without filters
    all_docs = list(collection.find({}))

    total_records = len(all_docs)
    total_qty = sum(safe_int(doc.get("Qty", 0)) for doc in all_docs)

    filtered_records = len(docs)
    filtered_qty = sum(safe_int(doc.get("Qty", 0)) for doc in docs)

    categories, brands, sizes = get_filter_options()

    return render_template(
        "incoming_stock.html",
        records=records,
        summary=summary,
        grand_total=grand_total,
        categories=categories,
        brands=brands,
        sizes=sizes,
        selected_category=selected_category,
        selected_brand=selected_brand,
        selected_size=selected_size,
        from_date=from_date,
        to_date=to_date,
        total_records=total_records,
        total_qty=total_qty,
        filtered_records=filtered_records,
        filtered_qty=filtered_qty,
    )

# -----------------------------------
# Cascading dropdown APIs
# -----------------------------------
@app.route("/api/brands", methods=["GET"])
def api_brands():
    brand_category = request.args.get("brand_category", "").strip()
    query = {}
    if brand_category:
        query["Brand_Category"] = brand_category

    brands = sorted(collection.distinct("Brand", query))
    return jsonify(brands)


@app.route("/api/sizes", methods=["GET"])
def api_sizes():
    brand_category = request.args.get("brand_category", "").strip()
    brand = request.args.get("brand", "").strip()

    query = {}
    if brand_category:
        query["Brand_Category"] = brand_category
    if brand:
        query["Brand"] = brand

    sizes = collection.distinct("Size_ML", query)
    sizes = sorted([int(x) for x in sizes if str(x).strip().isdigit()])
    return jsonify(sizes)


# -----------------------------------
# Add / Edit / Delete
# -----------------------------------
@app.route("/add", methods=["POST"])
def add_record():
    try:
        brand = request.form.get("Brand", "").strip()
        brand_category = request.form.get("Brand_Category", "").strip()
        size_ml = safe_int(request.form.get("Size_ML"))
        date_str = request.form.get("Date", "").strip()
        qty = safe_int(request.form.get("Qty"))

        if not all([brand, brand_category, size_ml, date_str]):
            flash("All required fields must be filled.", "danger")
            return redirect(url_for("dashboard"))

        now = datetime.utcnow()

        doc = {
            "Brand": brand,
            "Brand_Category": brand_category,
            "Size_ML": size_ml,
            "Date": date_str,
            "Qty": qty,
            "updated_at": now,
        }

        collection.update_one(
            {
                "Brand": brand,
                "Brand_Category": brand_category,
                "Size_ML": size_ml,
                "Date": date_str,
            },
            {
                "$set": doc,
                "$setOnInsert": {"created_at": now},
            },
            upsert=True
        )

        flash("Record added or updated successfully.", "success")
    except Exception as e:
        flash(f"Error adding record: {str(e)}", "danger")

    return redirect(url_for("dashboard"))


@app.route("/edit/<record_id>", methods=["GET", "POST"])
def edit_record(record_id):
    try:
        doc = collection.find_one({"_id": ObjectId(record_id)})
        if not doc:
            flash("Record not found.", "danger")
            return redirect(url_for("dashboard"))
    except Exception:
        flash("Invalid record id.", "danger")
        return redirect(url_for("dashboard"))

    if request.method == "POST":
        try:
            brand = request.form.get("Brand", "").strip()
            brand_category = request.form.get("Brand_Category", "").strip()
            size_ml = safe_int(request.form.get("Size_ML"))
            date_str = request.form.get("Date", "").strip()
            qty = safe_int(request.form.get("Qty"))

            update_doc = {
                "Brand": brand,
                "Brand_Category": brand_category,
                "Size_ML": size_ml,
                "Date": date_str,
                "Qty": qty,
                "updated_at": datetime.utcnow(),
            }

            collection.update_one(
                {"_id": ObjectId(record_id)},
                {"$set": update_doc}
            )

            flash("Record updated successfully.", "success")
            return redirect(url_for("dashboard"))
        except Exception as e:
            flash(f"Error updating record: {str(e)}", "danger")

    return render_template("edit_incoming_stock.html", record=serialize_doc(doc))


@app.route("/delete/<record_id>", methods=["POST"])
def delete_record(record_id):
    try:
        result = collection.delete_one({"_id": ObjectId(record_id)})
        if result.deleted_count:
            flash("Record deleted successfully.", "success")
        else:
            flash("Record not found.", "warning")
    except Exception as e:
        flash(f"Error deleting record: {str(e)}", "danger")

    return redirect(url_for("dashboard"))


# -----------------------------------
# CSV Import
# -----------------------------------
@app.route("/import-csv", methods=["POST"])
def import_csv():
    file = request.files.get("file")

    if not file or file.filename == "":
        flash("Please choose a CSV file.", "danger")
        return redirect(url_for("dashboard"))

    try:
        stream = io.StringIO(file.stream.read().decode("utf-8-sig"))
        reader = csv.DictReader(stream)

        required_columns = {"Brand", "Brand_Category", "Size_ML", "Date", "Qty"}
        if not reader.fieldnames or not required_columns.issubset(set(reader.fieldnames)):
            flash("CSV must contain Brand, Brand_Category, Size_ML, Date, Qty", "danger")
            return redirect(url_for("dashboard"))

        operations = []
        now = datetime.utcnow()
        processed = 0

        for row in reader:
            brand = (row.get("Brand") or "").strip()
            brand_category = (row.get("Brand_Category") or "").strip()
            size_ml = safe_int(row.get("Size_ML"))
            date_str = (row.get("Date") or "").strip()
            qty = safe_int(row.get("Qty"))

            if not all([brand, brand_category, size_ml, date_str]):
                continue

            doc = {
                "Brand": brand,
                "Brand_Category": brand_category,
                "Size_ML": size_ml,
                "Date": date_str,
                "Qty": qty,
                "updated_at": now,
            }

            operations.append(
                UpdateOne(
                    {
                        "Brand": brand,
                        "Brand_Category": brand_category,
                        "Size_ML": size_ml,
                        "Date": date_str,
                    },
                    {
                        "$set": doc,
                        "$setOnInsert": {"created_at": now},
                    },
                    upsert=True
                )
            )
            processed += 1

        if operations:
            collection.bulk_write(operations)

        flash(f"CSV import completed. Rows processed: {processed}", "success")
    except Exception as e:
        flash(f"CSV import failed: {str(e)}", "danger")

    return redirect(url_for("dashboard"))


# -----------------------------------
# Detailed export
# -----------------------------------
@app.route("/export-details", methods=["GET"])
def export_details():
    selected_category = request.args.get("brand_category", "").strip()
    selected_brand = request.args.get("brand", "").strip()
    selected_size = request.args.get("size_ml", "").strip()
    from_date = request.args.get("from_date", "").strip()
    to_date = request.args.get("to_date", "").strip()

    docs = get_filtered_records(
        brand_category=selected_category,
        brand=selected_brand,
        size_ml=selected_size,
        from_date=from_date,
        to_date=to_date
    )

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "_id", "Brand", "Brand_Category", "Size_ML", "Date", "Qty", "created_at", "updated_at"
    ])

    for doc in docs:
        writer.writerow([
            str(doc.get("_id", "")),
            doc.get("Brand", ""),
            doc.get("Brand_Category", ""),
            doc.get("Size_ML", ""),
            doc.get("Date", ""),
            doc.get("Qty", 0),
            doc.get("created_at", ""),
            doc.get("updated_at", ""),
        ])

    output.seek(0)
    return send_file(
        io.BytesIO(output.getvalue().encode("utf-8")),
        mimetype="text/csv",
        as_attachment=True,
        download_name="incoming_stock_details.csv"
    )


# -----------------------------------
# Summary export
# -----------------------------------
@app.route("/export-summary", methods=["GET"])
def export_summary():
    selected_category = request.args.get("brand_category", "").strip()
    selected_brand = request.args.get("brand", "").strip()
    selected_size = request.args.get("size_ml", "").strip()
    from_date = request.args.get("from_date", "").strip()
    to_date = request.args.get("to_date", "").strip()

    docs = get_filtered_records(
        brand_category=selected_category,
        brand=selected_brand,
        size_ml=selected_size,
        from_date=from_date,
        to_date=to_date
    )

    summary, grand_total = build_summary(docs)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Brand_Category", "Brand", "Size_ML", "Qty", "Row_Type"])

    for category, category_data in summary.items():
        for brand, brand_data in category_data["brands"].items():
            for size_ml, qty in brand_data["sizes"].items():
                writer.writerow([category, brand, size_ml, qty, "DETAIL"])

            writer.writerow([category, brand, "", brand_data["brand_total"], "BRAND_SUBTOTAL"])

        writer.writerow([category, "", "", category_data["category_total"], "CATEGORY_SUBTOTAL"])

    writer.writerow(["", "", "", grand_total, "GRAND_TOTAL"])

    output.seek(0)
    return send_file(
        io.BytesIO(output.getvalue().encode("utf-8")),
        mimetype="text/csv",
        as_attachment=True,
        download_name="incoming_stock_summary.csv"
    )


if __name__ == "__main__":
    app.run()