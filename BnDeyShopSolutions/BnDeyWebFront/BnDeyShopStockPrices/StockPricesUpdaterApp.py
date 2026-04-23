import csv
import io
from datetime import datetime
from bson import ObjectId
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    send_file,
    jsonify
)

app = Flask(__name__)
app.secret_key = "your-secret-key-change-this"

# =========================
# MongoDB Configuration
# =========================
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bndey_db_Experimental"
COLLECTION_NAME = "stock_prices"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]


# =========================
# Indexes
# =========================
def create_indexes():
    collection.create_index(
        [("Brand_Category", ASCENDING)]
    )
    collection.create_index(
        [("Brand", ASCENDING)]
    )
    collection.create_index(
        [("Size_ML", ASCENDING)]
    )

    # Unique composite key for upsert and duplicate prevention
    collection.create_index(
        [
            ("Brand", ASCENDING),
            ("Brand_Category", ASCENDING),
            ("Size_ML", ASCENDING),
        ],
        unique=True,
        name="unique_brand_category_size"
    )


create_indexes()


# =========================
# Helpers
# =========================
def clean_int(value, default=0):
    try:
        if value is None or str(value).strip() == "":
            return default
        return int(float(str(value).strip()))
    except Exception:
        return default


def normalize_text(value):
    return str(value or "").strip()


def generate_look_column(brand_category, brand, size_ml):
    return f"({brand_category}) - {brand} - {size_ml} ML"


def build_document_from_form(form):
    brand_category = normalize_text(form.get("Brand_Category"))
    brand = normalize_text(form.get("Brand"))
    size_ml = clean_int(form.get("Size_ML"))
    look_column = normalize_text(form.get("LookColumn"))

    if not look_column:
        look_column = generate_look_column(brand_category, brand, size_ml)

    return {
        "Size_ML": size_ml,
        "Brand": brand,
        "Brand_Category": brand_category,
        "LookColumn": look_column,
        "Maximum_Retail_Price_per_bottle": clean_int(form.get("Maximum_Retail_Price_per_bottle")),
        "Maximum_Retail_Price_per_bottle_OLD": clean_int(form.get("Maximum_Retail_Price_per_bottle_OLD")),
        "Maximum_Retail_Price_per_case": clean_int(form.get("Maximum_Retail_Price_per_case")),
        "Sl_No": clean_int(form.get("Sl_No")),
        "updated_at": datetime.utcnow(),
    }


def get_filter_options():
    categories = sorted([x for x in collection.distinct("Brand_Category") if x])
    brands = sorted([x for x in collection.distinct("Brand") if x])
    sizes = sorted([x for x in collection.distinct("Size_ML") if x is not None])
    return categories, brands, sizes


def build_query(brand_category="", brand="", size_ml=""):
    query = {}

    if brand_category:
        query["Brand_Category"] = normalize_text(brand_category)

    if brand:
        query["Brand"] = normalize_text(brand)

    if str(size_ml).strip():
        query["Size_ML"] = clean_int(size_ml)

    return query


# =========================
# Home / List
# =========================
@app.route("/")
def index():
    brand_category = request.args.get("brand_category", "").strip()
    brand = request.args.get("brand", "").strip()
    size_ml = request.args.get("size_ml", "").strip()

    query = build_query(brand_category, brand, size_ml)

    records = list(collection.find(query).sort([
        ("Brand_Category", ASCENDING),
        ("Brand", ASCENDING),
        ("Size_ML", ASCENDING)
    ]))

    categories, brands, sizes = get_filter_options()

    return render_template(
        "index.html",
        records=records,
        categories=categories,
        brands=brands,
        sizes=sizes,
        selected_brand_category=brand_category,
        selected_brand=brand,
        selected_size_ml=size_ml
    )


# =========================
# Create
# =========================
@app.route("/create", methods=["GET", "POST"])
def create_record():
    if request.method == "POST":
        doc = build_document_from_form(request.form)
        doc["created_at"] = datetime.utcnow()

        unique_query = {
            "Brand": doc["Brand"],
            "Brand_Category": doc["Brand_Category"],
            "Size_ML": doc["Size_ML"]
        }

        existing = collection.find_one(unique_query)
        if existing:
            flash(
                "Record already exists for this Brand + Brand_Category + Size_ML. Use edit instead.",
                "danger"
            )
            return render_template("form.html", record=request.form, page_title="Create Record")

        try:
            collection.insert_one(doc)
            flash("Record created successfully.", "success")
            return redirect(url_for("index"))
        except DuplicateKeyError:
            flash(
                "Duplicate record detected for Brand + Brand_Category + Size_ML.",
                "danger"
            )
            return render_template("form.html", record=request.form, page_title="Create Record")

    return render_template("form.html", record=None, page_title="Create Record")


# =========================
# Edit
# =========================
@app.route("/edit/<id>", methods=["GET", "POST"])
def edit_record(id):
    record = collection.find_one({"_id": ObjectId(id)})
    if not record:
        flash("Record not found.", "danger")
        return redirect(url_for("index"))

    if request.method == "POST":
        updated_doc = build_document_from_form(request.form)

        duplicate = collection.find_one({
            "Brand": updated_doc["Brand"],
            "Brand_Category": updated_doc["Brand_Category"],
            "Size_ML": updated_doc["Size_ML"],
            "_id": {"$ne": ObjectId(id)}
        })

        if duplicate:
            flash(
                "Another record already exists with the same Brand + Brand_Category + Size_ML.",
                "danger"
            )
            merged_record = dict(record)
            merged_record.update(updated_doc)
            return render_template("form.html", record=merged_record, page_title="Edit Record")

        try:
            collection.update_one(
                {"_id": ObjectId(id)},
                {"$set": updated_doc}
            )
            flash("Record updated successfully.", "success")
            return redirect(url_for("index"))
        except DuplicateKeyError:
            flash(
                "Duplicate record detected while updating.",
                "danger"
            )
            return render_template("form.html", record=record, page_title="Edit Record")

    return render_template("form.html", record=record, page_title="Edit Record")


# =========================
# Delete
# =========================
@app.route("/delete/<id>", methods=["POST"])
def delete_record(id):
    collection.delete_one({"_id": ObjectId(id)})
    flash("Record deleted successfully.", "warning")
    return redirect(url_for("index"))


# =========================
# CSV Export
# =========================
@app.route("/export")
def export_csv():
    brand_category = request.args.get("brand_category", "").strip()
    brand = request.args.get("brand", "").strip()
    size_ml = request.args.get("size_ml", "").strip()

    query = build_query(brand_category, brand, size_ml)

    records = list(collection.find(query).sort([
        ("Brand_Category", ASCENDING),
        ("Brand", ASCENDING),
        ("Size_ML", ASCENDING)
    ]))

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow([
        "Sl_No",
        "Brand_Category",
        "Brand",
        "Size_ML",
        "LookColumn",
        "Maximum_Retail_Price_per_bottle",
        "Maximum_Retail_Price_per_bottle_OLD",
        "Maximum_Retail_Price_per_case"
    ])

    for record in records:
        writer.writerow([
            record.get("Sl_No", ""),
            record.get("Brand_Category", ""),
            record.get("Brand", ""),
            record.get("Size_ML", ""),
            record.get("LookColumn", ""),
            record.get("Maximum_Retail_Price_per_bottle", ""),
            record.get("Maximum_Retail_Price_per_bottle_OLD", ""),
            record.get("Maximum_Retail_Price_per_case", ""),
        ])

    mem = io.BytesIO()
    mem.write(output.getvalue().encode("utf-8"))
    mem.seek(0)
    output.close()

    return send_file(
        mem,
        mimetype="text/csv",
        as_attachment=True,
        download_name="stock_prices_export.csv"
    )


# =========================
# CSV Import with Upsert
# =========================
@app.route("/import", methods=["GET", "POST"])
def import_csv():
    if request.method == "POST":
        file = request.files.get("file")

        if not file or file.filename == "":
            flash("Please choose a CSV file.", "danger")
            return redirect(url_for("import_csv"))

        try:
            stream = io.StringIO(file.stream.read().decode("utf-8"))
            reader = csv.DictReader(stream)

            inserted_count = 0
            updated_count = 0

            for row in reader:
                brand_category = normalize_text(row.get("Brand_Category"))
                brand = normalize_text(row.get("Brand"))
                size_ml = clean_int(row.get("Size_ML"))

                if not brand_category or not brand or not size_ml:
                    continue

                look_column = normalize_text(row.get("LookColumn"))
                if not look_column:
                    look_column = generate_look_column(brand_category, brand, size_ml)

                update_doc = {
                    "Brand_Category": brand_category,
                    "Brand": brand,
                    "Size_ML": size_ml,
                    "LookColumn": look_column,
                    "Maximum_Retail_Price_per_bottle": clean_int(row.get("Maximum_Retail_Price_per_bottle")),
                    "Maximum_Retail_Price_per_bottle_OLD": clean_int(row.get("Maximum_Retail_Price_per_bottle_OLD")),
                    "Maximum_Retail_Price_per_case": clean_int(row.get("Maximum_Retail_Price_per_case")),
                    "Sl_No": clean_int(row.get("Sl_No")),
                    "updated_at": datetime.utcnow(),
                }

                unique_query = {
                    "Brand": brand,
                    "Brand_Category": brand_category,
                    "Size_ML": size_ml
                }

                existing = collection.find_one(unique_query)

                collection.update_one(
                    unique_query,
                    {
                        "$set": update_doc,
                        "$setOnInsert": {"created_at": datetime.utcnow()}
                    },
                    upsert=True
                )

                if existing:
                    updated_count += 1
                else:
                    inserted_count += 1

            flash(
                f"CSV import completed. Inserted: {inserted_count}, Updated: {updated_count}",
                "success"
            )
            return redirect(url_for("index"))

        except Exception as e:
            flash(f"Import failed: {str(e)}", "danger")
            return redirect(url_for("import_csv"))

    return render_template("import.html")


# =========================
# Cascading filter API
# =========================
@app.route("/filter-options")
def filter_options():
    brand_category = request.args.get("brand_category", "").strip()
    brand = request.args.get("brand", "").strip()

    brand_query = {}
    if brand_category:
        brand_query["Brand_Category"] = brand_category

    brands = sorted([x for x in collection.distinct("Brand", brand_query) if x])

    size_query = {}
    if brand_category:
        size_query["Brand_Category"] = brand_category
    if brand:
        size_query["Brand"] = brand

    sizes = sorted([x for x in collection.distinct("Size_ML", size_query) if x is not None])

    return jsonify({
        "brands": brands,
        "sizes": sizes
    })


# =========================
# Run
# =========================
if __name__ == "__main__":
    app.run()