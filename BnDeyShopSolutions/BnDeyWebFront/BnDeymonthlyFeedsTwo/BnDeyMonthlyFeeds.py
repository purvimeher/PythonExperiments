from flask import Flask, render_template, request, redirect, url_for, flash, send_file
from pymongo import MongoClient, ASCENDING, UpdateOne
from bson import ObjectId
from datetime import datetime
from io import StringIO, BytesIO
import csv
import pandas as pd

app = Flask(__name__)
app.secret_key = "replace-with-a-secure-secret-key"

# -----------------------------
# MongoDB Configuration
# -----------------------------
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bndey_db_Experimental"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

monthly_stock_col = db["monthly_stock"]
stock_prices_col = db["stock_prices"]

# Duplicate handling index
monthly_stock_col.create_index(
    [("Brand", ASCENDING), ("Brand_Category", ASCENDING), ("Size_ML", ASCENDING), ("Date", ASCENDING)],
    unique=True
)

stock_prices_col.create_index(
    [("Brand", ASCENDING), ("Brand_Category", ASCENDING), ("Size_ML", ASCENDING)],
    unique=False
)


# -----------------------------
# Helpers
# -----------------------------
def parse_display_date_to_html(date_str: str) -> str:
    """dd/mm/yyyy -> yyyy-mm-dd"""
    if not date_str:
        return ""
    try:
        return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return ""


def parse_html_date_to_display(date_str: str) -> str:
    """yyyy-mm-dd -> dd/mm/yyyy"""
    if not date_str:
        return ""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%d/%m/%Y")
    except ValueError:
        return date_str


def safe_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def get_price_master_data():
    """Get stock_prices records ordered by Sl_No for dropdown linkage."""
    records = list(
        stock_prices_col.find(
            {},
            {
                "_id": 0,
                "Brand": 1,
                "Brand_Category": 1,
                "Size_ML": 1,
                "Sl_No": 1
            }
        ).sort("Sl_No", ASCENDING)
    )
    return records


def build_dropdown_map(price_records):
    """
    Builds nested map:
    {
      "Brand_Category": {
         "Brand1": [180, 375],
         "Brand2": [700]
      }
    }
    """
    mapping = {}
    for rec in price_records:
        category = rec.get("Brand_Category", "")
        brand = rec.get("Brand", "")
        size_ml = rec.get("Size_ML", "")

        if category not in mapping:
            mapping[category] = {}

        if brand not in mapping[category]:
            mapping[category][brand] = []

        if size_ml not in mapping[category][brand]:
            mapping[category][brand].append(size_ml)

    for category in mapping:
        for brand in mapping[category]:
            mapping[category][brand] = sorted(mapping[category][brand])

    return mapping


def get_distinct_filter_values():
    brands = sorted(monthly_stock_col.distinct("Brand"))
    categories = sorted(monthly_stock_col.distinct("Brand_Category"))
    dates = sorted(monthly_stock_col.distinct("Date"))
    sizes = sorted(monthly_stock_col.distinct("Size_ML"))
    return {
        "brands": brands,
        "categories": categories,
        "dates": dates,
        "sizes": sizes
    }


def get_sl_no_map():
    """Map (Brand, Brand_Category, Size_ML) -> Sl_No"""
    sl_map = {}
    cursor = stock_prices_col.find(
        {},
        {"Brand": 1, "Brand_Category": 1, "Size_ML": 1, "Sl_No": 1}
    )
    for rec in cursor:
        key = (rec.get("Brand"), rec.get("Brand_Category"), rec.get("Size_ML"))
        sl_map[key] = rec.get("Sl_No", 999999)
    return sl_map


def enrich_and_sort_monthly_stock(records):
    sl_map = get_sl_no_map()

    enriched = []
    for rec in records:
        key = (rec.get("Brand"), rec.get("Brand_Category"), rec.get("Size_ML"))
        rec["Sl_No"] = sl_map.get(key, 999999)
        rec["Date_html"] = parse_display_date_to_html(rec.get("Date"))
        enriched.append(rec)

    enriched.sort(key=lambda x: (
        x.get("Sl_No", 999999),
        x.get("Brand_Category", ""),
        x.get("Brand", ""),
        safe_int(x.get("Size_ML", 0)),
        x.get("Date", "")
    ))
    return enriched


def build_query_from_request(req):
    query = {}

    brand = req.args.get("brand", "").strip()
    brand_category = req.args.get("brand_category", "").strip()
    date_value = req.args.get("date", "").strip()
    size_ml = req.args.get("size_ml", "").strip()
    search = req.args.get("search", "").strip()

    if brand:
        query["Brand"] = brand
    if brand_category:
        query["Brand_Category"] = brand_category
    if size_ml:
        query["Size_ML"] = safe_int(size_ml)

    if date_value:
        # HTML date yyyy-mm-dd -> dd/mm/yyyy
        query["Date"] = parse_html_date_to_display(date_value)

    if search:
        query["$or"] = [
            {"Brand": {"$regex": search, "$options": "i"}},
            {"Brand_Category": {"$regex": search, "$options": "i"}},
            {"Date": {"$regex": search, "$options": "i"}},
        ]

    return query


def compute_summaries(records):
    """
    records = list of dicts from monthly_stock
    Returns:
      - total_qty
      - subtotal_by_brand
      - subtotal_by_brand_size
    """
    total_qty = 0
    subtotal_by_brand = {}
    subtotal_by_brand_size = {}

    for rec in records:
        brand = rec.get("Brand", "")
        size_ml = rec.get("Size_ML", "")
        qty = safe_int(rec.get("Qty", 0))

        total_qty += qty
        subtotal_by_brand[brand] = subtotal_by_brand.get(brand, 0) + qty

        key = (brand, size_ml)
        subtotal_by_brand_size[key] = subtotal_by_brand_size.get(key, 0) + qty

    subtotal_by_brand_rows = [
        {"Brand": brand, "Total_Qty": qty}
        for brand, qty in sorted(subtotal_by_brand.items(), key=lambda x: x[0])
    ]

    subtotal_by_brand_size_rows = [
        {"Brand": brand, "Size_ML": size_ml, "Total_Qty": qty}
        for (brand, size_ml), qty in sorted(subtotal_by_brand_size.items(), key=lambda x: (x[0][0], x[0][1]))
    ]

    return total_qty, subtotal_by_brand_rows, subtotal_by_brand_size_rows


# -----------------------------
# Routes
# -----------------------------
@app.route("/")
def index():
    query = build_query_from_request(request)
    raw_records = list(monthly_stock_col.find(query))
    records = enrich_and_sort_monthly_stock(raw_records)

    total_qty, subtotal_by_brand, subtotal_by_brand_size = compute_summaries(records)
    filter_values = get_distinct_filter_values()
    price_records = get_price_master_data()
    dropdown_map = build_dropdown_map(price_records)

    return render_template(
        "index.html",
        records=records,
        total_qty=total_qty,
        subtotal_by_brand=subtotal_by_brand,
        subtotal_by_brand_size=subtotal_by_brand_size,
        filter_values=filter_values,
        query_params=request.args,
        price_records=price_records,
        dropdown_map=dropdown_map
    )


@app.route("/add", methods=["GET", "POST"])
def add_record():
    price_records = get_price_master_data()
    dropdown_map = build_dropdown_map(price_records)

    if request.method == "POST":
        brand_category = request.form.get("brand_category", "").strip()
        brand = request.form.get("brand", "").strip()
        size_ml = safe_int(request.form.get("size_ml"))
        qty = safe_int(request.form.get("qty"))
        date_html = request.form.get("date", "").strip()
        date_display = parse_html_date_to_display(date_html)

        if not brand or not brand_category or not size_ml or not date_display:
            flash("Brand, Brand Category, Size_ML and Date are required.", "danger")
            return render_template(
                "form.html",
                action="Add",
                record=request.form,
                dropdown_map=dropdown_map
            )

        doc = {
            "Brand": brand,
            "Brand_Category": brand_category,
            "Size_ML": size_ml,
            "Date": date_display,
            "Qty": qty,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }

        try:
            monthly_stock_col.insert_one(doc)
            flash("Record added successfully.", "success")
            return redirect(url_for("index"))
        except Exception:
            # Duplicate: update qty instead of failing
            monthly_stock_col.update_one(
                {
                    "Brand": brand,
                    "Brand_Category": brand_category,
                    "Size_ML": size_ml,
                    "Date": date_display
                },
                {
                    "$set": {
                        "Qty": qty,
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            flash("Duplicate found. Existing record updated instead.", "warning")
            return redirect(url_for("index"))

    return render_template(
        "form.html",
        action="Add",
        record={},
        dropdown_map=dropdown_map
    )


@app.route("/edit/<record_id>", methods=["GET", "POST"])
def edit_record(record_id):
    record = monthly_stock_col.find_one({"_id": ObjectId(record_id)})
    if not record:
        flash("Record not found.", "danger")
        return redirect(url_for("index"))

    price_records = get_price_master_data()
    dropdown_map = build_dropdown_map(price_records)

    if request.method == "POST":
        brand_category = request.form.get("brand_category", "").strip()
        brand = request.form.get("brand", "").strip()
        size_ml = safe_int(request.form.get("size_ml"))
        qty = safe_int(request.form.get("qty"))
        date_html = request.form.get("date", "").strip()
        date_display = parse_html_date_to_display(date_html)

        if not brand or not brand_category or not size_ml or not date_display:
            flash("Brand, Brand Category, Size_ML and Date are required.", "danger")
            record["Date_html"] = parse_display_date_to_html(record.get("Date"))
            return render_template(
                "form.html",
                action="Edit",
                record=record,
                dropdown_map=dropdown_map
            )

        duplicate = monthly_stock_col.find_one({
            "Brand": brand,
            "Brand_Category": brand_category,
            "Size_ML": size_ml,
            "Date": date_display,
            "_id": {"$ne": ObjectId(record_id)}
        })

        if duplicate:
            flash("Duplicate record exists for Brand + Brand_Category + Size_ML + Date.", "danger")
            record["Date_html"] = parse_display_date_to_html(record.get("Date"))
            return render_template(
                "form.html",
                action="Edit",
                record=record,
                dropdown_map=dropdown_map
            )

        monthly_stock_col.update_one(
            {"_id": ObjectId(record_id)},
            {
                "$set": {
                    "Brand": brand,
                    "Brand_Category": brand_category,
                    "Size_ML": size_ml,
                    "Date": date_display,
                    "Qty": qty,
                    "updated_at": datetime.utcnow()
                }
            }
        )

        flash("Record updated successfully.", "success")
        return redirect(url_for("index"))

    record["Date_html"] = parse_display_date_to_html(record.get("Date"))
    return render_template(
        "form.html",
        action="Edit",
        record=record,
        dropdown_map=dropdown_map
    )


@app.route("/delete/<record_id>", methods=["POST"])
def delete_record(record_id):
    monthly_stock_col.delete_one({"_id": ObjectId(record_id)})
    flash("Record deleted successfully.", "success")
    return redirect(url_for("index"))


@app.route("/import", methods=["GET", "POST"])
def import_csv():
    if request.method == "POST":
        file = request.files.get("file")
        if not file or file.filename == "":
            flash("Please choose a CSV file.", "danger")
            return redirect(url_for("import_csv"))

        try:
            df = pd.read_csv(file)

            required_columns = ["Brand", "Brand_Category", "Size_ML", "Date", "Qty"]
            missing = [col for col in required_columns if col not in df.columns]
            if missing:
                flash(f"Missing required columns: {', '.join(missing)}", "danger")
                return redirect(url_for("import_csv"))

            operations = []
            inserted_or_updated = 0

            for _, row in df.iterrows():
                brand = str(row["Brand"]).strip()
                brand_category = str(row["Brand_Category"]).strip()
                size_ml = safe_int(row["Size_ML"])
                qty = safe_int(row["Qty"])
                raw_date = str(row["Date"]).strip()

                # Supports dd/mm/yyyy or yyyy-mm-dd
                try:
                    if "/" in raw_date:
                        date_display = datetime.strptime(raw_date, "%d/%m/%Y").strftime("%d/%m/%Y")
                    else:
                        date_display = datetime.strptime(raw_date, "%Y-%m-%d").strftime("%d/%m/%Y")
                except ValueError:
                    continue

                operations.append(
                    UpdateOne(
                        {
                            "Brand": brand,
                            "Brand_Category": brand_category,
                            "Size_ML": size_ml,
                            "Date": date_display
                        },
                        {
                            "$set": {
                                "Qty": qty,
                                "updated_at": datetime.utcnow()
                            },
                            "$setOnInsert": {
                                "created_at": datetime.utcnow()
                            }
                        },
                        upsert=True
                    )
                )
                inserted_or_updated += 1

            if operations:
                monthly_stock_col.bulk_write(operations)

            flash(f"CSV import completed. Processed {inserted_or_updated} rows.", "success")
            return redirect(url_for("index"))

        except Exception as exc:
            flash(f"CSV import failed: {str(exc)}", "danger")
            return redirect(url_for("import_csv"))

    return render_template("import_csv.html")


@app.route("/export")
def export_csv():
    query = build_query_from_request(request)
    raw_records = list(monthly_stock_col.find(query))
    records = enrich_and_sort_monthly_stock(raw_records)

    output = StringIO()
    writer = csv.writer(output)

    writer.writerow([
        "Sl_No", "Brand", "Brand_Category", "Size_ML", "Date", "Qty",
        "created_at", "updated_at"
    ])

    for rec in records:
        writer.writerow([
            rec.get("Sl_No", ""),
            rec.get("Brand", ""),
            rec.get("Brand_Category", ""),
            rec.get("Size_ML", ""),
            rec.get("Date", ""),
            rec.get("Qty", ""),
            rec.get("created_at", ""),
            rec.get("updated_at", "")
        ])

    mem = BytesIO()
    mem.write(output.getvalue().encode("utf-8"))
    mem.seek(0)
    output.close()

    return send_file(
        mem,
        mimetype="text/csv",
        as_attachment=True,
        download_name="monthly_stock_export.csv"
    )


if __name__ == "__main__":
    app.run()