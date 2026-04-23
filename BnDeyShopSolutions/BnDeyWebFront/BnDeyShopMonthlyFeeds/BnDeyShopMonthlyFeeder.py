import csv
import io
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from bson import ObjectId
from flask import (
    Flask,
    flash,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)
from pymongo import MongoClient, ASCENDING


class MongoConfig:
    MONGO_URI = "mongodb://localhost:27017/"
    DB_NAME = "bndey_db_Experimental"
    MONTHLY_STOCK_COLLECTION = "monthly_stock"
    STOCK_PRICES_COLLECTION = "stock_prices"


class MonthlyStockApp:
    def __init__(self):
        self.app = Flask(__name__)
        self.app.secret_key = "your-secret-key-change-this"

        self.client = MongoClient(MongoConfig.MONGO_URI)
        self.db = self.client[MongoConfig.DB_NAME]
        self.monthly_stock = self.db[MongoConfig.MONTHLY_STOCK_COLLECTION]
        self.stock_prices = self.db[MongoConfig.STOCK_PRICES_COLLECTION]

        self._ensure_indexes()
        self._register_routes()

    def _ensure_indexes(self) -> None:
        self.monthly_stock.create_index(
            [("Brand_Category", ASCENDING), ("Brand", ASCENDING), ("Size_ML", ASCENDING), ("Date", ASCENDING)]
        )
        self.stock_prices.create_index(
            [("Brand_Category", ASCENDING), ("Brand", ASCENDING), ("Size_ML", ASCENDING)]
        )
        self.stock_prices.create_index([("Sl_No", ASCENDING)])

    @staticmethod
    def parse_display_date_to_storage(date_str: str) -> str:
        """
        Input from HTML date field: YYYY-MM-DD
        Stored format: DD/MM/YYYY
        """
        if not date_str:
            return ""
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%d/%m/%Y")

    @staticmethod
    def parse_storage_date_to_html(date_str: str) -> str:
        """
        Stored format: DD/MM/YYYY
        Display in HTML date input: YYYY-MM-DD
        """
        if not date_str:
            return ""
        return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")

    @staticmethod
    def safe_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    def get_master_product_map(self) -> List[Dict[str, Any]]:
        """
        Get product master list from stock_prices ordered by Sl_No.
        """
        pipeline = [
            {
                "$project": {
                    "_id": 0,
                    "Brand_Category": 1,
                    "Brand": 1,
                    "Size_ML": 1,
                    "Sl_No": {"$ifNull": ["$Sl_No", 999999]},
                }
            },
            {"$sort": {"Sl_No": 1, "Brand_Category": 1, "Brand": 1, "Size_ML": 1}},
        ]
        return list(self.stock_prices.aggregate(pipeline))

    def get_filter_values(self) -> Dict[str, List[Any]]:
        categories = sorted(self.stock_prices.distinct("Brand_Category"))
        brands = sorted(self.stock_prices.distinct("Brand"))
        sizes = sorted(self.stock_prices.distinct("Size_ML"))

        # Dates come from monthly_stock because they are entered there
        dates = sorted(
            self.monthly_stock.distinct("Date"),
            key=lambda x: datetime.strptime(x, "%d/%m/%Y") if x else datetime.min
        )

        return {
            "categories": categories,
            "brands": brands,
            "sizes": sizes,
            "dates": dates,
        }

    def build_filter_query(self, form_data: Dict[str, str]) -> Dict[str, Any]:
        query: Dict[str, Any] = {}

        brand_category = form_data.get("brand_category", "").strip()
        brand = form_data.get("brand", "").strip()
        size_ml = form_data.get("size_ml", "").strip()
        date_value = form_data.get("date", "").strip()

        if brand_category:
            query["Brand_Category"] = brand_category
        if brand:
            query["Brand"] = brand
        if size_ml:
            query["Size_ML"] = self.safe_int(size_ml)
        if date_value:
            query["Date"] = date_value

        return query

    def get_monthly_stock_list(self, filters: Dict[str, str]) -> List[Dict[str, Any]]:
        match_query = self.build_filter_query(filters)

        pipeline = [
            {"$match": match_query},
            {
                "$lookup": {
                    "from": MongoConfig.STOCK_PRICES_COLLECTION,
                    "let": {
                        "brand": "$Brand",
                        "category": "$Brand_Category",
                        "size_ml": "$Size_ML",
                    },
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {"$eq": ["$Brand", "$$brand"]},
                                        {"$eq": ["$Brand_Category", "$$category"]},
                                        {"$eq": ["$Size_ML", "$$size_ml"]},
                                    ]
                                }
                            }
                        },
                        {
                            "$project": {
                                "_id": 0,
                                "Sl_No": 1,
                                "Maximum_Retail_Price_per_bottle": 1,
                                "Maximum_Retail_Price_per_case": 1,
                            }
                        },
                    ],
                    "as": "price_info",
                }
            },
            {
                "$addFields": {
                    "price_info": {"$arrayElemAt": ["$price_info", 0]},
                    "Sl_No": {
                        "$ifNull": [{"$arrayElemAt": ["$price_info.Sl_No", 0]}, 999999]
                    },
                    "Maximum_Retail_Price_per_bottle": {
                        "$ifNull": [{"$arrayElemAt": ["$price_info.Maximum_Retail_Price_per_bottle", 0]}, 0]
                    },
                    "Maximum_Retail_Price_per_case": {
                        "$ifNull": [{"$arrayElemAt": ["$price_info.Maximum_Retail_Price_per_case", 0]}, 0]
                    },
                }
            },
            {"$sort": {"Sl_No": 1, "Date": 1, "Brand_Category": 1, "Brand": 1, "Size_ML": 1}},
        ]

        records = list(self.monthly_stock.aggregate(pipeline))
        for record in records:
            record["id"] = str(record["_id"])
        return records

    def get_record_by_id(self, record_id: str) -> Optional[Dict[str, Any]]:
        try:
            record = self.monthly_stock.find_one({"_id": ObjectId(record_id)})
            if record:
                record["id"] = str(record["_id"])
            return record
        except Exception:
            return None

    def insert_record(self, data: Dict[str, Any]) -> None:
        now = datetime.utcnow()
        payload = {
            "Brand_Category": data["Brand_Category"],
            "Brand": data["Brand"],
            "Size_ML": self.safe_int(data["Size_ML"]),
            "Date": data["Date"],
            "Qty": self.safe_int(data["Qty"]),
            "created_at": now,
            "updated_at": now,
        }
        self.monthly_stock.insert_one(payload)

    def update_record(self, record_id: str, data: Dict[str, Any]) -> bool:
        try:
            result = self.monthly_stock.update_one(
                {"_id": ObjectId(record_id)},
                {
                    "$set": {
                        "Brand_Category": data["Brand_Category"],
                        "Brand": data["Brand"],
                        "Size_ML": self.safe_int(data["Size_ML"]),
                        "Date": data["Date"],
                        "Qty": self.safe_int(data["Qty"]),
                        "updated_at": datetime.utcnow(),
                    }
                },
            )
            return result.modified_count >= 0
        except Exception:
            return False

    def delete_record(self, record_id: str) -> bool:
        try:
            result = self.monthly_stock.delete_one({"_id": ObjectId(record_id)})
            return result.deleted_count > 0
        except Exception:
            return False

    def import_csv_data(self, uploaded_file) -> int:
        """
        Expected CSV columns:
        Brand_Category,Brand,Size_ML,Date,Qty
        Date can be DD/MM/YYYY or YYYY-MM-DD
        """
        content = uploaded_file.read().decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(content))
        inserted_count = 0

        for row in reader:
            brand_category = (row.get("Brand_Category") or "").strip()
            brand = (row.get("Brand") or "").strip()
            size_ml = self.safe_int(row.get("Size_ML"))
            qty = self.safe_int(row.get("Qty"))
            date_value = (row.get("Date") or "").strip()

            if not brand_category or not brand or not size_ml or not date_value:
                continue

            # Normalize date
            try:
                if "-" in date_value:
                    normalized_date = datetime.strptime(date_value, "%Y-%m-%d").strftime("%d/%m/%Y")
                else:
                    normalized_date = datetime.strptime(date_value, "%d/%m/%Y").strftime("%d/%m/%Y")
            except ValueError:
                continue

            self.monthly_stock.insert_one(
                {
                    "Brand_Category": brand_category,
                    "Brand": brand,
                    "Size_ML": size_ml,
                    "Date": normalized_date,
                    "Qty": qty,
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                }
            )
            inserted_count += 1

        return inserted_count

    def export_csv_data(self, filters: Dict[str, str]):
        records = self.get_monthly_stock_list(filters)

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(
            [
                "Sl_No",
                "Brand_Category",
                "Brand",
                "Size_ML",
                "Date",
                "Qty",
                "Maximum_Retail_Price_per_bottle",
                "Maximum_Retail_Price_per_case",
            ]
        )

        for row in records:
            writer.writerow(
                [
                    row.get("Sl_No", ""),
                    row.get("Brand_Category", ""),
                    row.get("Brand", ""),
                    row.get("Size_ML", ""),
                    row.get("Date", ""),
                    row.get("Qty", ""),
                    row.get("Maximum_Retail_Price_per_bottle", ""),
                    row.get("Maximum_Retail_Price_per_case", ""),
                ]
            )

        mem = io.BytesIO()
        mem.write(output.getvalue().encode("utf-8"))
        mem.seek(0)
        output.close()

        return mem

    def _register_routes(self) -> None:
        @self.app.route("/")
        def home():
            return redirect(url_for("monthly_stock_list"))

        @self.app.route("/monthly-stock", methods=["GET"])
        def monthly_stock_list():
            filters = {
                "brand_category": request.args.get("brand_category", "").strip(),
                "brand": request.args.get("brand", "").strip(),
                "size_ml": request.args.get("size_ml", "").strip(),
                "date": request.args.get("date", "").strip(),
            }

            records = self.get_monthly_stock_list(filters)
            filter_values = self.get_filter_values()
            master_products = self.get_master_product_map()

            return render_template(
                "monthly_stock_list.html",
                records=records,
                filters=filters,
                filter_values=filter_values,
                master_products=master_products,
            )

        @self.app.route("/monthly-stock/add", methods=["GET", "POST"])
        def monthly_stock_add():
            master_products = self.get_master_product_map()
            filter_values = self.get_filter_values()

            if request.method == "POST":
                brand_category = request.form.get("brand_category", "").strip()
                brand = request.form.get("brand", "").strip()
                size_ml = request.form.get("size_ml", "").strip()
                date_html = request.form.get("date", "").strip()
                qty = request.form.get("qty", "").strip()

                if not all([brand_category, brand, size_ml, date_html, qty]):
                    flash("All fields are required.", "danger")
                    return render_template(
                        "monthly_stock_form.html",
                        action="Add",
                        record=request.form,
                        master_products=master_products,
                        filter_values=filter_values,
                    )

                data = {
                    "Brand_Category": brand_category,
                    "Brand": brand,
                    "Size_ML": size_ml,
                    "Date": self.parse_display_date_to_storage(date_html),
                    "Qty": qty,
                }

                self.insert_record(data)
                flash("Monthly stock record added successfully.", "success")
                return redirect(url_for("monthly_stock_list"))

            return render_template(
                "monthly_stock_form.html",
                action="Add",
                record=None,
                master_products=master_products,
                filter_values=filter_values,
            )

        @self.app.route("/monthly-stock/edit/<record_id>", methods=["GET", "POST"])
        def monthly_stock_edit(record_id: str):
            existing = self.get_record_by_id(record_id)
            if not existing:
                flash("Record not found.", "danger")
                return redirect(url_for("monthly_stock_list"))

            master_products = self.get_master_product_map()
            filter_values = self.get_filter_values()

            if request.method == "POST":
                brand_category = request.form.get("brand_category", "").strip()
                brand = request.form.get("brand", "").strip()
                size_ml = request.form.get("size_ml", "").strip()
                date_html = request.form.get("date", "").strip()
                qty = request.form.get("qty", "").strip()

                if not all([brand_category, brand, size_ml, date_html, qty]):
                    flash("All fields are required.", "danger")
                    existing["Date_html"] = self.parse_storage_date_to_html(existing.get("Date", ""))
                    return render_template(
                        "monthly_stock_form.html",
                        action="Edit",
                        record=existing,
                        master_products=master_products,
                        filter_values=filter_values,
                    )

                updated = self.update_record(
                    record_id,
                    {
                        "Brand_Category": brand_category,
                        "Brand": brand,
                        "Size_ML": size_ml,
                        "Date": self.parse_display_date_to_storage(date_html),
                        "Qty": qty,
                    },
                )

                if updated:
                    flash("Monthly stock record updated successfully.", "success")
                else:
                    flash("Failed to update record.", "danger")

                return redirect(url_for("monthly_stock_list"))

            existing["Date_html"] = self.parse_storage_date_to_html(existing.get("Date", ""))
            return render_template(
                "monthly_stock_form.html",
                action="Edit",
                record=existing,
                master_products=master_products,
                filter_values=filter_values,
            )

        @self.app.route("/monthly-stock/delete/<record_id>", methods=["POST"])
        def monthly_stock_delete(record_id: str):
            if self.delete_record(record_id):
                flash("Record deleted successfully.", "success")
            else:
                flash("Record not found or could not be deleted.", "danger")
            return redirect(url_for("monthly_stock_list"))

        @self.app.route("/monthly-stock/import", methods=["GET", "POST"])
        def monthly_stock_import():
            if request.method == "POST":
                uploaded_file = request.files.get("csv_file")
                if not uploaded_file or uploaded_file.filename == "":
                    flash("Please choose a CSV file.", "danger")
                    return redirect(url_for("monthly_stock_import"))

                inserted_count = self.import_csv_data(uploaded_file)
                flash(f"{inserted_count} records imported successfully.", "success")
                return redirect(url_for("monthly_stock_list"))

            return render_template("monthly_stock_import.html")

        @self.app.route("/monthly-stock/export", methods=["GET"])
        def monthly_stock_export():
            filters = {
                "brand_category": request.args.get("brand_category", "").strip(),
                "brand": request.args.get("brand", "").strip(),
                "size_ml": request.args.get("size_ml", "").strip(),
                "date": request.args.get("date", "").strip(),
            }
            file_data = self.export_csv_data(filters)

            return send_file(
                file_data,
                mimetype="text/csv",
                as_attachment=True,
                download_name="monthly_stock_export.csv",
            )

        @self.app.route("/monthly-stock/product-options", methods=["GET"])
        def monthly_stock_product_options():
            """
            Simple endpoint for JS-linked dropdown filtering.
            """
            products = self.get_master_product_map()

            category = request.args.get("brand_category", "").strip()
            brand = request.args.get("brand", "").strip()

            filtered = products
            if category:
                filtered = [p for p in filtered if p.get("Brand_Category") == category]
            if brand:
                filtered = [p for p in filtered if p.get("Brand") == brand]

            categories = sorted(set(p["Brand_Category"] for p in products if p.get("Brand_Category")))
            brands = sorted(set(p["Brand"] for p in filtered if p.get("Brand")))
            sizes = sorted(set(p["Size_ML"] for p in filtered if p.get("Size_ML") is not None))

            return {
                "categories": categories,
                "brands": brands,
                "sizes": sizes,
                "products": filtered,
            }

    def run(self):
        self.app.run(debug=True, host="127.0.0.1", port=5000)


if __name__ == "__main__":
    app_instance = MonthlyStockApp()
    app_instance.run()