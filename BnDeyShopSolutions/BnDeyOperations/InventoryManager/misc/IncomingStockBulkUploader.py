from __future__ import annotations

import csv
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Tuple, Any, List

from pymongo import MongoClient, UpdateOne, ASCENDING
from pymongo.errors import BulkWriteError


class IncomingStockBulkUploader:
    """
    Bulk CSV uploader for incoming_stock collection.

    Features:
    1. Aggregates duplicate rows within the CSV
    2. Uses upsert to avoid duplicate documents in MongoDB
    3. Prevents duplicate runs of the same file using file hash tracking
    4. Creates required indexes automatically
    """

    def __init__(
        self,
        mongo_uri: str = "mongodb://localhost:27017/",
        db_name: str = "inventory_db",
        incoming_stock_collection: str = "incoming_stock",
        upload_log_collection: str = "upload_audit_log",
    ) -> None:
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.incoming_stock = self.db[incoming_stock_collection]
        self.upload_log = self.db[upload_log_collection]

        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        """
        Create indexes needed for idempotent loads.
        """
        # Prevent duplicate business rows
        self.incoming_stock.create_index(
            [
                ("Brand_Category", ASCENDING),
                ("Brand", ASCENDING),
                ("Size_ML", ASCENDING),
                ("Date", ASCENDING),
            ],
            unique=True,
            name="uniq_incoming_stock_business_key",
        )

        # Prevent the exact same file from being processed twice
        self.upload_log.create_index(
            [("file_hash", ASCENDING)],
            unique=True,
            name="uniq_upload_file_hash",
        )

    @staticmethod
    def _utcnow() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _normalize_text(value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip()

    @staticmethod
    def _normalize_int(value: Any, field_name: str) -> int:
        try:
            return int(float(str(value).strip()))
        except Exception as exc:
            raise ValueError(f"Invalid integer for {field_name}: {value}") from exc

    def _compute_file_hash(self, csv_path: str) -> str:
        """
        Hash file bytes so the same exact file is not reprocessed.
        """
        sha256 = hashlib.sha256()
        with open(csv_path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    def _already_processed(self, file_hash: str) -> bool:
        return self.upload_log.find_one({"file_hash": file_hash}) is not None

    def _mark_file_processed(
        self,
        file_hash: str,
        csv_path: str,
        row_count: int,
        aggregated_count: int,
        mongo_result: Dict[str, Any],
    ) -> None:
        self.upload_log.insert_one(
            {
                "file_name": Path(csv_path).name,
                "file_path": str(Path(csv_path).resolve()),
                "file_hash": file_hash,
                "row_count": row_count,
                "aggregated_row_count": aggregated_count,
                "mongo_result": mongo_result,
                "processed_at": self._utcnow(),
            }
        )

    def _read_and_aggregate_csv(
        self, csv_path: str
    ) -> Tuple[Dict[Tuple[str, str, int, str], Dict[str, Any]], int]:
        """
        Read CSV and aggregate duplicate rows inside the file.

        Expected CSV columns:
        Brand_Category, Brand, Size_ML, Date, Qty
        """
        aggregated: Dict[Tuple[str, str, int, str], Dict[str, Any]] = {}
        raw_row_count = 0

        with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)

            required_columns = {"Brand_Category", "Brand", "Size_ML", "Date", "Qty"}
            missing = required_columns - set(reader.fieldnames or [])
            if missing:
                raise ValueError(f"Missing CSV columns: {sorted(missing)}")

            for row in reader:
                raw_row_count += 1

                brand_category = self._normalize_text(row.get("Brand_Category"))
                brand = self._normalize_text(row.get("Brand"))
                size_ml = self._normalize_int(row.get("Size_ML"), "Size_ML")
                stock_date = self._normalize_text(row.get("Date"))
                qty = self._normalize_int(row.get("Qty"), "Qty")

                if not brand_category:
                    raise ValueError(f"Empty Brand_Category at row {raw_row_count}")
                if not brand:
                    raise ValueError(f"Empty Brand at row {raw_row_count}")
                if not stock_date:
                    raise ValueError(f"Empty Date at row {raw_row_count}")
                if qty <= 0:
                    raise ValueError(f"Qty must be > 0 at row {raw_row_count}")

                key = (brand_category, brand, size_ml, stock_date)

                if key not in aggregated:
                    aggregated[key] = {
                        "Brand_Category": brand_category,
                        "Brand": brand,
                        "Size_ML": size_ml,
                        "Date": stock_date,
                        "Qty": qty,
                    }
                else:
                    # Aggregate duplicate rows in same CSV
                    aggregated[key]["Qty"] += qty

        return aggregated, raw_row_count

    def upload_csv(self, csv_path: str) -> Dict[str, Any]:
        """
        Upload CSV into incoming_stock using bulk upsert.

        Duplicate handling strategy:
        - If duplicate rows exist inside CSV -> aggregate Qty
        - If same business key already exists in MongoDB -> update Qty
        - If same exact file is uploaded again -> skip entire run
        """
        csv_path = str(Path(csv_path).resolve())
        file_hash = self._compute_file_hash(csv_path)

        if self._already_processed(file_hash):
            return {
                "status": "skipped",
                "message": "This file was already processed earlier. Duplicate run prevented.",
                "file_path": csv_path,
                "file_hash": file_hash,
            }

        aggregated_rows, raw_row_count = self._read_and_aggregate_csv(csv_path)

        operations: List[UpdateOne] = []
        now = self._utcnow()

        for _, doc in aggregated_rows.items():
            filter_doc = {
                "Brand_Category": doc["Brand_Category"],
                "Brand": doc["Brand"],
                "Size_ML": doc["Size_ML"],
                "Date": doc["Date"],
            }

            update_doc = {
                # For incoming stock, safest duplicate behavior is to SET final aggregated qty
                # instead of incrementing, so reruns do not double count.
                "$set": {
                    "Brand_Category": doc["Brand_Category"],
                    "Brand": doc["Brand"],
                    "Size_ML": doc["Size_ML"],
                    "Date": doc["Date"],
                    "Qty": doc["Qty"],
                    "updated_at": now,
                },
                "$setOnInsert": {
                    "created_at": now,
                },
            }

            operations.append(
                UpdateOne(
                    filter_doc,
                    update_doc,
                    upsert=True,
                )
            )

        if not operations:
            return {
                "status": "success",
                "message": "CSV had no data rows.",
                "file_path": csv_path,
                "file_hash": file_hash,
            }

        try:
            result = self.incoming_stock.bulk_write(operations, ordered=False)

            mongo_result = {
                "matched_count": result.matched_count,
                "modified_count": result.modified_count,
                "upserted_count": len(result.upserted_ids),
            }

            self._mark_file_processed(
                file_hash=file_hash,
                csv_path=csv_path,
                row_count=raw_row_count,
                aggregated_count=len(aggregated_rows),
                mongo_result=mongo_result,
            )

            return {
                "status": "success",
                "message": "Incoming stock CSV uploaded successfully.",
                "file_path": csv_path,
                "file_hash": file_hash,
                "raw_csv_rows": raw_row_count,
                "aggregated_rows": len(aggregated_rows),
                "mongo_result": mongo_result,
            }

        except BulkWriteError as exc:
            raise RuntimeError(f"Bulk upload failed: {exc.details}") from exc


if __name__ == "__main__":
    uploader = IncomingStockBulkUploader(
        mongo_uri="mongodb://localhost:27017/",
        db_name="inventory_db",
        incoming_stock_collection="incoming_stock",
        upload_log_collection="upload_audit_log",
    )

    csv_file = "/path/to/Incoming_Stock_09042026.csv"
    result = uploader.upload_csv(csv_file)
    print(result)