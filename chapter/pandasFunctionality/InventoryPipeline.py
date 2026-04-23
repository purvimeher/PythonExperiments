import os
import json
import hashlib
from datetime import datetime
from typing import List, Optional

import pandas as pd


class InventoryPipeline:
    def __init__(
        self,
        input_dir: str,
        output_dir: str,
        processing_log_file: str = "processed_files.json"
    ):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.processing_log_path = os.path.join(output_dir, processing_log_file)

        os.makedirs(self.output_dir, exist_ok=True)

        self.product_keys = ["Brand_Category", "Brand", "Size_ML"]
        self.transaction_keys = ["Date", "Brand_Category", "Brand", "Size_ML"]

        self._initialize_processing_log()

    # ----------------------------
    # Processing log / idempotency
    # ----------------------------
    def _initialize_processing_log(self) -> None:
        if not os.path.exists(self.processing_log_path):
            with open(self.processing_log_path, "w", encoding="utf-8") as f:
                json.dump({"processed_files": []}, f, indent=2)

    def _read_processing_log(self) -> dict:
        with open(self.processing_log_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _write_processing_log(self, log_data: dict) -> None:
        with open(self.processing_log_path, "w", encoding="utf-8") as f:
            json.dump(log_data, f, indent=2)

    def _calculate_file_hash(self, file_path: str) -> str:
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(8192)
                if not chunk:
                    break
                sha256.update(chunk)
        return sha256.hexdigest()

    def is_file_already_processed(self, file_path: str) -> bool:
        file_hash = self._calculate_file_hash(file_path)
        log_data = self._read_processing_log()
        return file_hash in log_data["processed_files"]

    def mark_file_as_processed(self, file_path: str) -> None:
        file_hash = self._calculate_file_hash(file_path)
        log_data = self._read_processing_log()

        if file_hash not in log_data["processed_files"]:
            log_data["processed_files"].append(file_hash)
            self._write_processing_log(log_data)

    # ----------------------------
    # CSV loading and validation
    # ----------------------------
    def load_csv(self, file_path: str, required_columns: List[str]) -> pd.DataFrame:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        df = pd.read_csv(file_path)

        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(
                f"Missing required columns in {file_path}: {missing_columns}"
            )

        return df

    def standardize_dataframe(self, df: pd.DataFrame, has_date: bool) -> pd.DataFrame:
        df = df.copy()

        # Clean column values
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].astype(str).str.strip()

        # Type conversions
        df["Brand_Category"] = df["Brand_Category"].astype(str)
        df["Brand"] = df["Brand"].astype(str)
        df["Size_ML"] = pd.to_numeric(df["Size_ML"], errors="coerce").fillna(0).astype(int)
        df["Qty"] = pd.to_numeric(df["Qty"], errors="coerce").fillna(0).astype(int)

        if has_date:
            df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
            if df["Date"].isna().any():
                bad_rows = df[df["Date"].isna()]
                raise ValueError(f"Invalid Date values found:\n{bad_rows}")

        return df

    def validate_non_negative_qty(self, df: pd.DataFrame, df_name: str) -> None:
        invalid_rows = df[df["Qty"] < 0]
        if not invalid_rows.empty:
            raise ValueError(f"{df_name} contains negative Qty rows:\n{invalid_rows}")

    def add_look_column(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["LookColumn"] = (
            "(" + df["Brand_Category"].astype(str) + ") - " +
            df["Brand"].astype(str) + " - " +
            df["Size_ML"].astype(str) + " ML"
        )
        return df

    # ----------------------------
    # Duplicate handling
    # ----------------------------
    def drop_exact_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.drop_duplicates().copy()

    def aggregate_stock_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        For initial stock: aggregate by product keys.
        """
        grouped = (
            df.groupby(self.product_keys, as_index=False)["Qty"]
            .sum()
            .sort_values(self.product_keys)
            .reset_index(drop=True)
        )
        return grouped

    def aggregate_transaction_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        For incoming stock and daily sales: aggregate by Date + product keys.
        """
        grouped = (
            df.groupby(self.transaction_keys, as_index=False)["Qty"]
            .sum()
            .sort_values(self.transaction_keys)
            .reset_index(drop=True)
        )
        return grouped

    # ----------------------------
    # Inventory logic
    # ----------------------------
    def load_initial_inventory(self, file_path: str) -> pd.DataFrame:
        required_columns = ["Brand_Category", "Brand", "Size_ML", "Qty"]

        df = self.load_csv(file_path, required_columns)
        df = self.standardize_dataframe(df, has_date=False)
        df = self.drop_exact_duplicates(df)
        df = self.aggregate_stock_data(df)
        self.validate_non_negative_qty(df, "Initial Stock")

        df = self.add_look_column(df)
        df["Last_Updated"] = pd.Timestamp.now()

        return df

    def load_transaction_file(self, file_path: str, file_type: str) -> pd.DataFrame:
        required_columns = ["Date", "Brand_Category", "Brand", "Size_ML", "Qty"]

        df = self.load_csv(file_path, required_columns)
        df = self.standardize_dataframe(df, has_date=True)
        df = self.drop_exact_duplicates(df)
        df = self.aggregate_transaction_data(df)
        self.validate_non_negative_qty(df, file_type)

        return df

    def apply_incoming_stock(
        self,
        inventory_df: pd.DataFrame,
        incoming_df: pd.DataFrame,
        apply_date: Optional[pd.Timestamp] = None
    ) -> pd.DataFrame:
        inventory_df = inventory_df.copy()
        incoming_df = incoming_df.copy()

        if apply_date is not None:
            incoming_df = incoming_df[incoming_df["Date"] == apply_date].copy()

        if incoming_df.empty:
            return inventory_df

        incoming_grouped = (
            incoming_df.groupby(self.product_keys, as_index=False)["Qty"]
            .sum()
            .rename(columns={"Qty": "Incoming_Qty"})
        )

        merged = pd.merge(
            inventory_df,
            incoming_grouped,
            on=self.product_keys,
            how="outer"
        )

        merged["Qty"] = merged["Qty"].fillna(0).astype(int)
        merged["Incoming_Qty"] = merged["Incoming_Qty"].fillna(0).astype(int)
        merged["Qty"] = merged["Qty"] + merged["Incoming_Qty"]

        merged.drop(columns=["Incoming_Qty"], inplace=True)

        merged["LookColumn"] = (
            "(" + merged["Brand_Category"].astype(str) + ") - " +
            merged["Brand"].astype(str) + " - " +
            merged["Size_ML"].astype(str) + " ML"
        )
        merged["Last_Updated"] = pd.Timestamp.now()

        return merged.sort_values(self.product_keys).reset_index(drop=True)

    def apply_daily_sales(
        self,
        inventory_df: pd.DataFrame,
        sales_df: pd.DataFrame,
        apply_date: Optional[pd.Timestamp] = None
    ) -> pd.DataFrame:
        inventory_df = inventory_df.copy()
        sales_df = sales_df.copy()

        if apply_date is not None:
            sales_df = sales_df[sales_df["Date"] == apply_date].copy()

        if sales_df.empty:
            return inventory_df

        sales_grouped = (
            sales_df.groupby(self.product_keys, as_index=False)["Qty"]
            .sum()
            .rename(columns={"Qty": "Sold_Qty"})
        )

        merged = pd.merge(
            inventory_df,
            sales_grouped,
            on=self.product_keys,
            how="left"
        )

        merged["Qty"] = merged["Qty"].fillna(0).astype(int)
        merged["Sold_Qty"] = merged["Sold_Qty"].fillna(0).astype(int)

        # Check insufficient stock before subtracting
        insufficient = merged[merged["Sold_Qty"] > merged["Qty"]].copy()
        if not insufficient.empty:
            raise ValueError(
                "Insufficient stock for some products:\n"
                f"{insufficient[[*self.product_keys, 'Qty', 'Sold_Qty']]}"
            )

        merged["Qty"] = merged["Qty"] - merged["Sold_Qty"]
        merged.drop(columns=["Sold_Qty"], inplace=True)

        merged["LookColumn"] = (
            "(" + merged["Brand_Category"].astype(str) + ") - " +
            merged["Brand"].astype(str) + " - " +
            merged["Size_ML"].astype(str) + " ML"
        )
        merged["Last_Updated"] = pd.Timestamp.now()

        return merged.sort_values(self.product_keys).reset_index(drop=True)

    # ----------------------------
    # Reporting / snapshots
    # ----------------------------
    def create_inventory_snapshot(
        self,
        inventory_df: pd.DataFrame,
        snapshot_date: pd.Timestamp
    ) -> pd.DataFrame:
        snapshot_df = inventory_df.copy()
        snapshot_df["Date"] = snapshot_date
        snapshot_df["updated_at"] = pd.Timestamp.now()

        ordered_columns = [
            "Brand_Category",
            "Brand",
            "Size_ML",
            "Date",
            "Qty",
            "LookColumn",
            "Last_Updated",
            "updated_at"
        ]

        for col in ordered_columns:
            if col not in snapshot_df.columns:
                snapshot_df[col] = None

        return snapshot_df[ordered_columns]

    def create_daily_summary(
        self,
        incoming_df: pd.DataFrame,
        sales_df: pd.DataFrame,
        process_date: pd.Timestamp
    ) -> pd.DataFrame:
        incoming_day = incoming_df[incoming_df["Date"] == process_date]["Qty"].sum()
        sales_day = sales_df[sales_df["Date"] == process_date]["Qty"].sum()

        summary = pd.DataFrame([{
            "Date": process_date,
            "Total_Incoming_Qty": int(incoming_day),
            "Total_Sold_Qty": int(sales_day),
            "Processed_At": pd.Timestamp.now()
        }])

        return summary

    # ----------------------------
    # Save outputs
    # ----------------------------
    def save_csv(self, df: pd.DataFrame, filename: str) -> str:
        output_path = os.path.join(self.output_dir, filename)
        df.to_csv(output_path, index=False)
        return output_path

    # ----------------------------
    # Main processing methods
    # ----------------------------
    def process_day(
        self,
        inventory_df: pd.DataFrame,
        incoming_df: pd.DataFrame,
        sales_df: pd.DataFrame,
        process_date: pd.Timestamp
    ) -> pd.DataFrame:
        updated_inventory = self.apply_incoming_stock(
            inventory_df=inventory_df,
            incoming_df=incoming_df,
            apply_date=process_date
        )

        updated_inventory = self.apply_daily_sales(
            inventory_df=updated_inventory,
            sales_df=sales_df,
            apply_date=process_date
        )

        return updated_inventory

    def run_pipeline(
        self,
        initial_stock_file: str,
        incoming_stock_file: str,
        daily_sales_file: str
    ) -> pd.DataFrame:
        # Idempotency at file level
        files_to_check = [initial_stock_file, incoming_stock_file, daily_sales_file]
        already_processed = [
            file_path for file_path in files_to_check
            if self.is_file_already_processed(file_path)
        ]

        if already_processed:
            raise ValueError(
                f"These files were already processed before: {already_processed}"
            )

        # Load
        inventory_df = self.load_initial_inventory(initial_stock_file)
        incoming_df = self.load_transaction_file(incoming_stock_file, "Incoming Stock")
        sales_df = self.load_transaction_file(daily_sales_file, "Daily Sales")

        # Build complete date list
        all_dates = sorted(set(incoming_df["Date"].dropna().tolist() + sales_df["Date"].dropna().tolist()))

        if not all_dates:
            raise ValueError("No valid dates found in incoming stock or daily sales files.")

        current_inventory = inventory_df.copy()
        snapshot_files = []
        summary_rows = []

        for process_date in all_dates:
            current_inventory = self.process_day(
                inventory_df=current_inventory,
                incoming_df=incoming_df,
                sales_df=sales_df,
                process_date=process_date
            )

            snapshot_df = self.create_inventory_snapshot(
                inventory_df=current_inventory,
                snapshot_date=process_date
            )

            safe_date = process_date.strftime("%d_%m_%Y")
            snapshot_file = self.save_csv(
                snapshot_df,
                f"inventory_snapshot_{safe_date}.csv"
            )
            snapshot_files.append(snapshot_file)

            summary_df = self.create_daily_summary(
                incoming_df=incoming_df,
                sales_df=sales_df,
                process_date=process_date
            )
            summary_rows.append(summary_df)

        # Save final inventory
        final_inventory = current_inventory.copy()
        final_inventory["updated_at"] = pd.Timestamp.now()

        self.save_csv(final_inventory, "final_inventory.csv")

        # Save incoming cleaned
        self.save_csv(incoming_df, "incoming_stock_cleaned.csv")

        # Save sales cleaned
        self.save_csv(sales_df, "daily_sales_cleaned.csv")

        # Save daily summary
        all_summary_df = pd.concat(summary_rows, ignore_index=True)
        self.save_csv(all_summary_df, "daily_processing_summary.csv")

        # Mark files as processed only after success
        for file_path in files_to_check:
            self.mark_file_as_processed(file_path)

        return final_inventory


if __name__ == "__main__":
    input_dir = "/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/pandasFunctionality/input_data"
    output_dir = "/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/pandasFunctionality/output_data"

    pipeline = InventoryPipeline(
        input_dir=input_dir,
        output_dir=output_dir
    )

    initial_stock_file = os.path.join(input_dir, "initial_stock.csv")
    incoming_stock_file = os.path.join(input_dir, "incoming_stock.csv")
    daily_sales_file = os.path.join(input_dir, "daily_sales.csv")

    try:
        final_inventory_df = pipeline.run_pipeline(
            initial_stock_file=initial_stock_file,
            incoming_stock_file=incoming_stock_file,
            daily_sales_file=daily_sales_file
        )

        print("Pipeline completed successfully.")
        print(final_inventory_df)

    except Exception as e:
        print(f"Pipeline failed: {e}")