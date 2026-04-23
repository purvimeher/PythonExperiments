import math
from datetime import datetime
from typing import Dict, List, Any, Tuple

import pandas as pd
import streamlit as st
from bson import ObjectId

from config import COLLECTIONS
from db import get_collection


HIDDEN_COLUMNS = {"_id"}
DATE_FIELD_HINTS = {"Date", "created_at", "updated_at", "Inserted_At", "Last_Updated"}
NUMERIC_FIELD_HINTS = {
    "Qty",
    "Size_ML",
    "Sl_No",
    "Total_Amount",
    "Maximum_Retail_Price_per_bottle",
    "Maximum_Retail_Price_per_bottle_OLD",
    "Maximum_Retail_Price_per_case",
}
FILTER_ORDER = ["Date", "YearMonth", "Brand_Category", "Brand", "Size_ML"]


# -----------------------------
# Generic helpers
# -----------------------------
def normalize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    clean = {}
    for key, value in record.items():
        if key in HIDDEN_COLUMNS:
            continue
        if isinstance(value, datetime):
            clean[key] = value.strftime("%Y-%m-%d %H:%M:%S")
        else:
            clean[key] = value
    return clean


def parse_value(value: Any):
    if value is None:
        return None
    if isinstance(value, str):
        val = value.strip()
        if val == "":
            return ""
        lowered = val.lower()
        if lowered in {"true", "false"}:
            return lowered == "true"
        try:
            if "." in val:
                return float(val)
            return int(val)
        except Exception:
            return val
    return value


def coerce_record_types(record: Dict[str, Any]) -> Dict[str, Any]:
    return {k: parse_value(v) for k, v in record.items()}


def is_numeric_field(field: str) -> bool:
    return field in NUMERIC_FIELD_HINTS


def is_date_field(field: str) -> bool:
    return field in DATE_FIELD_HINTS


def get_collection_config(collection_name: str) -> Dict[str, Any]:
    return COLLECTIONS[collection_name]


def fetch_all_records(collection_name: str) -> List[Dict[str, Any]]:
    collection = get_collection(collection_name)
    return list(collection.find())


def get_display_dataframe(collection_name: str) -> pd.DataFrame:
    docs = fetch_all_records(collection_name)
    clean_docs = [normalize_record(doc) for doc in docs]
    df = pd.DataFrame(clean_docs)

    expected_fields = get_collection_config(collection_name)["fields"]
    if df.empty:
        return pd.DataFrame(columns=expected_fields)

    existing = [c for c in expected_fields if c in df.columns]
    extra = [c for c in df.columns if c not in existing and c != "_id"]
    ordered = existing + extra
    return df[ordered]


def get_raw_records_with_ids(collection_name: str) -> List[Dict[str, Any]]:
    return list(get_collection(collection_name).find())


def safe_to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0)


def paginate_dataframe(df: pd.DataFrame, page_num: int, page_size: int) -> pd.DataFrame:
    start_idx = (page_num - 1) * page_size
    end_idx = start_idx + page_size
    return df.iloc[start_idx:end_idx]


def export_csv(df: pd.DataFrame, collection_name: str, key_prefix: str):
    export_df = df.drop(columns=[c for c in ["_id"] if c in df.columns], errors="ignore")
    csv_bytes = export_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label=f"Export {collection_name} CSV",
        data=csv_bytes,
        file_name=f"{collection_name}.csv",
        mime="text/csv",
        key=f"{key_prefix}_{collection_name}_export_csv",
    )


# -----------------------------
# Cascading filters
# -----------------------------
def get_filter_sequence(collection_name: str) -> List[str]:
    configured = get_collection_config(collection_name).get("filters", [])
    ordered = [f for f in FILTER_ORDER if f in configured]
    remainder = [f for f in configured if f not in ordered]
    return ordered + remainder


def get_filter_options_from_subset(df: pd.DataFrame, field: str) -> List[str]:
    if field not in df.columns:
        return ["All"]
    values = df[field].dropna().astype(str).unique().tolist()
    values = sorted(values)
    return ["All"] + values


def apply_filters(df: pd.DataFrame, filters: Dict[str, Any]) -> pd.DataFrame:
    filtered = df.copy()
    for field, selected in filters.items():
        if selected != "All" and field in filtered.columns:
            filtered = filtered[filtered[field].astype(str) == str(selected)]
    return filtered


def render_cascading_filters(collection_name: str, df: pd.DataFrame, key_prefix: str) -> Dict[str, Any]:
    filter_sequence = get_filter_sequence(collection_name)
    if not filter_sequence:
        return {}

    cols = st.columns(len(filter_sequence))
    selections = {}
    working_df = df.copy()

    for idx, field in enumerate(filter_sequence):
        options = get_filter_options_from_subset(working_df, field)
        current_selection = st.selectbox(
            f"Filter by {field}",
            options,
            index=0,
            key=f"{key_prefix}_{collection_name}_filter_{field}",
        )
        selections[field] = current_selection
        if current_selection != "All" and field in working_df.columns:
            working_df = working_df[working_df[field].astype(str) == str(current_selection)]

    return selections


# -----------------------------
# Summary helpers
# -----------------------------
def render_metric_cards(collection_name: str, filtered_df: pd.DataFrame, full_df: pd.DataFrame):
    c1, c2, c3, c4 = st.columns(4)
    qty_total = 0
    if "Qty" in filtered_df.columns and not filtered_df.empty:
        qty_total = safe_to_numeric(filtered_df["Qty"]).sum()

    with c1:
        st.metric("Collection", get_collection_config(collection_name)["title"])
    with c2:
        st.metric("Total Records", len(full_df))
    with c3:
        st.metric("Filtered Records", len(filtered_df))
    with c4:
        st.metric("Filtered Qty", int(qty_total) if float(qty_total).is_integer() else round(qty_total, 2))


def build_group_summary(df: pd.DataFrame, group_field: str) -> pd.DataFrame:
    if df.empty or group_field not in df.columns:
        return pd.DataFrame()

    grouped = df.groupby(group_field, dropna=False).size().reset_index(name="Records")
    if "Qty" in df.columns:
        qty_summary = df.copy()
        qty_summary["Qty"] = safe_to_numeric(qty_summary["Qty"])
        qty_grouped = qty_summary.groupby(group_field, dropna=False)["Qty"].sum().reset_index(name="Qty Total")
        grouped = grouped.merge(qty_grouped, on=group_field, how="left")

    return grouped.sort_values(by="Records", ascending=False)


def render_summary_cards_by_brand_category(filtered_df: pd.DataFrame, collection_name: str, key_prefix: str):
    st.subheader("Summary")
    col1, col2 = st.columns(2)

    with col1:
        if "Brand_Category" in filtered_df.columns:
            st.markdown("**Totals by Brand Category**")
            brand_cat_df = build_group_summary(filtered_df, "Brand_Category")
            if brand_cat_df.empty:
                st.info("No Brand_Category summary available.")
            else:
                st.dataframe(brand_cat_df, use_container_width=True, hide_index=True)
                export_csv(brand_cat_df, f"{collection_name}_summary_by_brand_category", f"{key_prefix}_brand_category")

    with col2:
        if "Brand" in filtered_df.columns:
            st.markdown("**Totals by Brand**")
            brand_df = build_group_summary(filtered_df, "Brand")
            if brand_df.empty:
                st.info("No Brand summary available.")
            else:
                st.dataframe(brand_df, use_container_width=True, hide_index=True)
                export_csv(brand_df, f"{collection_name}_summary_by_brand", f"{key_prefix}_brand")


# -----------------------------
# Duplicate-safe CSV import
# -----------------------------
def csv_to_records(uploaded_file) -> List[Dict[str, Any]]:
    df = pd.read_csv(uploaded_file)
    df = df.loc[:, ~df.columns.str.contains(r"^Unnamed")]
    if "_id" in df.columns:
        df = df.drop(columns=["_id"])
    records = df.to_dict(orient="records")
    return [coerce_record_types(r) for r in records]


def get_dedup_keys(collection_name: str) -> List[str]:
    mapping = {
        "daily_sales": ["Date", "Brand_Category", "Brand", "Size_ML"],
        "monthly_sales": ["YearMonth", "Brand_Category", "Brand", "Size_ML"],
        "incoming_stock": ["Date", "Brand_Category", "Brand", "Size_ML"],
        "current_inventory": ["Date", "Brand_Category", "Brand", "Size_ML"],
        "stock_prices": ["Brand_Category", "Brand", "Size_ML"],
    }
    return mapping.get(collection_name, [])


def deduplicate_import_records(records: List[Dict[str, Any]], key_fields: List[str]) -> Tuple[List[Dict[str, Any]], int]:
    if not key_fields:
        return records, 0

    deduped = {}
    duplicate_count = 0

    for row in records:
        key = tuple(str(row.get(field, "")).strip() for field in key_fields)
        if key in deduped:
            duplicate_count += 1
            existing = deduped[key]
            if "Qty" in row and "Qty" in existing:
                try:
                    existing["Qty"] = safe_to_numeric(pd.Series([existing.get("Qty", 0), row.get("Qty", 0)])).sum()
                except Exception:
                    existing["Qty"] = row.get("Qty")
            else:
                existing.update(row)
        else:
            deduped[key] = row.copy()

    return list(deduped.values()), duplicate_count


def stock_prices_upsert_many(records: List[Dict[str, Any]]) -> Tuple[int, int]:
    collection = get_collection("stock_prices")
    inserted = 0
    updated = 0
    now = datetime.utcnow()

    for row in records:
        query = {
            "Brand_Category": row.get("Brand_Category"),
            "Brand": row.get("Brand"),
            "Size_ML": row.get("Size_ML"),
        }
        payload = row.copy()
        payload["updated_at"] = now
        payload.setdefault("created_at", now)

        existing = collection.find_one(query)
        if existing:
            collection.update_one({"_id": existing["_id"]}, {"$set": payload})
            updated += 1
        else:
            collection.insert_one(payload)
            inserted += 1

    return inserted, updated


def generic_duplicate_safe_import(collection_name: str, records: List[Dict[str, Any]]) -> Tuple[int, int]:
    collection = get_collection(collection_name)
    key_fields = get_dedup_keys(collection_name)
    deduped_records, duplicates_in_file = deduplicate_import_records(records, key_fields)
    inserted = 0
    skipped_existing = 0
    now = datetime.utcnow()

    for row in deduped_records:
        row = row.copy()
        row.pop("_id", None)
        row.setdefault("created_at", now)
        row["updated_at"] = now

        query = {field: row.get(field) for field in key_fields} if key_fields else row
        exists = collection.find_one(query) if key_fields else None
        if exists:
            skipped_existing += 1
            continue
        collection.insert_one(row)
        inserted += 1

    return inserted, skipped_existing + duplicates_in_file


def import_csv_to_collection(collection_name: str, uploaded_file, key_prefix: str):
    if uploaded_file is None:
        return

    records = csv_to_records(uploaded_file)
    if not records:
        st.warning("CSV file contains no records.")
        return

    st.caption("Import mode: duplicate-safe. Existing rows are skipped for most collections; stock_prices uses upsert.")

    if collection_name == "stock_prices":
        deduped_records, duplicates_in_file = deduplicate_import_records(records, get_dedup_keys(collection_name))
        inserted, updated = stock_prices_upsert_many(deduped_records)
        st.success(
            f"stock_prices import complete. Inserted: {inserted}, Updated: {updated}, Duplicates merged in file: {duplicates_in_file}."
        )
        return

    inserted, skipped = generic_duplicate_safe_import(collection_name, records)
    st.success(f"Import complete. Inserted: {inserted}, Skipped/Merged duplicates: {skipped}.")


# -----------------------------
# Typed form rendering
# -----------------------------
def typed_widget_for_field(field: str, value: Any, key: str):
    if is_numeric_field(field):
        default_val = 0.0
        try:
            default_val = float(value) if value not in [None, ""] else 0.0
        except Exception:
            default_val = 0.0
        number_value = st.number_input(field, value=default_val, key=key)
        if float(number_value).is_integer():
            return int(number_value)
        return float(number_value)

    if is_date_field(field):
        default_str = "" if value in [None, ""] else str(value)
        return st.text_input(field, value=default_str, key=key, help="Use your existing format, e.g. 09/04/2026")

    return st.text_input(field, value="" if value is None else str(value), key=key)


def render_add_record_form(collection_name: str, key_prefix: str):
    with st.expander("Add Record", expanded=False):
        editable_fields = get_collection_config(collection_name)["editable_fields"]
        values = {}
        cols = st.columns(2)

        for idx, field in enumerate(editable_fields):
            with cols[idx % 2]:
                values[field] = typed_widget_for_field(
                    field,
                    "",
                    key=f"{key_prefix}_{collection_name}_add_{field}",
                )

        if st.button("Add Record", key=f"{key_prefix}_{collection_name}_add_btn"):
            payload = {k: v for k, v in values.items() if not (isinstance(v, str) and v.strip() == "")}
            now = datetime.utcnow()
            payload["created_at"] = now
            payload["updated_at"] = now

            if collection_name == "stock_prices":
                inserted, updated = stock_prices_upsert_many([payload])
                st.success(f"stock_prices saved. Inserted: {inserted}, Updated: {updated}.")
            else:
                key_fields = get_dedup_keys(collection_name)
                query = {field: payload.get(field) for field in key_fields} if key_fields else payload
                exists = get_collection(collection_name).find_one(query) if key_fields else None
                if exists:
                    st.warning("Duplicate record detected. Record was not inserted.")
                else:
                    get_collection(collection_name).insert_one(payload)
                    st.success("Record added successfully.")
            st.rerun()


def render_edit_delete_section(collection_name: str, key_prefix: str):
    with st.expander("Edit / Delete Record", expanded=False):
        raw_docs = get_raw_records_with_ids(collection_name)
        if not raw_docs:
            st.info("No records found.")
            return

        label_map = {}
        for doc in raw_docs:
            doc_id = str(doc.get("_id"))
            label_parts = []
            for field in get_collection_config(collection_name)["editable_fields"][:4]:
                if field in doc:
                    label_parts.append(f"{field}: {doc[field]}")
            label_map[doc_id] = " | ".join(label_parts) if label_parts else doc_id

        selected_id = st.selectbox(
            "Select record",
            options=list(label_map.keys()),
            format_func=lambda x: label_map[x],
            key=f"{key_prefix}_{collection_name}_edit_select",
        )

        selected_doc = next((d for d in raw_docs if str(d.get("_id")) == selected_id), None)
        if not selected_doc:
            st.warning("Selected record not found.")
            return

        updated_values = {}
        editable_fields = get_collection_config(collection_name)["editable_fields"]
        cols = st.columns(2)

        for idx, field in enumerate(editable_fields):
            current_val = selected_doc.get(field, "")
            with cols[idx % 2]:
                updated_values[field] = typed_widget_for_field(
                    field,
                    current_val,
                    key=f"{key_prefix}_{collection_name}_edit_{selected_id}_{field}",
                )

        c1, c2 = st.columns(2)
        with c1:
            if st.button("Update Record", key=f"{key_prefix}_{collection_name}_update_btn_{selected_id}"):
                update_payload = {field: updated_values[field] for field in editable_fields}
                update_payload["updated_at"] = datetime.utcnow()
                get_collection(collection_name).update_one(
                    {"_id": ObjectId(selected_id)},
                    {"$set": update_payload},
                )
                st.success("Record updated successfully.")
                st.rerun()

        with c2:
            if st.button("Delete Record", key=f"{key_prefix}_{collection_name}_delete_btn_{selected_id}"):
                get_collection(collection_name).delete_one({"_id": ObjectId(selected_id)})
                st.success("Record deleted successfully.")
                st.rerun()


# -----------------------------
# Page renderer
# -----------------------------
def render_collection_page(collection_name: str, key_prefix: str):
    st.title(get_collection_config(collection_name)["title"])
    full_df = get_display_dataframe(collection_name)

    st.subheader("Upload CSV")
    uploaded_file = st.file_uploader(
        f"Import CSV into {collection_name}",
        type=["csv"],
        key=f"{key_prefix}_{collection_name}_uploader",
    )
    if uploaded_file is not None:
        import_csv_to_collection(collection_name, uploaded_file, key_prefix)

    render_add_record_form(collection_name, key_prefix)
    render_edit_delete_section(collection_name, key_prefix)

    st.subheader("Filters")
    selected_filters = render_cascading_filters(collection_name, full_df, key_prefix)
    filtered_df = apply_filters(full_df, selected_filters)

    render_metric_cards(collection_name, filtered_df, full_df)
    render_summary_cards_by_brand_category(filtered_df, collection_name, key_prefix)

    st.subheader("Results")
    page_size = st.selectbox(
        "Rows per page",
        options=[5, 10, 20, 50, 100],
        index=1,
        key=f"{key_prefix}_{collection_name}_page_size",
    )

    total_rows = len(filtered_df)
    total_pages = max(1, math.ceil(total_rows / page_size))
    page_num = st.number_input(
        "Page",
        min_value=1,
        max_value=total_pages,
        value=1,
        step=1,
        key=f"{key_prefix}_{collection_name}_page_number",
    )

    page_df = paginate_dataframe(filtered_df, int(page_num), int(page_size))
    st.dataframe(page_df, use_container_width=True, hide_index=True)
    export_csv(filtered_df, collection_name, key_prefix)
    st.caption(f"Showing page {page_num} of {total_pages} | {total_rows} matching rows")


# -----------------------------
# Dashboard summary helper
# -----------------------------
def get_collection_summary(collection_name: str):
    df = get_display_dataframe(collection_name)
    qty_sum = 0
    if "Qty" in df.columns and not df.empty:
        qty_sum = safe_to_numeric(df["Qty"]).sum()
    return {
        "collection": collection_name,
        "title": get_collection_config(collection_name)["title"],
        "records": len(df),
        "qty_sum": qty_sum,
    }