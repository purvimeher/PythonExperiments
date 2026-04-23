MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bndey_db_Experimental"

COLLECTIONS = {
    "daily_sales": {
        "title": "Daily Sales",
        "fields": ["Date", "Brand_Category", "Brand", "Size_ML", "Qty", "created_at", "updated_at"],
        "editable_fields": ["Date", "Brand_Category", "Brand", "Size_ML", "Qty"],
        "filters": ["Date", "Brand_Category", "Brand", "Size_ML"],
    },
    "monthly_sales": {
        "title": "Monthly Sales",
        "fields": ["YearMonth", "Brand_Category", "Brand", "Size_ML", "Qty", "Total_Amount", "Inserted_At"],
        "editable_fields": ["YearMonth", "Brand_Category", "Brand", "Size_ML", "Qty", "Total_Amount"],
        "filters": ["YearMonth", "Brand_Category", "Brand", "Size_ML"],
    },
    "incoming_stock": {
        "title": "Incoming Stock",
        "fields": ["Date", "Brand_Category", "Brand", "Size_ML", "Qty", "created_at", "updated_at"],
        "editable_fields": ["Date", "Brand_Category", "Brand", "Size_ML", "Qty"],
        "filters": ["Date", "Brand_Category", "Brand", "Size_ML"],
    },
    "stock_prices": {
        "title": "Stock Prices",
        "fields": [
            "Sl_No",
            "Brand_Category",
            "Brand",
            "Size_ML",
            "LookColumn",
            "Maximum_Retail_Price_per_bottle",
            "Maximum_Retail_Price_per_bottle_OLD",
            "Maximum_Retail_Price_per_case",
        ],
        "editable_fields": [
            "Sl_No",
            "Brand_Category",
            "Brand",
            "Size_ML",
            "LookColumn",
            "Maximum_Retail_Price_per_bottle",
            "Maximum_Retail_Price_per_bottle_OLD",
            "Maximum_Retail_Price_per_case",
        ],
        "filters": ["Brand_Category", "Brand", "Size_ML"],
    },
    "current_inventory": {
        "title": "Current Inventory",
        "fields": ["Date", "Brand_Category", "Brand", "Size_ML", "Qty", "Last_Updated", "updated_at"],
        "editable_fields": ["Date", "Brand_Category", "Brand", "Size_ML", "Qty"],
        "filters": ["Date", "Brand_Category", "Brand", "Size_ML"],
    },
}