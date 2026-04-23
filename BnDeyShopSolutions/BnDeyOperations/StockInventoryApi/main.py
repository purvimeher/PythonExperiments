from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from stock_price_service import StockPriceService
from typing import List

app = FastAPI()
service = StockPriceService()
service.create_indexes()


class StockPriceModel(BaseModel):
    Brand: str = Field(..., example="OFFICERS CHOICE PRESTIGE WHISKY")
    Size_ML: int = Field(..., example=1000)
    Brand_Category: str = Field(..., example="Deluxe Prestige Brand")
    LookColumn: str = Field(..., example="(Deluxe Prestige Brand) - OFFICERS CHOICE PRESTIGE WHISKY - 1000 ML")
    Maximum_Retail_Price_per_bottle: float = Field(..., example=376)
    Maximum_Retail_Price_per_bottle_OLD: float = Field(default=0, example=340)
    Maximum_Retail_Price_per_case: float = Field(..., example=3387)
    Sl_No: int = Field(..., example=1)


class StockPriceAPI:

    @staticmethod
    @app.get("/stock-prices")
    def fetch_all():

        return service.get_all_stock_prices()

    @staticmethod
    @app.get("/stock-price/{record_id}")
    def fetch_by_id(record_id: str):

        result = service.get_stock_price_by_id(record_id)

        if not result:
            raise HTTPException(
                status_code=404,
                detail="Record not found"
            )

        return result

    @staticmethod
    @app.get("/stock-price")
    def fetch_by_brand_size(
            brand: str,
            size_ml: int
    ):

        result = service.get_stock_price(
            brand,
            size_ml
        )

        if not result:
            raise HTTPException(
                status_code=404,
                detail="Stock price not found"
            )

        return result

    # POST — Insert new record
    @staticmethod
    @app.post("/stock-price")
    def create_stock_price(
            stock: StockPriceModel
    ):

        result = service.create_stock_price(
            stock.dict()
        )

        if result.get("message") == "Record already exists":
            raise HTTPException(
                status_code=400,
                detail="Duplicate record"
            )

        return result

    # PUT — Update existing record
    @staticmethod
    @app.put("/stock-price/{record_id}")
    def update_stock_price(
            record_id: str,
            stock: StockPriceModel
    ):

        result = service.update_stock_price(
            record_id,
            stock.dict()
        )

        if not result:
            raise HTTPException(
                status_code=404,
                detail="Record not found"
            )

        return result

    @staticmethod
    @app.post("/stock-price/upsert")
    def upsert_stock_price(stock: StockPriceModel):
        return service.upsert_stock_price(stock.model_dump())

    @staticmethod
    @app.post("/stock-price/bulk-upsert")
    def bulk_upsert_stock_prices(stocks: List[StockPriceModel]):
        payload = [stock.model_dump() for stock in stocks]
        return service.bulk_upsert_stock_prices(payload)
