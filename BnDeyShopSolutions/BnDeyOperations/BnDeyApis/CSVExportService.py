import io

import pandas as pd

from BnDeyShopSolutions.BnDeyOperations.BnDeyApis.Database import Database


class CSVExportService:

    def __init__(self):
        db = Database()
        self.collection = db.get_collection("daily_sales")

    def fetch_data(self):
        data = list(
            self.collection.find(
                {},
                {"_id": 0}
            )
        )

        return data

    def generate_csv(self):
        records = self.fetch_data()

        if not records:
            return None

        df = pd.DataFrame(records)

        output = io.StringIO()

        df.to_csv(
            output,
            index=False
        )

        return output.getvalue()
