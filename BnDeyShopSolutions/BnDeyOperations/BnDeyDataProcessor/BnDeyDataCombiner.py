import pandas as pd
import glob
import os

from BnDeyShopSolutions.BnDeyOperations.configs.ConfigLoader import ConfigLoader


class CSVCombiner:

    def __init__(self, folder_path, output_file):
        self.folder_path = folder_path
        self.output_file = output_file

    def combine_csv_files(self):

        # Get all CSV files
        csv_files = glob.glob(os.path.join(self.folder_path, "*.csv"))

        if not csv_files:
            print("No CSV files found.")
            return

        print(f"Found {len(csv_files)} CSV files")

        dataframes = []

        for file in csv_files:
            print("Reading:", file)

            df = pd.read_csv(file)

            dataframes.append(df)

        # Combine all files
        combined_df = pd.concat(dataframes, ignore_index=True)

        print("Total records before dedup:", len(combined_df))

        # Remove duplicates
        combined_df.drop_duplicates(
            subset=[
                "Brand",
                "Brand_Category",
                "Size_ML",
                "Date"
            ],
            keep="last",
            inplace=True
        )

        print("Total records after dedup:", len(combined_df))

        # Save output
        combined_df.to_csv(self.output_file, index=False)

        print("Combined file saved to:", self.output_file)


if __name__ == "__main__":
    config = ConfigLoader.load_config()
    daily_sales_folder = config["paths"]["daily_sales_csv_path"]
    output_folder = config["paths"]["output_fdr"]
    combiner = CSVCombiner(
        folder_path=daily_sales_folder,
        output_file=f"{output_folder}combined_daily_sales.csv"
    )

    combiner.combine_csv_files()