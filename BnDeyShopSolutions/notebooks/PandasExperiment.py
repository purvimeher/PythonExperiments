import pandas as pd

class PandasExperiment:
        def __init__(self) -> None:
            self.df = None
            self.aggregated_data = None



        def readCsv(self, filename):
            self.df = pd.read_csv(filename)

        def aggregateValuesByCategory(self):
            self.aggregated_data = self.df.groupby(['Date','Brand_Category','Brand']).agg(
                total_sales=('Qty', 'sum')
            )

            print(self.aggregated_data)

        def rankSalesByDate(self):
            self.df['Rank Date according to Qty'] = self.df.groupby('Brand')['Qty'].transform(lambda x: x.rank(ascending=False))
            print(self.df)

        def calculateTotalSales(self):
            self.df['Total Revenue'] = sum (self.df['Price'] * self.df['Qty'])
            print(self.df)



pandasexperiment = PandasExperiment()
pandasexperiment.readCsv('/Users/mehermeka/PycharmProjects/PythonProjectSelenium/BnDeyShopSolutions/notebooks/DailySalesExperiment.csv')
# pandasexperiment.aggregateValuesByCategory()
# pandasexperiment.rankSalesByDate()
# pandasexperiment.rankSalesByDateBySales()
pandasexperiment.calculateTotalSales()



