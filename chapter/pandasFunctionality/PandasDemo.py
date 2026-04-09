import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import plotly.express as px


class PandaFunctionality:

    def plotWithPanda(self):
        df1 = pd.read_csv('/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/pandasFunctionality/df1', index_col=0)
        df2 = pd.read_csv('/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/pandasFunctionality/df2')
        _stockLevels = pd.read_csv('/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/pandasFunctionality/StockLevels')
        # df2.plot.area(alpha=0.4)
        _stockLevels.plot.area(ax=plt.gca())
        plt.show()


    def plotSalesDataWithPandas(self):
        data = {
            'product': ['Old Monk', 'Jhonny Walker', 'Chivas Regal', 'Blenders Pride', 'Blue Label', 'Sula Wines'],
            'units': ['375ml', '700ml', '1000ml', '375ml', '1000ml', '600ml'],
            'initial_Stock': ['1000', '1000', '1000', '1000', '1000', '1000'],
            'sales': [100, 200, 150, 300, 250, 200],
            'end_Of_Day_stock': [900, 800, 850, 700, 750, 800],
            'date': ['2026-04-07', '2026-04-07', '2026-04-07', '2026-04-07', '2026-04-07', '2026-04-07']
        }

        df = pd.DataFrame(data)
        print(df)
        sales_by_product = df.groupby('product')['sales'].sum()
        print(sales_by_product)

        top_products = df.groupby('product')['sales'].sum().sort_values(ascending=False)
        print(top_products)

        sales_by_product.plot(kind='bar')
        plt.title('Sales by Product')
        plt.xlabel('Product')
        plt.ylabel('Total Sales')
        plt.show()

    def checkPerformanceFromStockLevels(self):
        df = pd.read_csv('sales.csv', parse_dates=['date'])
        result = df.groupby('Name')['Sold'].sum()
        print(result)

        df['month'] = df['date'].dt.to_period('M')
        monthly_sales = df.groupby('month')['Sold'].sum()

        sales_by_product = df.groupby('Name')['Sold'].sum().sort_values(ascending=False)

        top_products = sales_by_product.head(5)

        avg_sales = df.groupby('Name')['Sold'].mean()

        sales_by_product.plot(kind='bar')
        plt.title('Sales by Product')
        plt.xlabel('Product')
        plt.ylabel('Total Sales')
        plt.show()

        sales_by_product.plot(kind='pie', autopct='%1.1f%%')
        plt.title('Sales Contribution by Product')
        plt.ylabel('')
        plt.show()


    def plotWithPloty(self):
        _stockLevels = pd.read_csv('/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/pandasFunctionality/StockLevels')
        fig = px.pie(_stockLevels,names='Name', values='Sold',
             title='Volume of Sales,2000', hole=.3
            )
        fig.update_traces(hovertemplate="Name: %{label} : <br>Sold: %{value}")
        fig.show()


pandaDemo = PandaFunctionality()
# pandaDemo.plotWithPanda()
# pandaDemo.plotSalesDataWithPandas()
# pandaDemo.checkPerformanceFromStockLevels()
pandaDemo.plotWithPloty()