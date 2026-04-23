from BnDeyShopSolutions.BnDeyOperations.InventoryManager.misc.SalesGraphsGenerator import SalesGraphsGenerator
from BnDeyShopSolutions.BnDeyOperations.InventoryManager.misc.SalesReportsGenerator import SalesReportsGenerator
from BnDeyShopSolutions.BnDeyOperations.configs.ConfigLoader import ConfigLoader


class BnDeyMainReportsGenerator:
    def __init__(self, db="bndey_db"):
        self.db = db
        config = ConfigLoader.load_config()
        self.daily_sales_csv_file = config["csvFileNames"]["daily_sales_csv"]
        self.salesGraphsGenerator = SalesGraphsGenerator(self.db)
        self.salesReportGenerator = SalesReportsGenerator(self.daily_sales_csv_file,self.db)
        self.salesReportGenerator.calculateTotalMontlySalesAndSaveIntoDb()
        self.salesReportGenerator.generateMonthlySalesReportintoCsvAndDb()

    def generateBarChartReportChart(self):
        self.salesGraphsGenerator.generateMonthlySalesBarChartReports()

    def generatePieChartReportChart(self):
        self.salesGraphsGenerator.generateMonthlySalesPieCharts()

    def generateTotalMonthlyReportChart(self):
        self.salesGraphsGenerator.generatePlotyForTotalMonthlySales()





bndeyMainReportsGenerator = BnDeyMainReportsGenerator()
# bndeyMainReportsGenerator.generateBarChartReportChart()
bndeyMainReportsGenerator.generatePieChartReportChart()
bndeyMainReportsGenerator.generateTotalMonthlyReportChart()