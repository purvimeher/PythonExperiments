from .home_routes import home_bp
from .stock_prices_routes import stock_prices_bp
from .current_inventory_routes import current_inventory_bp
from .incoming_stock_routes import incoming_stock_bp
from .daily_sales_routes import daily_sales_bp
from .monthly_sales_routes import monthly_sales_bp


def register_blueprints(app):
    app.register_blueprint(home_bp)
    app.register_blueprint(stock_prices_bp)
    app.register_blueprint(current_inventory_bp)
    app.register_blueprint(incoming_stock_bp)
    app.register_blueprint(daily_sales_bp)
    app.register_blueprint(monthly_sales_bp)