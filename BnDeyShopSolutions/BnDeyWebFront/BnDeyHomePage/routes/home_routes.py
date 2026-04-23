from flask import Blueprint, render_template, redirect, url_for

home_bp = Blueprint("home", __name__)


@home_bp.route("/")
def home():
    return redirect(url_for("stock_prices.show_stock_prices"))


@home_bp.route("/dashboard")
def dashboard():
    return render_template("dashboard.html")