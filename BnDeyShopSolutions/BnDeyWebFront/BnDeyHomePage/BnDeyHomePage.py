from flask import Flask
from config import Config
from extensions import init_mongo
from routes import register_blueprints


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    init_mongo(app)
    register_blueprints(app)

    return app


app = create_app()

if __name__ == "__main__":
    app.run()