from flask import Flask
from ask_sdk_core.skill_builder import SkillBuilder
from ask_sdk_core.dispatch_components import AbstractRequestHandler
from ask_sdk_core.utils import is_request_type, is_intent_name
from flask_ask_sdk.skill_adapter import SkillAdapter
from pymongo import MongoClient
import re

app = Flask(__name__)

client = MongoClient("mongodb://localhost:27017/")
db = client["bndey_db"]

stock_items = db["tally_stock_items"]


class LaunchRequestHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return is_request_type("LaunchRequest")(handler_input)

    def handle(self, handler_input):
        speech = "Welcome to B N Dey inventory. Ask me stock details for a brand."
        return handler_input.response_builder.speak(speech).ask(speech).response


class StockDetailsIntentHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return is_intent_name("StockIntent")(handler_input)

    def handle(self, handler_input):
        slots = handler_input.request_envelope.request.intent.slots

        brand_name = None
        size_ml = None

        if slots.get("brandName") and slots["brandName"].value:
            brand_name = slots["brandName"].value.strip()

        if slots.get("sizeMl") and slots["sizeMl"].value:
            size_ml = int(slots["sizeMl"].value)

        if not brand_name:
            speech = "Please tell me the brand name."
            return handler_input.response_builder.speak(speech).ask(speech).response

        query = {
            "brand": {
                "$regex": re.escape(brand_name),
                "$options": "i"
            }
        }

        if size_ml:
            query["size_ml"] = size_ml

        item = stock_items.find_one(query)

        if not item:
            speech = f"I could not find stock details for {brand_name}."
            return handler_input.response_builder.speak(speech).response

        stock_item_name = item.get("stock_item_name", "Unknown item")
        quantity = item.get("quantity", 0)
        size = item.get("size_ml", "")
        rate = item.get("rate", 0)
        unit = item.get("unit", "NOS")
        amount = item.get("amount", 0)

        speech = (
            f"{stock_item_name}. "
            f"Size is {size} ml. "
            f"Available quantity is {quantity} {unit}. "
            f"Rate is rupees {rate}. "
            f"Amount is rupees {abs(amount)}."
        )

        return handler_input.response_builder.speak(speech).response


class HelpIntentHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return is_intent_name("AMAZON.HelpIntent")(handler_input)

    def handle(self, handler_input):
        speech = "You can ask, stock details for Amrut Fusion 750 ml."
        return handler_input.response_builder.speak(speech).ask(speech).response


class CancelStopIntentHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return (
                is_intent_name("AMAZON.CancelIntent")(handler_input)
                or is_intent_name("AMAZON.StopIntent")(handler_input)
        )

    def handle(self, handler_input):
        speech = "Goodbye."
        return handler_input.response_builder.speak(speech).response


sb = SkillBuilder()

sb.add_request_handler(LaunchRequestHandler())
sb.add_request_handler(StockDetailsIntentHandler())
sb.add_request_handler(HelpIntentHandler())
sb.add_request_handler(CancelStopIntentHandler())

skill_adapter = SkillAdapter(
    skill=sb.create(),
    skill_id="amzn1.ask.skill.5a8cc59b-4708-4b60-a87d-ff079349053e",
    app=app
)

skill_adapter.register(app=app, route="/alexa")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050)
