from flask import Flask
from ask_sdk_core.skill_builder import SkillBuilder
from ask_sdk_core.dispatch_components import AbstractRequestHandler
from ask_sdk_core.utils import is_request_type, is_intent_name
from flask_ask_sdk.skill_adapter import SkillAdapter
from pymongo import MongoClient

app = Flask(__name__)

@app.route("/")
def home():
    return "B N Dey Alexa Flask app is running"


client = MongoClient("mongodb://localhost:27017/")
db = client["bndey_db"]
inventory = db["tally_stock_items"]


class LaunchRequestHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return is_request_type("LaunchRequest")(handler_input)

    def handle(self, handler_input):
        speech = "Welcome to B N Dey inventory. Ask me stock for a brand and size."
        return handler_input.response_builder.speak(speech).ask(speech).response


class StockIntentHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return is_intent_name("StockIntent")(handler_input)

    def handle(self, handler_input):
        slots = handler_input.request_envelope.request.intent.slots

        brand_slot = slots.get("brand")
        size_slot = slots.get("size")

        brand = brand_slot.value if brand_slot and brand_slot.value else None
        size = int(size_slot.value) if size_slot and size_slot.value else None

        if not brand:
            speech = "Please tell me the brand name."
            return handler_input.response_builder.speak(speech).ask(speech).response

        query = {
            "Brand": {"$regex": brand, "$options": "i"}
        }

        if size:
            query["Size_ML"] = size

        result = inventory.find_one(query)

        if result:
            qty = result.get("Qty", 0)
            actual_brand = result.get("Brand", brand)
            actual_size = result.get("Size_ML", size)
            speech = f"{actual_brand} {actual_size} ml has {qty} bottles in stock."
        else:
            speech = f"I could not find stock for {brand}."

        return handler_input.response_builder.speak(speech).response


sb = SkillBuilder()
sb.add_request_handler(LaunchRequestHandler())
sb.add_request_handler(StockIntentHandler())

skill_adapter = SkillAdapter(
    skill=sb.create(),
    skill_id="amzn1.ask.skill.5a8cc59b-4708-4b60-a87d-ff079349053e",
    app=app
)

skill_adapter.register(app=app, route="/alexa")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050, debug=True)