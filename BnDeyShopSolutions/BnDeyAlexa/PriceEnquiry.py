from flask import Flask
from flask_ask_sdk.skill_adapter import SkillAdapter

from ask_sdk_core.skill_builder import SkillBuilder
from ask_sdk_core.dispatch_components import AbstractRequestHandler
from ask_sdk_core.utils import is_request_type, is_intent_name

from pymongo import MongoClient
from dotenv import load_dotenv

import os
import re

load_dotenv()

app = Flask(__name__)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
SKILL_ID = os.getenv("SKILL_ID")

client = MongoClient(MONGO_URI)
db = client["bndey_db"]

# Change this if your collection name is singular
collection = db["tally_stock_items"]
# collection = db["tally_stock_item"]

sb = SkillBuilder()


def number_words_to_digits(text):
    text = f" {text.lower()} "

    replacements = {
        " zero ": " 0 ",
        " one ": " 1 ",
        " two ": " 2 ",
        " too ": " 2 ",
        " to ": " 2 ",
        " three ": " 3 ",
        " four ": " 4 ",
        " five ": " 5 ",
        " six ": " 6 ",
        " seven ": " 7 ",
        " eight ": " 8 ",
        " nine ": " 9 ",
    }

    for word, digit in replacements.items():
        text = text.replace(word, digit)

    return " ".join(text.split())


def extract_item_and_size(item_text):
    item_text = number_words_to_digits(item_text)

    # Alexa often converts "ml" into full words
    item_text = item_text.replace("milliliters", " ml ")
    item_text = item_text.replace("milliliter", " ml ")
    item_text = item_text.replace("millilitres", " ml ")
    item_text = item_text.replace("millilitre", " ml ")
    item_text = item_text.replace("millilitre", " ML ")
    item_text = item_text.replace("liters", " litre ")
    item_text = item_text.replace("liter", " litre ")
    item_text = item_text.replace("litres", " litre ")
    item_text = item_text.replace("litre", " litre ")

    size_ml = None

    match = re.search(r"\b(60|90|180|200|250|275|330|375|500|650|700|750|1000)\b", item_text)

    if match:
        size_ml = int(match.group(1))
        item_name = item_text.replace(match.group(1), " ")
    else:
        item_name = item_text

    item_name = item_name.replace("ml", " ")
    item_name = item_name.replace("m l", " ")
    item_name = item_name.replace("litre", " ")
    item_name = item_name.replace("-", " ")
    item_name = " ".join(item_name.split())

    return item_name, size_ml


def normalize_for_search(text):
    text = text.lower()

    replacements = {
        "&": " and ",
        "-": " ",
        "_": " ",
        ".": " ",
        ",": " ",
        "'": "",
        "\"": "",
        "milliliters": " ",
        "milliliter": " ",
        "millilitres": " ",
        "millilitre": " ",
        "liters": " ",
        "liter": " ",
        "litres": " ",
        "litre": " ",
        "ml": " ",
        "m l": " ",
        "whisky": "whiskey",
        "whiskey": "whisky",
    }

    text = f" {text} "

    for old, new in replacements.items():
        text = text.replace(old, new)

    return " ".join(text.split())


def build_word_match_query(item_name, size_ml=None):
    normalized = normalize_for_search(item_name)
    words = normalized.split()

    ignored_words = {
        "the", "a", "an", "of", "and",
        "ml", "m", "l",
        "milliliter", "milliliters",
        "millilitre", "millilitres",
        "liter", "liters", "litre", "litres"
    }

    words = [word for word in words if word not in ignored_words]

    and_conditions = []

    for word in words:
        and_conditions.append({
            "$or": [
                {"brand": {"$regex": word, "$options": "i"}},
                {"stock_item_name": {"$regex": word, "$options": "i"}}
            ]
        })

    if size_ml:
        and_conditions.append({"size_ml": size_ml})

    if not and_conditions:
        return {}

    return {"$and": and_conditions}


def find_stock_item(item_name, size_ml=None):
    query = build_word_match_query(item_name, size_ml)

    print("Mongo word query:", query)

    result = collection.find_one(query)

    if result:
        return result

    # fallback 1: try direct brand search
    direct_brand_query = {
        "brand": {
            "$regex": normalize_for_search(item_name),
            "$options": "i"
        }
    }

    if size_ml:
        direct_brand_query["size_ml"] = size_ml

    print("Fallback brand query:", direct_brand_query)

    result = collection.find_one(direct_brand_query)

    if result:
        return result

    # fallback 2: try direct stock item search
    direct_item_query = {
        "stock_item_name": {
            "$regex": normalize_for_search(item_name),
            "$options": "i"
        }
    }

    if size_ml:
        direct_item_query["size_ml"] = size_ml

    print("Fallback item query:", direct_item_query)

    result = collection.find_one(direct_item_query)

    if result:
        return result

    # fallback 3: try relaxed search without size
    relaxed_query = build_word_match_query(item_name, None)

    print("Relaxed query without size:", relaxed_query)

    result = collection.find_one(relaxed_query)

    return result


class LaunchRequestHandler(AbstractRequestHandler):

    def can_handle(self, handler_input):
        return is_request_type("LaunchRequest")(handler_input)

    def handle(self, handler_input):
        speech = (
            "Welcome to Dey inventory. "
            "You can ask, rate of Bacardi Mango Chilli 375 ml."
        )

        return (
            handler_input.response_builder
            .speak(speech)
            .ask(speech)
            .response
        )


class GetRateIntentHandler(AbstractRequestHandler):

    def can_handle(self, handler_input):
        return is_intent_name("GetRateIntent")(handler_input)

    def handle(self, handler_input):
        slots = handler_input.request_envelope.request.intent.slots

        item_text = None

        if slots.get("item") and slots["item"].value:
            item_text = slots["item"].value

        if not item_text:
            speech = "Please say the stock item name. For example, rate of Bacardi Mango Chilli 375 ml."
            return (
                handler_input.response_builder
                .speak(speech)
                .ask(speech)
                .response
            )

        item_name, size_ml = extract_item_and_size(item_text)

        print("Alexa said:", item_text)
        print("Extracted item:", item_name)
        print("Extracted size:", size_ml)

        result = find_stock_item(item_name, size_ml)

        if result:
            stock_name = result.get("stock_item_name", "stock item")
            rate = result.get("rate", 0)
            quantity = result.get("quantity", 0)
            size = result.get("size_ml", "")

            speech = (
                f"{stock_name}. "
                f"{size} ml rate is {rate} rupees. "
                f"Available quantity is {quantity}."
            )
        else:
            if size_ml:
                speech = f"I could not find {item_name} in {size_ml} ml."
            else:
                speech = f"I could not find {item_name}. Please also say the size."

        return (
            handler_input.response_builder
            .speak(speech)
            .set_should_end_session(False)
            .response
        )


class HelpIntentHandler(AbstractRequestHandler):

    def can_handle(self, handler_input):
        return is_intent_name("AMAZON.HelpIntent")(handler_input)

    def handle(self, handler_input):
        speech = "You can say, rate of Bacardi Mango Chilli 375 ml."

        return (
            handler_input.response_builder
            .speak(speech)
            .ask(speech)
            .response
        )


class FallbackIntentHandler(AbstractRequestHandler):

    def can_handle(self, handler_input):
        return is_intent_name("AMAZON.FallbackIntent")(handler_input)

    def handle(self, handler_input):
        speech = "Sorry, I did not understand. Say, rate of Bacardi Mango Chilli 375 ml."

        return (
            handler_input.response_builder
            .speak(speech)
            .ask(speech)
            .response
        )


class CancelOrStopIntentHandler(AbstractRequestHandler):

    def can_handle(self, handler_input):
        return (
            is_intent_name("AMAZON.CancelIntent")(handler_input)
            or is_intent_name("AMAZON.StopIntent")(handler_input)
        )

    def handle(self, handler_input):
        speech = "Goodbye."

        return (
            handler_input.response_builder
            .speak(speech)
            .response
        )


sb.add_request_handler(LaunchRequestHandler())
sb.add_request_handler(GetRateIntentHandler())
sb.add_request_handler(HelpIntentHandler())
sb.add_request_handler(FallbackIntentHandler())
sb.add_request_handler(CancelOrStopIntentHandler())

skill_adapter = SkillAdapter(
    skill=sb.create(),
    skill_id=SKILL_ID,
    app=app
)

skill_adapter.register(app=app, route="/alexa")


@app.route("/")
def health_check():
    return "B N Dey Alexa Inventory API is running"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5060, debug=True)