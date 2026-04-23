import json


class ConfigLoader:

    @staticmethod
    def load_config(config_file="/Users/mehermeka/PycharmProjects/PythonProjectSelenium/BnDeyShopSolutions/BnDeyOperations/configs/config.json"):
        with open(config_file, "r") as file:
            config = json.load(file)

        return config