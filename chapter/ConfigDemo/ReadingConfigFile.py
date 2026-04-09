from configparser import ConfigParser

class ConfigReader:
    def __init__(self):
        pass

    def readConfigFile(self):
        config = ConfigParser()
        config.read('/Users/mehermeka/PycharmProjects/PythonProjectSelenium/config.ini')
        return config


configparser = ConfigReader()
_config = configparser.readConfigFile()
print(_config.sections())
