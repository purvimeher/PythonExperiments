import json
import requests


class MakeApiRequest:

    def __init__(self, url):
        self.url = url


    def makeAGetRequest(self):
        try:
            response = requests.get(self.url)
            return json.dumps(response.json(), indent=4)
        except Exception as ee:
            return f"Message : {ee}"


makeApiRequest = MakeApiRequest('https://api.first.org/data/v1/countries')
print(makeApiRequest.makeAGetRequest())