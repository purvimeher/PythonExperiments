import json

import requests


class ApiRequests:

    def __init__(self, url):
        self.url = url

    def getAllUsers(self):
        response = requests.get(self.url)
        return json.dumps(response.json(), indent=4)

    def getUserById(self, id):
        response = requests.get(f'{self.url}/{id}')
        return json.dumps(response.json(), indent=4)

    def createUser(self, jsonData):
        response = requests.post(self.url, json=jsonData)
        return response

    def loginWithAwtToken(self, url, jsonData):
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, json=jsonData, headers=headers)
        json.dumps(response.json(), indent=4)
        accessToken = response.json()['access_token']
        userProfileUrl = 'https://api.escuelajs.co/api/v1/auth/profile'
        userProfileData = {
            "Authorization": ""
        }
        userProfileData["Authorization"] = "Bearer " + accessToken
        response = requests.get(userProfileUrl, userProfileData)
        return json.dumps(response.json(), indent=4)




# usage from here
apiRequests = ApiRequests('https://api.escuelajs.co/api/v1/users')
# print(apiRequests.getAllUsers())
print(apiRequests.getUserById(7))
testData = {
    "name": "Nicolas",
    "email": "nico@gmail.com",
    "password": "1234",
    "avatar": "https://picsum.photos/800"
}

response = apiRequests.createUser(testData)
print(f'Status code of Creating User Response is :: {response.status_code}')
print(f'Final Response after creating user is ::  {json.dumps(response.json(), indent=4)}')
loginData = {
    "email": "john@mail.com",
    "password": "changeme"
}
print(
    f'User profile Response ::  {apiRequests.loginWithAwtToken('https://api.escuelajs.co/api/v1/auth/login', loginData)}')
