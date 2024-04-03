import requests

url = 'https://api.games.betfair.com/rest/v1'
headers = {
    'gamexAPIAgent': 'maryBrown@AOL.com.myGames.4.0',
    'gamexAPIAgentInstance': '0d69ee8290ee2f9b336c1f060e3497a5'
}
params = {
    'username': 'sampleuser'
}
auth = ('username', 'password')  # Replace 'username' and 'password' with your actual credentials

response = requests.get(url, headers=headers, params=params, auth=auth)

print(response.text)  # This will print the response body
