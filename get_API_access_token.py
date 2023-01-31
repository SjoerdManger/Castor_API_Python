# basic API call authentication
# needs the following packages:
# - requests: pip install requests (using: requests 2.28.1)
# - dotenv: pip3 install python-dotenv (using: python-dotenv-0.20.0)

import requests as req
import json  # needed to convert API call to JSON


def get_access_token(client_id, client_secret):

    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials"
    }

    token_url = "https://data.castoredc.com/oauth/token"
    response = req.post(token_url, data)
    json_response = json.loads(response.text)
    access_token = json_response["access_token"]

    return access_token

