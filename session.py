import os
import requests
import json
from urllib.parse import urlencode
import logging
from dotenv import load_dotenv

# Set up logging
log_format = '%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s'
logging.basicConfig(filename='betfair.log', level=logging.INFO, format=log_format, datefmt='%d-%b %H:%M')
logger = logging.getLogger()


class BetfairLogin:
    def __init__(self):
        self.url = "https://identitysso-cert.betfair.com/api/certlogin"
        self.headers = {
            "X-Application": os.getenv("APPLICATION_KEY"),
            "Content-Type": "application/x-www-form-urlencoded"
        }
        self.username = os.getenv("USERNAME")
        self.password = os.getenv("PASSWORD")
        self.encoded_username = urlencode({"username": self.username})
        self.encoded_password = urlencode({"password": self.password})
        self.data = f"{self.encoded_username}&{self.encoded_password}"
        self.cert = (os.getenv("PEM_PATH"), os.getenv("PEM_PATH"))

    def login_session(self):
        logger.info("logging in")
        response = requests.post(self.url, headers=self.headers, data=self.data, cert=self.cert)
        return json.loads(response.text)

    def get_session(self):
        logger.info("retrieving sessionToken")
        login_dict = self.login_session()
        return login_dict.get("sessionToken")

    def get_status(self):
        logger.info("retrieving login status")
        login_dict = self.login_session()
        return login_dict.get("loginStatus")

loggedIn = BetfairLogin()
session = loggedIn.get_session()

filename = ".env"

with open(filename, 'r') as file:
        lines = file.readlines()
    
    # Remove the last line
lines = lines[:-1]
lines.append(f'SESSION="{session}"\n')

with open(filename, 'w') as file:
        file.writelines(lines)
load_dotenv()
print(loggedIn.get_status())


