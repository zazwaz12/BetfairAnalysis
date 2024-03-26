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
    
    def update_environment_variables(self, filename):
        with open(filename, 'r') as file:
            lines = file.readlines()
            
        # Remove the last line
        lines = lines[:-1]
        lines.append(f'SESSION="{session}"\n')

        with open(filename, 'w') as file:
                file.writelines(lines)
        load_dotenv()
    
    def instantiate_session(self):
        with open(".env", "r") as file:
            for line in file:
                if line.startswith("SESSION="):
                    session = line.split("=", 1)[1].replace('"', "").strip()
                    break
        os.environ["SESSION"] = session
         

loggedIn = BetfairLogin()
session = loggedIn.get_session()
logger.info(f"new session attempt: {loggedIn.get_status()}")
loggedIn.update_environment_variables(".env")

import betfairlightweight

trading = betfairlightweight.APIClient(loggedIn.username, loggedIn.password, os.getenv("APPLICATION_KEY"))

trading.login_interactive()
event_types = trading.betting.list_event_types()

markets = trading.betting.list_market_catalogue(filter={"event_type_id":"4"})
markets = [m.market_id for m in markets]
print(markets)
info = trading.betting.list_market_book(markets)

selectionID = [(x.selection_id) for x in [book.runners for book in info][0]][0]
instruction = [
        {
            "selectionId": f"{selectionID}",
            "side": "BACK",
            "orderType": "LIMIT",
            "limitOrder": {
                "size": "10",
                "price": "2",
                "persistenceType": "PERSIST"
            }
        }
    ]
print(markets[0], instruction)

trading.betting.place_orders(markets[0], instruction)
#print([o.selection_id for o in trading.betting.list_current_orders()])
#print([i.event for i in trading.betting.list_events()])
'''"instructions": [
                {
                    "selectionId": "",
                    "side": "BACK",
                    "orderType": "LIMIT",
                    "limitOrder": {
                        "size": "10",
                        "price": "2",
                        "persistenceType": "PERSIST"
                    }
                }
            ]

'''
