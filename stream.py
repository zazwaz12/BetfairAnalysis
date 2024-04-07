import ssl
import socket
import os
import time
import requests
import json
from urllib.parse import urlencode
from dotenv import load_dotenv


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
        response = requests.post(self.url, headers=self.headers, data=self.data, cert=self.cert)
        return json.loads(response.text)

    def get_session(self):
        login_dict = self.login_session()
        return login_dict.get("sessionToken")

    def get_status(self):
        login_dict = self.login_session()
        return login_dict.get("loginStatus")
    
    def update_environment_variables(self, filename, session):
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

"""
initial_clk is the given on a new connection
clk is updated on a market change
using the two together, an image of market odds at a certain time can be referenced
used as a cache, to only retrieve changes on the last image
"""
initial_clk =""
clk = "AAAAAAAA"

#creating new session
sess = BetfairLogin()
session = sess.get_session()

#updating and persisting environment variables
sess.update_environment_variables(".env", session)
sess.instantiate_session()

#now variables defined in .env file can be accessed
app_key = os.getenv("APPLICATION_KEY")
while os.getenv("SESSION") == "None":
    print(os.getenv("SESSION"))
    sess.get_session()
    sess.update_environment_variables(".env", session)
    sess.instantiate_session()
session = os.getenv("SESSION")

#socket connection options - this is Betfair specific
options = {
    'host': 'stream-api.betfair.com',
    'port': 443
}

#establish connection to the socket
context = ssl.create_default_context()
with socket.create_connection((options['host'], options['port'])) as sock:
    with context.wrap_socket(sock, server_hostname=options['host']) as ssock:
        #authentication message
        auth_message = f'{{"op": "authentication", "appKey": "{app_key}", "session":"{session}"}}\r\n'
        ssock.sendall(auth_message.encode())

        #subscription message
        market_subscription_message = (
            '{"op":"marketSubscription",'
            '"marketFilter":{"eventTypeIds":["2", "4", "61420"],"marketTypes":["MATCH_ODDS"], "inPlay":true},'
            '"marketDataFilter":{"ladderLevels": 1, "fields":["EX_BEST_OFFERS"]}}\r\n'
        )
        print(market_subscription_message)
        ssock.sendall(market_subscription_message.encode())


        #set initial time and flag
        start_time = time.time()
        elapsed_time = 0
        ten_seconds_passed = False
        complete_json = ""
        while True:
            data = ssock.recv(1024)
            if not data:
                break
            json_str = data.decode()
            if "HEARTBEAT" not in json_str and "connection" not in json_str and "status" not in json_str:
                complete_json += json_str
            else:
                # Write the received JSON string to the file
                with open("newStream.json", "a") as outfile:
                    if len(complete_json) > 0:
                        outfile.write(complete_json)
                        complete_json = ""

