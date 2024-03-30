import ssl
import socket
import os
import logging
from dotenv import load_dotenv 
from CallerModules.session import BetfairLogin
from CallerModules.kafka import read_the_json
import time
from datetime import datetime, timedelta
import json

# Get the current time
current_time = datetime.now()

# Add 12 hours to the current time
end_time = current_time + timedelta(hours=1)

# Format the timestamps in the required format (ISO 8601)
current_time_str = current_time.strftime("%Y-%m-%dT%H:%M:%SZ")
end_time_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

# Set up logging
log_format = '%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s'
logging.basicConfig(filename='betfair.log', level=logging.INFO, format=log_format, datefmt='%d-%b %H:%M')
logger = logging.getLogger()

loggedIn = BetfairLogin()
session = loggedIn.get_session()
logger.info(f"new session attempt: {loggedIn.get_status()}")
loggedIn.update_environment_variables(".env")
loggedIn.instantiate_session()

# Now you can access the variables defined in your .env file like this
app_key = os.getenv("APPLICATION_KEY")
session = os.getenv("SESSION")
print(session)

# Socket connection options - this is Betfair specific
options = {
    'host': 'stream-api.betfair.com',
    'port': 443
}

count = 0
# Establish connection to the socket
context = ssl.create_default_context()
with socket.create_connection((options['host'], options['port'])) as sock:
    with context.wrap_socket(sock, server_hostname=options['host']) as ssock:
        # Send authentication message
        auth_message = f'{{"op": "authentication", "appKey": "{app_key}", "session":"{session}"}}\r\n'
        ssock.sendall(auth_message.encode())
        # event id for cricket, tennis and AFL are: 2, 4 and 61420
        market_subscription_message = '{"op":"marketSubscription", "marketFilter":{"eventTypeIds":["2", "4", "61420"],"marketTypes":["MATCH_ODDS"], "inPlay":true},"marketDataFilter":{"ladderLevels": 1, "fields":["EX_BEST_OFFERS", "SP_TRADED"]}}\r\n'
        ssock.sendall(market_subscription_message.encode())
        # Set initial time and flag
        start_time = time.time()
        elapsed_time = 0
        ten_seconds_passed = False
        complete_json = ""
        while True:
            data = ssock.recv(1024)
            if not data:
                break
            count += 1
            json_str = data.decode()
            if "HEARTBEAT" not in json_str:
                complete_json += json_str
                logger.info(json_str + "\n\n")
            else:
                print(json_str)
                # Write the received JSON string to the file
                with open("unprocessedmarkets.json", "a") as outfile:
                    if len(complete_json) > 0:
                        outfile.write(complete_json + "\n\n\n")
                        complete_json = ""

                # Check if 10 seconds have passed
                elapsed_time = time.time() - start_time
                if elapsed_time >= 10:
                    ten_seconds_passed = True
                try:
                    # true every 10 seconds
                    if ten_seconds_passed:
                        # Call the json reading to Kafka
                        read_the_json()

                        # Reset the timer and flag
                        complete_json = ""
                        start_time = time.time()
                        elapsed_time = 0
                        ten_seconds_passed = False
                except:
                    logger.info(f"Not adding: {complete_json}")
            


print('Connection closed')
