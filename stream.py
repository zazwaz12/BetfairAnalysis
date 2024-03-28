import ssl
import socket
import os
import logging
from dotenv import load_dotenv 
from session import BetfairLogin

from datetime import datetime, timedelta

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
        print("Connected")
        
        # Send authentication message
        auth_message = f'{{"op": "authentication", "appKey": "{app_key}", "session":"{session}"}}\r\n'
        ssock.sendall(auth_message.encode())
        
        # Subscribe to order/market stream
        # event id for cricket, tennis and AFL are: 2, 4 and 61420
        market_subscription_message = '{"op":"marketSubscription","marketFilter":{"eventTypeIds":["2", "4", "61420","marketTypes":["MATCH_ODDS"], "inPlay":true},"marketDataFilter":{"ladderLevels": 1, "fields":["EX_BEST_OFFERS", "SP_TRADED"]}}\r\n'
        #market_subscription_message = market_subscription_message.replace("current_time_str", current_time_str)
        #market_subscription_message = market_subscription_message.replace("end_time_str", end_time_str)
        print(market_subscription_message)
        ssock.sendall(market_subscription_message.encode())
        
        while True:
            data = ssock.recv(1024)
            if not data:
                break
            count += 1
            json_str = data.decode()
            if json_str != "":
                # Write the received JSON string to the file
                with open("unprocessedmarkets.json", "a") as outfile:  # Use 'a' for append mode
                    outfile.write(json_str + '\n')


print('Connection closed')
