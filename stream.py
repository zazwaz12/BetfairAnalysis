import ssl
import socket
import os
import logging
from dotenv import load_dotenv 
from session import BetfairLogin

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
        market_subscription_message = '{"op":"marketSubscription","marketFilter":{"bettingTypes":["ODDS"],"eventTypeIds":["4", "61420"],"turnInPlayEnabled":true,"marketTypes":["MATCH_ODDS"]},"marketDataFilter":{}}\r\n'
        ssock.sendall(market_subscription_message.encode())
        
        # Receive data from the socket
        while True:
            data = ssock.recv(1024)
            if not data:
                break
            print("Received:", data.decode())

print('Connection closed')
