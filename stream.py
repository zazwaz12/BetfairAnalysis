import ssl
import socket
import os
from dotenv import load_dotenv 

# Load environment variables from .env file
load_dotenv()

# Now you can access the variables defined in your .env file like this
app_key = os.getenv("APPLICATION_KEY")
session = os.getenv("SESSION")

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
        order_subscription_message = '{"op":"orderSubscription","orderFilter":{"includeOverallPosition":false,"customerStrategyRefs":["betstrategy1"],"partitionMatchedByStrategyRef":true},"segmentationEnabled":true}\r\n'
        market_subscription_message = '{"op":"marketSubscription","id":2,"marketFilter":{"bspMarket":true,"bettingTypes":["ODDS"],"eventTypeIds":["2", "4", "61420"],"turnInPlayEnabled":true,"marketTypes":["MATCH_ODDS"]},"marketDataFilter":{}}\r\n'
        ssock.sendall(order_subscription_message.encode())
        ssock.sendall(market_subscription_message.encode())
        
        # Receive data from the socket
        while True:
            data = ssock.recv(1024)
            if not data:
                break
            print("Received:", data.decode())

print('Connection closed')
