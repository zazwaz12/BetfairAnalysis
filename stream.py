import ssl
import socket
import os
from CallerModules.logSetup import logger
from CallerModules.session import BetfairLogin
from CallerModules.kafka import read_the_json
import time

"""
initial_clk is the given on a new connection
clk is updated on a market change
using the two together, an image of market odds at a certain time can be referenced
used as a cache, to only retrieve changes on the last image
"""
initial_clk ="G9SezqQFHYfNsJUFFp/Z+p8F"
clk = "AAAAAAAA"

#creating new session
sess = BetfairLogin()
logger.info("requesting a session")
session = sess.get_session()
logger.info("session created")

#updating and persisting environment variables
sess.update_environment_variables(".env", session)
sess.instantiate_session()

#now variables defined in .env file can be accessed
app_key = os.getenv("APPLICATION_KEY")
print(os.getenv("SESSION"))
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
        logger.info("sending stream exchange authentication")
        ssock.sendall(auth_message.encode())

        #subscription message
        market_subscription_message = (
            '{"op":"marketSubscription", "initialClk":"' + initial_clk + '","clk":"' + clk + '",'
            '"marketFilter":{"eventTypeIds":["2", "4", "61420"],"marketTypes":["MATCH_ODDS"], "inPlay":true},'
            '"marketDataFilter":{"ladderLevels": 1, "fields":["EX_BEST_OFFERS", "SP_TRADED"]}}\r\n'
        )
        ssock.sendall(market_subscription_message.encode())
        logger.info("sending market subscription message")

        with open("unprocessedmarkets.json", "w") as outfile:
            logger.info("clearing json file")

        #set initial time and flag
        start_time = time.time()
        elapsed_time = 0
        ten_seconds_passed = False
        complete_json = ""
        while True:
            data = ssock.recv(1024)
            if not data:
                logger.info("no response")
                break
            json_str = data.decode()
            if "HEARTBEAT" not in json_str and "connection" not in json_str and "status" not in json_str:
                complete_json += json_str
            else:
                # Write the received JSON string to the file
                with open("unprocessedmarkets.json", "a") as outfile:
                    if len(complete_json) > 0:
                        logger.info("sending market snapshot to json")
                        outfile.write(complete_json)
                        complete_json = ""

                        # Check if 10 seconds have passed
                elapsed_time = time.time() - start_time
                if elapsed_time >= 10:
                    ten_seconds_passed = True
                try:
                    # true every 10 seconds
                    if ten_seconds_passed:
                        # Call the json reading to Kafka
                        read_the_json(clk_value=clk, initialClk_value=initial_clk)

                        # Reset the timer and flag
                        complete_json = ""
                        start_time = time.time()
                        elapsed_time = 0
                        ten_seconds_passed = False
                except:
                    logger.info(f"not adding:\n\t{complete_json}")
            


logger.info("connection closed")
