import ssl
import socket
import os
from running.ops.logSetup import logger
from running.ops.session import BetfairLogin
from dagster import op

"""
initial_clk is the given on a new connection
clk is updated on a market change
using the two together, an image of market odds at a certain time can be referenced
used as a cache, to only retrieve changes on the last image
"""

@op
def start_stream():
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
            logger.info("sending stream exchange authentication")
            ssock.sendall(auth_message.encode())

            #subscription message
            market_subscription_message = (
                '{"op":"marketSubscription",'
                '"marketFilter":{"eventTypeIds":["2", "4", "61420"],"marketTypes":["MATCH_ODDS"], "inPlay":true},'
                '"marketDataFilter":{"ladderLevels": 1, "fields":["EX_BEST_OFFERS"]}}\r\n'
            )
            print(market_subscription_message)
            ssock.sendall(market_subscription_message.encode())
            logger.info("sending market subscription message")

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
                    with open("newStream.json", "a") as outfile:
                        if len(complete_json) > 0:
                            logger.info("sending market snapshot to json")
                            outfile.write(complete_json)
                            complete_json = ""


    logger.info("connection closed")
