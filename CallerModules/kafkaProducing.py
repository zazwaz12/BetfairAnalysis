import json
import json_lines
import sys
import os
from CallerModules.logSetup import logger

from confluent_kafka import Producer

#Function that reads in the client properties accounting for comments and extra spaces
def read_ccloud_config(config_file):
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct the absolute path to the config file
    config_path = os.path.join(script_dir, config_file)
    conf = {}
    with open(config_path) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    return conf

#Function that returns the response from the server
def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                            (msg.topic(), msg.partition(), msg.offset()))




def read_the_json ():
    producer = Producer(read_ccloud_config(config_file="client.properties"))
    # Iterating through the json lines file
    with open('unprocessedmarkets.json', 'r') as f:
        for line in f:
            try:
                # Send the raw line as the value without decoding it as JSON
                producer.produce(
                    topic="BettingMarketOdds", 
                    value=line.encode('utf-8'),
                    callback=delivery_callback
                )
                producer.poll(0)
            except Exception as e:
                print("Error:", e)
                continue

    # Closing the file handle
    open('unprocessedmarkets.json', 'w').close()
    producer.flush()
    f.close()