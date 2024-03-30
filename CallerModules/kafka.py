import json
import json_lines
import sys

from confluent_kafka import Producer

#Function that reads in the client properties accounting for comments and extra spaces
def read_ccloud_config(config_file):
    conf = {}
    with open(config_file) as fh:
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

producer = Producer(read_ccloud_config("client.properties"))


def read_the_json ():
    # Iterating through the json lines file
    with open('unprocessedmarkets.json', 'rb') as f:
        for line in f:
            try:
                item = next(json_lines.reader([line.decode()]))
                #print(item, item["op"])
                producer.produce(
                    topic="BettingMarketOdds", 
                    key=json.dumps(item['op']).encode('utf-8'), 
                    value=json.dumps(item).encode('utf-8'),
                    callback=delivery_callback
                )
                producer.poll(0)
            except (json.decoder.JSONDecodeError, IndexError):
                print("Error decoding or indexing JSON line:", line.decode())
                continue

    # Closing the file handle
    open('unprocessedmarkets.json', 'w').close()
    producer.flush()
    f.close()