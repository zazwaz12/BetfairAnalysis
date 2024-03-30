from confluent_kafka import Consumer, KafkaException, KafkaError
from CallerModules.kafkaProducing import read_ccloud_config
import sys
import json

def consume_from_topic(topic):
    consumer = Consumer(read_ccloud_config(config_file="client.properties"))
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(1.0)  # Adjust timeout as needed
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition
                    continue
                else:
                    raise KafkaException(msg.error())
            else:
                # Message is consumed successfully
                msg_value = msg.value().decode('utf-8')
                # Assuming the message value is JSON
                message_json = json.loads(msg_value)
                # Process the JSON message here
                print("Received message:", message_json)
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')
    finally:
        # Close consumer on exit
        consumer.close()

if __name__ == '__main__':
    consume_from_topic("BettingMarketOdds")

