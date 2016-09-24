from kafka import KafkaConsumer
import os
import nexmo
import json

client      = nexmo.Client( key=os.environ["API_KEY"],
                            secret=os.environ["API_SECRET"])
consumer    = KafkaConsumer(bootstrap_servers=os.environ["KAFKA"],
                            value_deserializer=lambda m: json.loads(m.decode('ascii')))
consumer.subscribe(['kafka_test1'])

while True:
    for message in consumer:
        client.send_message({   'from'  : message.value['from'],
                                'to'    : message.value['to'],
                                'text'  : message.value['text']})
