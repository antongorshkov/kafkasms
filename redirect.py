from kafka import KafkaConsumer
from kafka import KafkaProducer
from datetime import datetime, timedelta
import thread
import time
import os
import nexmo

broker = os.environ["KAFKA"]
api_key=os.environ["API_KEY"]
api_secret=os.environ["API_SECRET"]
from_num=os.environ["FROM_NUM"]
client = nexmo.Client(key=api_key, secret=api_secret)

producer = KafkaProducer(bootstrap_servers=broker, batch_size=128000)
with open('fruits') as f:
  d = dict([x.rstrip(),1] for x in f)

def send_msgs(msg, size):
    for _ in range(size):
        producer.send('kafka1', msg.encode('utf-8'))

while True:
    consumer = KafkaConsumer(bootstrap_servers=broker)
    consumer.subscribe(['sms'])
    for msg in consumer:
        tokens = msg.value.split()
        try:
            size = int(tokens[1])
            size = min([size,10000])
            value = tokens[0].title()
            sender = tokens[2]
            if value in d:
                new_msg = '{"value":"'+value+'", "from_num":"'+sender+'", "debug":"'+msg.value+'"}'
                response = 'Congrats! You just sent ' + tokens[1] + ' ' + value + '!'
                r=client.send_message({'from': from_num, 'to': sender, 'text': response})
                thread.start_new_thread(send_msgs, (new_msg, size))
            else:
                response = 'Sorry, I dont recognize ' + value + ' as a valid fruit.'
                client.send_message({'from': from_num, 'to': sender, 'text': response})

        except:
            print "Unexpected error"
