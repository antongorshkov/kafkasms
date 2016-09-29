from kafka import KafkaConsumer
from kafka import KafkaProducer
from datetime import datetime, timedelta
import thread
import time
import os
import json

broker = os.environ["KAFKA"]
api_key=os.environ["API_KEY"]
api_secret=os.environ["API_SECRET"]
from_num=os.environ["FROM_NUM"]

producer = KafkaProducer(bootstrap_servers=broker, batch_size=128000, acks=0, max_in_flight_requests_per_connection=10)

with open('fruits') as f:
  d = dict([x.rstrip(),1] for x in f)

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def send_json(msg,topic):
    r = producer.send(topic, json.dumps(msg).encode('ascii'))

def send_msgs(msg, size):
    for _ in range(size):
        producer.send('kafka1', msg.encode('utf-8'))

def process_tokens(tokens):
    size = int(tokens[1])
    size = min([size,1000])
    value = tokens[0].title()
    sender = tokens[2]
    if value in d:
        new_msg = '{"value":"'+value+'", "from_num":"'+sender+'", "debug":"'+msg.value+'"}'
        response = 'Congrats! You just sent ' + str(size) + ' ' + value + '!'
        send_json({'from': from_num, 'to': sender, 'text': response}, 'sms_response')
        thread.start_new_thread(send_msgs, (new_msg, size))
    else:
        response = 'Sorry, I dont recognize ' + value + ' as a valid fruit.'
        send_json({'from': from_num, 'to': sender, 'text': response}, 'sms_response')

consumer = KafkaConsumer(bootstrap_servers=broker)
consumer.subscribe(['sms'])

while True:
    for msg in consumer:
        try:
            tokens = msg.value.split()
            if is_number(tokens[0]):
                send_json({'sender': tokens[1], 'val': tokens[0]},'vote')
                #producer.send('vote',tokens[0].encode('utf-8'))
            else:
                process_tokens(tokens)
        except:
            print "Unexpected error"
