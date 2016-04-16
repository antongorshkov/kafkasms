from kafka import KafkaConsumer
from kafka import KafkaProducer
from datetime import datetime, timedelta
import thread
import time

producer = KafkaProducer(batch_size=128000)

def send_msgs(msg, size):
    for _ in range(size):
        producer.send('simple1', msg.encode('utf-8'))

while True:
    consumer = KafkaConsumer('sms')
    for msg in consumer:
        tokens = msg.value.split()
        try:
            size = int(tokens[1])
            size = min([size,10000])
            value = tokens[0]
            new_msg = '{"value":"'+value+'", "debug":"'+msg.value+'"}'
            thread.start_new_thread(send_msgs, (new_msg, size))
        except:
            print "Unexpected error"
            raise
