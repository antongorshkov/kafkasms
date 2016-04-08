from kafka import KafkaConsumer
from kafka import KafkaProducer
from datetime import datetime, timedelta

producer = KafkaProducer()

while True:
    consumer = KafkaConsumer('sms')
    for msg in consumer:
        tokens = msg.value.split()
        try:
            size = int(tokens[1])
            value = tokens[0]
            new_msg = '{"value":"'+value+'", "debug":"'+msg.value+'"}'
            for _ in range(size):
                producer.send('simple1', new_msg.encode('utf-8'))
        except:
            print "Unexpected error"
            raise
