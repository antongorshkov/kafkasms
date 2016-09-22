from flask import Flask
from flask import request
from kafka import KafkaProducer
import os

producer = KafkaProducer(bootstrap_servers=os.environ["KAFKA"])

def send(msg,sender):
    msg=msg+' '+sender
    producer.send('sms', msg.encode('utf-8'))
    return '<p>Sent %s to sms topic</p>\n' % msg

application = Flask(__name__)
application.add_url_rule('/<username>', 'send', (lambda username: send(request.args.get('text'), request.args.get('msisdn'))))

if __name__ == "__main__":
    application.run(host= '0.0.0.0')
