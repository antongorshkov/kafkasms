# kafkasms
Simple client to receive messages from SMS(HTTP GET really) and persist onto a Kafka topic

##Instructions:

```
sudo pip install Flask
sudo pip install kafka-python
```

Run main web-server:

```
python application.py &
```

and then run the re-direct:

```
python redirect.py
```
