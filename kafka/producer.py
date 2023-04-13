from kafka import kafkaProducer

# Create Message
msg = "testing kafka"

# Create a producer
producer = kafkaProducer(bootstrap_servers='localhost:9092')

## Functions to send messages to Kafka
def kafka_python_producer_async(producer,msg):
    producer.send('mytesttopic',msg).add_callback(success).add_errback(error)
    producer.flush()

def success(metadata):
    print(metadata.topic)

def error(exception):
    print(exception)

print("start producing")

kafka_python_producer_async(producer,bytes(msg,'utf-8'))

print("done")