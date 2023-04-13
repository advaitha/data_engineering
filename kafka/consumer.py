from kafka import KafkaConsumer


# Consumer that waits for new messages
def kafka_python_consumer():
    # consume using the topic name and group id
    consumer = KafkaConsumer(
        "mytesttopic", group_id="mypythonconsumer", bootstrap_servers="localhost:9092"
    )
    for msg in consumer:
        print(msg)


print("start consuming")

# start the consumer
kafka_python_consumer()

print("done")
