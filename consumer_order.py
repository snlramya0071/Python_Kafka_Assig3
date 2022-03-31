from confluent_kafka import Consumer

# bootstrap server here is the broker 
c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'orders-group-rm',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['orders-5min'])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value().decode('utf-8')))

c.close()


