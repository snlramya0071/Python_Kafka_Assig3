# from kafka import KafkaProducer
from confluent_kafka import Producer
from confluent_kafka import KafkaError
import random
import uuid
import datetime
import json
import time

#  kafka-topics  --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4 --topic orders

# for testing to check whether we have invoice generated or not.
# kafka-console-consumer --bootstrap-server localhost:9092 --topic orders


TOPIC = "orders"
SAMPLES = 1000
DELAY = 5 # seconds
states = ["AL", "CA", "AK", "AZ", "AR", "CO", "CT", "DE", "FL", "GA", "HI", "Id", "IL", "IN", "IA", "KS", "KY", "IT", "LA", "ME", "MD","TX"]
item_ids = ['85123A', '71053', '84406B', '84406G', '84406E']
customer_codes = [17850, 13047, 12583, 17850]

# broker runs in linux machine
# producer = Producer{'bootstrap_servers' ='localhost:9092'}
producer = Producer({'bootstrap.servers': 'localhost:9092'})


for i in range(SAMPLES):
    state =  random.choice(states)
    invoice_no = str(uuid.uuid4().fields[-1])[:6]
    invoice_no = int(invoice_no)
    
    customer_code =  random.choice(customer_codes)
   
    current_time = datetime.datetime.now()
    timestamp = int(current_time.timestamp()*1000)
    # invoice_date = current_time.strftime('%m/%d/%Y %H:%M') 
    
    number_of_items = random.randint(3, 10)
    
    for j in range(number_of_items):
        # MM/dd/yyyy hh:mm
        order_id = int(random.randint(1, 100000))
        quantity = random.randint(1, 10)
        # number = str(random.randint(1,100))
        prices = int(random.randint(1, 50))
        item_id =  str(random.randint(1,100))
        invoice = {  "Order_id":order_id,
                     "Item_id": item_id ,
                     "Quantity": quantity,
                     "Price": prices,
                     "timestamp": timestamp ,
                     "State"  : state  }
    
        invoice_str = json.dumps(invoice)
        print ("POS ", invoice_str)

        # key = invoice["Country"]
        # producer.send(TOPIC, key=bytes(key,'utf-8'), value=bytes(invoice_str, 'utf-8'))
        # key = cccccccccccccccccc
        # producer.flush(TOPIC, key=bytes(key,'utf-8'), value=bytes(invoice_str, 'utf-8'))
        key = invoice["State"].encode('utf-8')
        # producer.produce(TOPIC, key=bytes(key,'utf-8'), value=bytes(invoice_str, 'utf-8'))
        # value = payload.encode('utf-8')
        producer.produce(TOPIC, key=key, value = invoice_str)
        
    time.sleep(DELAY)