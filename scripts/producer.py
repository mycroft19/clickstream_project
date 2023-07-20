from kafka import KafkaProducer , KafkaConsumer
import json

"""creating a producer and topic from the loacalhost assuming we have the variable data"""
data = []
producer = KafkaProducer(bootstrap_servers=<bootstrap_server>)

for item in data:
    # Remove the unique row identifier from the item and use the remaining data as values
    data_values = {k: v for k, v in item.items() if k != 'unique_row_id'}

    # Convert the values to a string representation (e.g., JSON)
    value = json.dumps(data_values).encode('utf-8')
    """Send the message with the key-value pair to the 'your_topic' topic"""
    producer.send(<topic>, value=value)
producer.flush()
producer.close()