from kafka import KafkaConsumer
from pymongo import MongoClient

"""Connect to MongoDB"""
client = MongoClient('mongodb://localhost:27017')
db = client['clickstream_data']
collection1 = db['Click_data']
collection2 = db['Geo_data']
collection3 = db['User_agent_data']

"""define kafka consumer configuration and create a instance"""
bootstrap_servers = 'localhost:9092'
topic = 'clickstream2'
consumer_group_id = 'your_consumer_group'


consumer = KafkaConsumer(
    topic,
    group_id=consumer_group_id,
    bootstrap_servers=bootstrap_servers  # Assuming the data is encoded as UTF-8 strings
)

"""Consume messages from Kafka and store in MongoDB"""

for message in consumer:
    data = message  # Assuming the message value contains the data

"""Extract relevant fields from the data dictionary"""

    unique_id = f"id{message.offset}"
    user_id = data[1]
    time_stamp = data[2]
    url = data[3]
    country = data[7]
    city = data[8]
    user_browser = data[4]
    user_os = data[5]
    device = data[6]
    

"""Storing the data in respective mongodb collections"""
    collection1.update_one({'_id': unique_id}, {'$set': {'user_id': user_id ,'time_stamp' : time_stamp , 'url': url }}, upsert=True)
    collection2.update_one({'_id': unique_id}, {'$set': {'country': country, 'city': city}}, upsert=True)
    collection3.update_one({'_id': unique_id}, {'$set': {'user_browser' : user_browser ,'user_os' :user_os,'device' : device}}, upsert=True)

"""Close the Kafka consumer and MongoDB connection"""
consumer.close()
client.close()
