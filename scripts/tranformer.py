from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, avg , count
from pymongo import MongoClient
from elasticsearch import Elasticsearch

"""Create a SparkSession with elastic search"""
spark = SparkSession.builder \
    .appName("YourAppName") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-<your_version>") \
    .getOrCreate()

"""Read data from MongoDB into a DataFrame"""
mongo_uri = "mongodb://your_mongodb_uri"  # Replace with your MongoDB URI
db_name = "your_database_name"            # Replace with your MongoDB database name
collection_name = "your_collection_name"  # Replace with your MongoDB collection name

df = spark.read.format("mongo").option("uri", mongo_uri).option("database", db_name).option("collection", collection_name).load()
df.createOrReplaceTempView("temptable")

"""here we are using sql to do the aggregation"""
result = spark.sql("""
    SELECT url, country, COUNT(DISTINCT user_id) AS unique_users, COUNT(*) AS clicks, SUM(timestamp_diff) / COUNT(*) AS avg_time_spent
    FROM (
        SELECT url, country, user_id, timestamp,
               UNIX_TIMESTAMP(LEAD(timestamp) OVER (PARTITION BY url, country, user_id ORDER BY timestamp)) - UNIX_TIMESTAMP(timestamp) AS timestamp_diff
        FROM click_data
    ) t
    GROUP BY url, country
    ORDER BY url, country
""")

"""to store the data in elastic search we are creatign mapping"""
index_mapping = {
    "mappings": {
        "properties": {
            "url": {"type": "keyword"},
            "country": {"type": "keyword"},
            "click_count": {"type": "integer"},
            "unique_users": {"type": "integer"},
            "avg_time_spent": {"type": "float"}
        }
    }
}
#Create an Elasticsearch client
es = Elasticsearch()

# Define the index name
index_name = <"clickstream_index">

# Create the index with the defined mapping
es.indices.create(index=index_name, body=index_mapping)


# Convert RDD to a list of Elasticsearch documents
documents = result.map(lambda item: {
    "url": item["url"],
    "country": item["country"],
    "click_count": item["click_count"],
    "unique_users": item["unique_users"],
    "avg_time_spent": item["avg_time_spent"]
}).collect()

# Index the documents in Elasticsearch
for document in documents:
    es.index(index=index_name, body=document)