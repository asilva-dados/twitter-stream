from kafka import KafkaConsumer
import json
import boto3
import os
import pandas as pd
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql import SparkSession

scala_version = '2.12'
spark_version = '3.1.2'
topic_name = 'twitter'
bucket_name = 'bucket-dados-deltalake' 
delta_location = bucket_name + "/raw"
checkpoint_location = bucket_name + "/checkpoints";
kafka_broker = 'localhost:9092'
update_kafka_schema = 'False'
aws_access_key = os.environ['AWS_ACCESS_KEY']
aws_access_secret_key = os.environ['AWS_SECRET_KEY']   
s3_resource = boto3.resource('s3',
                             aws_access_key_id=aws_access_key,
                            aws_secret_access_key=aws_access_secret_key,
                            )
s3_client = s3_resource.meta.client

packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.1'
]

def read_stream_kafka_topic():
    spark = SparkSession \
            .builder \
            .appName("twitter_raw") \
            .config("spark.sql.debug.maxToStringFields", "100") \
            .config("spark.jars.packages", ",".join(packages))\
            .getOrCreate()

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("kafka.security.protocol", "SSL") \
        .option("failOnDataLoss", "false") \
        .option("subscribe", topic_name) \
        .option("includeHeaders", "true") \
        .option("startingOffsets", "latest") \
        .option("spark.streaming.kafka.maxRatePerPartition", "50") \
        .load()
    return kafka_df

def func_call(df, batch_id):
    df.selectExpr("CAST(value AS STRING) as json")
    requests = df.rdd.map(lambda x: x.value).collect()
    logging.info(requests)

def write_stream_kafka_topic_s3(kafka_df):
    kafka_df.selectExpr("CAST(value AS STRING) as json") \
            .writeStream \
            .format("json") \
            .option("checkpointLocation",checkpoint_location) \
            .option("path", f's3a://{delta_location}/twitter') \
            .trigger(processingTime="5 minutes") \
            .start().awaitTermination()


if __name__ == '__main__':
    kafka_df = read_stream_kafka_topic()
    write_stream_kafka_topic_s3(kafka_df)
    

  