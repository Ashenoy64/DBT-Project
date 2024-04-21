from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType
from pyspark.sql.functions import from_json
import os


KAFKA_BROKER = os.getenv("KAFKA_BROKER")
SPARK_BROKER = os.getenv("SPARK_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_PROCESSED_TOPIC = os.getenv("KAFKA_PROCESSED_TOPIC")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION")
KEYWORDS=os.getenv("KEYWORDS").split(",")

# Define the schema for comments
comment_schema = StructType([
    StructField("body", StringType(), nullable=True),
    StructField("score", IntegerType(), nullable=True),
    StructField("created_utc", TimestampType(), nullable=True),
    StructField("id", StringType(), nullable=True),
    StructField("permalink", StringType(), nullable=True),
    StructField("ups", IntegerType(), nullable=True),
    StructField("downs", IntegerType(), nullable=True),
    StructField("author", StringType(), nullable=True)
])

# Define the schema for posts
post_schema = StructType([
    StructField("title", StringType(), nullable=True),
    StructField("selftext", StringType(), nullable=True),
    StructField("url", StringType(), nullable=True),
    StructField("score", IntegerType(), nullable=True),
    StructField("authorName", StringType(), nullable=True),
    StructField("id", StringType(), nullable=True),
    StructField("created_utc", TimestampType(), nullable=True),
    StructField("permalink", StringType(), nullable=True),
    StructField("ups", IntegerType(), nullable=True),
    StructField("downs", IntegerType(), nullable=True),
    StructField("num_comments", IntegerType(), nullable=True),
    StructField("comments", ArrayType(comment_schema), nullable=True)
])

# Create a SparkSession
spark = SparkSession.builder.master(SPARK_BROKER).appName("RedditDataAnalysis").getOrCreate()

# Read streaming data from Kafka
streaming_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Convert value column to JSON and expand it using the defined schema
json_df = streaming_df.selectExpr("cast(value as string) as value")
json_expanded_df = json_df.withColumn("value", from_json(json_df["value"], post_schema)).select("value.*") 
df = json_expanded_df.select("title", "selftext", "url", "score", "authorName", "id", "created_utc", "permalink", "ups", "downs", "num_comments", "comments")

df2 = df.where(
    df['selftext'].rlike("|".join(["(" + pat + ")" for pat in KEYWORDS]))
)


df2.selectExpr("to_json(struct(*)) as value").writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", KAFKA_PROCESSED_TOPIC) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .start() \
        .awaitTermination()

