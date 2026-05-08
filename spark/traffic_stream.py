from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("TrafficMonitoring") \
    .getOrCreate()

schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("vehicle_count", IntegerType()),
    StructField("avg_speed", DoubleType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic-data") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)")

traffic_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

processed_df = traffic_df.withColumn(
    "congestion_index",
    col("vehicle_count") / col("avg_speed")
)

critical_df = processed_df.filter(col("avg_speed") < 10)

query1 = processed_df.writeStream \
    .format("csv") \
    .option("path", "../data/processed") \
    .option("checkpointLocation", "../data/checkpoint1") \
    .outputMode("append") \
    .start()

query2 = critical_df.writeStream \
    .format("csv") \
    .option("path", "../data/alerts") \
    .option("checkpointLocation", "../data/checkpoint2") \
    .outputMode("append") \
    .start()

query1.awaitTermination()
query2.awaitTermination()