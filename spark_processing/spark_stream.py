"""
Spark Structured Streaming — Traffic Congestion Processor
===========================================================
Consumes raw traffic events from Kafka, computes:
  1. Congestion Index using 5-minute tumbling windows (Stream Layer).
  2. Immediate CRITICAL alert if avg_speed < 10 km/h — written to Postgres + critical-traffic topic.

Event Time Handling:
  - Uses the `timestamp` field embedded in the JSON payload as event time.
  - Watermark of 2 minutes tolerates late/out-of-order sensor data.
  - Processing time is used only for checkpoint bookkeeping.

Run:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,\
    org.postgresql:postgresql:42.6.0 spark_stream.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, avg, sum as _sum,
    when, lit, to_timestamp, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, BooleanType
)

# ── Configuration ──────────────────────────────────────────────────────────────
KAFKA_BROKER          = os.getenv("KAFKA_BROKER",   "kafka:9092")
KAFKA_TOPIC_IN        = os.getenv("TOPIC_IN",        "traffic-raw")
KAFKA_TOPIC_CRITICAL  = os.getenv("TOPIC_CRITICAL",  "critical-traffic")
CHECKPOINT_BASE       = os.getenv("CHECKPOINT_DIR",  "/tmp/spark_checkpoints")

PG_URL  = os.getenv("PG_URL",  "jdbc:postgresql://postgres:5432/smartcity")
PG_USER = os.getenv("PG_USER", "admin")
PG_PASS = os.getenv("PG_PASS", "admin123")

WINDOW_DURATION    = "5 minutes"
WATERMARK_DURATION = "2 minutes"
CRITICAL_SPEED_KMPH = 10.0

# ── Schema ─────────────────────────────────────────────────────────────────────
TRAFFIC_SCHEMA = StructType([
    StructField("sensor_id",     StringType(),  True),
    StructField("junction_name", StringType(),  True),
    StructField("timestamp",     StringType(),  True),  # ISO-8601
    StructField("vehicle_count", IntegerType(), True),
    StructField("avg_speed",     DoubleType(),  True),
    StructField("is_critical",   BooleanType(), True),
])


# ── Congestion Index Formula ───────────────────────────────────────────────────
# CI = (avg_vehicle_count / max_capacity) * (1 - avg_speed / free_flow_speed)
# Simplified: CI = total_vehicles / (avg_speed + 1) — scaled to 0-100
MAX_CAPACITY   = 100
FREE_FLOW_SPEED = 60.0


def compute_congestion_index(total_vehicles, mean_speed):
    """UDF-free expression: higher CI → worse congestion (0-100 scale)."""
    # Handled inline via withColumn expressions in the streaming query


# ── Postgres writer helper ─────────────────────────────────────────────────────
def write_to_postgres(df, table: str):
    df.write \
      .format("jdbc") \
      .option("url", PG_URL) \
      .option("dbtable", table) \
      .option("user", PG_USER) \
      .option("password", PG_PASS) \
      .option("driver", "org.postgresql.Driver") \
      .mode("append") \
      .save()


def foreach_batch_congestion(batch_df, batch_id):
    """Write windowed congestion results to Postgres."""
    if batch_df.isEmpty():
        return
    print(f"[Batch {batch_id}] Writing {batch_df.count()} congestion records to Postgres …")
    write_to_postgres(batch_df, "congestion_index")


def foreach_batch_critical(batch_df, batch_id):
    """Write critical alerts to Postgres AND forward to critical-traffic Kafka topic."""
    if batch_df.isEmpty():
        return

    count = batch_df.count()
    print(f"[Batch {batch_id}] ⚠  {count} CRITICAL alert(s) detected!")

    # Write to Postgres alerts table
    write_to_postgres(batch_df, "critical_alerts")

    # Forward to Kafka critical-traffic topic
    import json
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    for row in batch_df.collect():
        payload = {
            "sensor_id":        row["sensor_id"],
            "junction_name":    row["junction_name"],
            "alert_time":       str(row["alert_time"]),
            "avg_speed":        row["avg_speed"],
            "vehicle_count":    row["vehicle_count"],
            "severity":         "CRITICAL",
        }
        producer.send(KAFKA_TOPIC_CRITICAL, value=payload)
        print(f"   → Alert forwarded: {payload['sensor_id']} | speed={payload['avg_speed']} km/h")
    producer.flush()
    producer.close()


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    spark = (
        SparkSession.builder
        .appName("SmartCity_TrafficStream")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_BASE)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("  Spark Streaming — Colombo Traffic Congestion Processor")
    print("=" * 60)

    # ── 1. Read from Kafka ─────────────────────────────────────────────────────
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC_IN)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── 2. Parse JSON payload ──────────────────────────────────────────────────
    parsed_df = (
        raw_df
        .selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_ts")
        .select(from_json(col("json_str"), TRAFFIC_SCHEMA).alias("data"), "kafka_ts")
        .select("data.*", "kafka_ts")
        # Event Time: use producer-embedded timestamp (not Kafka ingestion time)
        .withColumn("event_time", to_timestamp(col("timestamp")))
        .withWatermark("event_time", WATERMARK_DURATION)
    )

    # ── 3. Stream A: 5-minute Tumbling Window → Congestion Index ──────────────
    congestion_df = (
        parsed_df
        .groupBy(
            col("sensor_id"),
            col("junction_name"),
            window(col("event_time"), WINDOW_DURATION)
        )
        .agg(
            avg("avg_speed").alias("mean_speed_kmph"),
            _sum("vehicle_count").alias("total_vehicles"),
        )
        .withColumn(
            "congestion_index",
            # CI: 0 (free flow) → 100 (gridlock)
            (
                (col("total_vehicles") / lit(MAX_CAPACITY)) *
                (lit(1.0) - col("mean_speed_kmph") / lit(FREE_FLOW_SPEED))
            ).cast("double") * 100
        )
        .withColumn("congestion_level",
            when(col("congestion_index") >= 70, "SEVERE")
            .when(col("congestion_index") >= 40, "MODERATE")
            .otherwise("LOW")
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end",   col("window.end"))
        .drop("window")
    )

    query_congestion = (
        congestion_df.writeStream
        .outputMode("update")
        .foreachBatch(foreach_batch_congestion)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/congestion")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # ── 4. Stream B: Immediate Critical Alert ─────────────────────────────────
    critical_df = (
        parsed_df
        .filter(col("avg_speed") < CRITICAL_SPEED_KMPH)
        .select(
            col("sensor_id"),
            col("junction_name"),
            col("avg_speed"),
            col("vehicle_count"),
            col("event_time").alias("alert_time"),
        )
        .withColumn("processing_time", current_timestamp())
    )

    query_critical = (
        critical_df.writeStream
        .outputMode("append")
        .foreachBatch(foreach_batch_critical)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/critical")
        .trigger(processingTime="5 seconds")   # fast poll for critical alerts
        .start()
    )

    print("Streaming queries started. Waiting for data …")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()