"""Spark Structured Streaming: Kafka topic `wiki.edits` → Postgres `raw.stg_edit`.

Why land into raw and let dbt promote to core:
- Streaming stays a single fast INSERT per event with no inline dim upserts.
- Replay is cheap: truncate raw, rerun dbt.
- The same Spark cluster handles batch (Clickstream) so the engine count stays at one.

Window-function logic (RANK over minute, LAG for velocity, sliding 10-min for
vandalism) lives in dbt marts on top of `core.fact_edit`. See dbt/models/marts/.
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

KAFKA_BROKERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "wiki.edits")

PG_HOST = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT = os.environ.get("POSTGRES_PORT", "5432")
PG_DB = os.environ.get("POSTGRES_DB", "wiki")
PG_USER = os.environ.get("POSTGRES_USER", "wiki")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "wiki")
# stringtype=unspecified lets Postgres infer JSONB from the column definition
# rather than rejecting strings sent for JSONB columns.
PG_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}?stringtype=unspecified"

CHECKPOINT_DIR = "/tmp/checkpoints/stg_edit"

# Schema of the *original* Wikimedia recentchange envelope (subset of fields we use).
WIKI_SCHEMA = StructType([
    StructField("id", LongType(), True),
    StructField("type", StringType(), True),
    StructField("namespace", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("user", StringType(), True),
    StructField("bot", BooleanType(), True),
    StructField("minor", BooleanType(), True),
    StructField("wiki", StringType(), True),
    StructField("revision", StructType([
        StructField("old", LongType(), True),
        StructField("new", LongType(), True),
    ]), True),
    StructField("length", StructType([
        StructField("old", IntegerType(), True),
        StructField("new", IntegerType(), True),
    ]), True),
])

IPV4_RE = r"^(\d{1,3}\.){3}\d{1,3}$"
IPV6_RE = r"^[0-9a-fA-F:]+$"


def _write_to_postgres(batch_df, batch_id: int) -> None:
    """foreachBatch sink: append one micro-batch to raw.stg_edit via JDBC."""
    if batch_df.rdd.isEmpty():
        return
    (batch_df.write
        .format("jdbc")
        .mode("append")
        .option("url", PG_URL)
        .option("dbtable", "raw.stg_edit")
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", "1000")
        .save())


def main() -> None:
    spark = (SparkSession.builder
        .appName("wiki-edits-stream")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    raw_kafka = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "5000")
        .load())

    parsed = (raw_kafka
        .select(
            F.col("value").cast("string").alias("raw_payload"),
            F.from_json(F.col("value").cast("string"), WIKI_SCHEMA).alias("e"),
        )
        .where(F.col("e.type").isin("edit", "new"))
        .where(F.col("e.title").isNotNull())
        .where(F.col("e.namespace").isNotNull())
        .select(
            F.coalesce(F.col("e.revision.new"), F.col("e.id")).cast("long").alias("edit_id"),
            F.col("e.revision.new").alias("rev_id"),
            F.col("e.revision.old").alias("parent_id"),
            F.from_unixtime(F.col("e.timestamp")).cast("timestamp").alias("ts"),
            F.col("e.title").alias("page_title"),
            F.col("e.namespace"),
            F.col("e.wiki").alias("wiki_db"),
            F.col("e.user").alias("editor"),
            (F.col("e.user").rlike(IPV4_RE) | F.col("e.user").rlike(IPV6_RE)).alias("is_anon"),
            F.coalesce(F.col("e.bot"), F.lit(False)).alias("is_bot"),
            F.coalesce(F.col("e.minor"), F.lit(False)).alias("is_minor"),
            (F.coalesce(F.col("e.length.new"), F.lit(0)) - F.coalesce(F.col("e.length.old"), F.lit(0))).alias("bytes_changed"),
            F.col("e.comment"),
            F.col("raw_payload"),
            F.current_timestamp().alias("ingested_at"),
        )
        .filter(F.col("edit_id").isNotNull() & (F.col("edit_id") > 0))
        # Reorder columns to match raw.stg_edit DDL exactly.
        .select(
            "edit_id", "rev_id", "parent_id", "ts",
            "page_title", "namespace", "wiki_db",
            "editor", "is_anon", "is_bot", "is_minor",
            "bytes_changed", "comment", "raw_payload", "ingested_at",
        )
    )

    query = (parsed.writeStream
        .foreachBatch(_write_to_postgres)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .start())

    query.awaitTermination()


if __name__ == "__main__":
    main()
