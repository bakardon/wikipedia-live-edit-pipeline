"""Spark batch: load Wikipedia Clickstream TSV → Postgres `raw.stg_clickstream`.

Source: https://dumps.wikimedia.org/other/clickstream/
Format: tab-separated `referer<TAB>target<TAB>nav_type<TAB>nav_count`.

Prefers a decompressed `*.tsv` if present (splittable reads, lower driver RAM).
Otherwise reads `*.tsv.gz` (single-threaded gzip; may OOM on small Docker limits).
"""
from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

PG_HOST = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT = os.environ.get("POSTGRES_PORT", "5432")
PG_DB = os.environ.get("POSTGRES_DB", "wiki")
PG_USER = os.environ.get("POSTGRES_USER", "wiki")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "wiki")
PG_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

CLICKSTREAM_PATH = os.environ.get("CLICKSTREAM_PATH", "/data/clickstream")
CLICKSTREAM_MONTH = os.environ.get("CLICKSTREAM_MONTH", "2026-04")
CLICKSTREAM_LANG = os.environ.get("CLICKSTREAM_LANG", "en")

CS_SCHEMA = StructType([
    StructField("referer", StringType(), False),
    StructField("target", StringType(), False),
    StructField("nav_type", StringType(), False),
    StructField("nav_count", IntegerType(), False),
])


def main() -> None:
    base = f"clickstream-{CLICKSTREAM_LANG}wiki-{CLICKSTREAM_MONTH}"
    dir_path = Path(CLICKSTREAM_PATH)
    tsv_plain = dir_path / f"{base}.tsv"
    tsv_gz = dir_path / f"{base}.tsv.gz"
    if tsv_plain.exists():
        file_path, use_gzip = tsv_plain, False
    elif tsv_gz.exists():
        file_path, use_gzip = tsv_gz, True
    else:
        sys.exit(
            f"ERROR: neither {tsv_plain} nor {tsv_gz} found. "
            "Run download-clickstream, or decompress: gunzip -c …tsv.gz > …tsv (Spark OOMs less on plain TSV)."
        )

    dump_month_date = datetime.strptime(f"{CLICKSTREAM_MONTH}-01", "%Y-%m-%d").date()
    wiki_db_str = f"{CLICKSTREAM_LANG}wiki"

    spark = (SparkSession.builder
        .appName("clickstream-batch")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.files.maxPartitionBytes", "134217728")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    reader = spark.read.schema(CS_SCHEMA).option("sep", "\t")
    if use_gzip:
        reader = reader.option("compression", "gzip")
    print(f"reading {file_path} (gzip={use_gzip})")
    df = (reader
        .csv(str(file_path))
        .where(F.col("nav_count").isNotNull() & (F.col("nav_count") > 0))
        .where(F.col("target").isNotNull())
        .withColumn("wiki_db", F.lit(wiki_db_str))
        .withColumn("dump_month", F.lit(dump_month_date))
        .withColumn("ingested_at", F.current_timestamp())
        .select("referer", "target", "nav_type", "nav_count",
                "wiki_db", "dump_month", "ingested_at"))

    # Single pass to JDBC — avoid df.count() here (extra full scan + agg; OOM on tight Docker RAM).
    print(f"writing {file_path} to raw.stg_clickstream …")

    (df.write
        .format("jdbc")
        .mode("append")
        .option("url", PG_URL)
        .option("dbtable", "raw.stg_clickstream")
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", "10000")
        .save())

    print("done; verify row count in Postgres: select count(*) from raw.stg_clickstream;")


if __name__ == "__main__":
    main()
