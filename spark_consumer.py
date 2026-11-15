from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_json
from pyspark.sql.types import IntegerType, StringType, StructType

# Create Spark session
spark = SparkSession.builder.appName("StudentAverage").getOrCreate()

# Define schema
schema = StructType() \
    .add("name", StringType()) \
    .add("math", IntegerType()) \
    .add("physics", IntegerType())

# Read data from Kafka
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9094")
    .option("subscribe", "students")
    .load()
)

# Convert Kafka data into JSON
parsed_df = (
    df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

# Calculate average
avg_df = parsed_df.withColumn(
    "average",
    expr("(math + physics) / 2")
)

# Print stream output
query = (
    avg_df.writeStream
    .outputMode("append")
    .format("console")
    .start()
)

# Keep stream running
query.awaitTermination()
