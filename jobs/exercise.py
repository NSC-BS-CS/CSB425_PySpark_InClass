from pyspark.sql import SparkSession
from pyspark.sql.functions import when, round, current_timestamp, col
import time

# ─────────────────────────────────────────────────────────────
# EXERCISE — coffee_orders.csv
# Complete the sections marked TODO
# ─────────────────────────────────────────────────────────────

# TODO 1: Create a SparkSession
# - appName should be "CoffeeExercise"
# - Connect to the master at spark://spark-master:7077
# - Set log level to WARN
spark = ???


# TODO 2: Read the CSV
# Path: /opt/spark/data/raw/coffee_orders.csv
# Use header=True and inferSchema=True
# Print the schema and row count
df = ???


# TODO 3: Add three columns using withColumn()
# a) "order_size": "small" if Total < 10, "medium" if Total < 30, "large" otherwise
# b) "revenue_per_item": Total / Quantity, rounded to 2 decimal places
# c) "processed_at": current timestamp
df_transformed = ???


# TODO 4: Write the transformed DataFrame to Parquet and ORC
# Paths:
#   Parquet -> /opt/spark/data/output/parquet
#   ORC     -> /opt/spark/data/output/orc
# Use mode="overwrite" for both


# TODO 5: Register a temp view and run a Spark SQL query
# View name: "orders"
# Query: count of orders, total revenue, and average order total
# grouped by Channel and order_size, ordered by Channel
# Time the query using time.time() and print the elapsed time


# TODO 6: Stop the SparkSession
spark.stop()
