from pyspark.sql import SparkSession
from pyspark.sql.functions import when, round, current_timestamp, col
import time

# ─────────────────────────────────────────────────────────────
# PART 1: SparkSession
# Talk through: what is a SparkSession, what is the master URL,
# why appName matters, what setLogLevel does
# ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("CoffeeDemo") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n" + "="*60)
print("DEMO — coffee_orders.csv")
print("="*60 + "\n")

# ─────────────────────────────────────────────────────────────
# PART 2: Reading CSV
# Talk through: header, inferSchema, why inferSchema is
# convenient but risky in production, printSchema, count
# ─────────────────────────────────────────────────────────────
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/opt/spark/data/raw/coffee_orders.csv")

print("Schema (inferred):")
df.printSchema()
print(f"Row count: {df.count()}\n")
df.show(5)

# ─────────────────────────────────────────────────────────────
# PART 3: Transformations
# Talk through: when().otherwise() vs pandas np.where,
# col() references, round(), current_timestamp(),
# withColumn() chaining, lazy evaluation
# ─────────────────────────────────────────────────────────────
df_transformed = df \
    .withColumn(
        "order_size",
        when(col("Total") < 10, "small")
        .when(col("Total") < 30, "medium")
        .otherwise("large")
    ) \
    .withColumn(
        "revenue_per_item",
        round(col("Total") / col("Quantity"), 2)
    ) \
    .withColumn("processed_at", current_timestamp())

print("After transformations (sample):")
df_transformed.select(
    "Order_ID", "Item", "Quantity", "Total",
    "order_size", "revenue_per_item", "Channel"
).show(10)

# ─────────────────────────────────────────────────────────────
# PART 4: Writing formats
# Talk through: why three formats, what overwrite means,
# the Avro package requirement, what part files are
# ─────────────────────────────────────────────────────────────
print("Writing Parquet...")
df_transformed.write.mode("overwrite").parquet("/opt/spark/data/output/parquet")
print("  -> done\n")

print("Writing Avro...")
df_transformed.write.mode("overwrite").format("avro").save("/opt/spark/data/output/avro")
print("  -> done\n")

print("Writing ORC...")
df_transformed.write.mode("overwrite").orc("/opt/spark/data/output/orc")
print("  -> done\n")

# ─────────────────────────────────────────────────────────────
# PART 5: Spark SQL
# Talk through: createOrReplaceTempView registers a virtual table,
# spark.sql() runs standard SQL against it,
# .collect() forces execution (lazy evaluation),
# time.time() for benchmarking
# ─────────────────────────────────────────────────────────────
df_transformed.createOrReplaceTempView("orders")

print("Running Spark SQL query on in-memory DataFrame...")
start = time.time()
result = spark.sql("""
    SELECT
        Channel,
        order_size,
        COUNT(*) AS order_count,
        ROUND(SUM(Total), 2) AS total_revenue,
        ROUND(AVG(Total), 2) AS avg_order_total
    FROM orders
    GROUP BY Channel, order_size
    ORDER BY Channel, order_size
""")
result.collect()
elapsed = round(time.time() - start, 4)
result.show(truncate=False)
print(f"Query time (in-memory): {elapsed}s\n")

# ─────────────────────────────────────────────────────────────
# PART 6: Read back from Parquet and query
# Talk through: reading binary format back, same SQL works,
# columnar advantage on aggregations
# ─────────────────────────────────────────────────────────────
print("Reading back from Parquet and running same query...")
df_parquet = spark.read.parquet("/opt/spark/data/output/parquet")
df_parquet.createOrReplaceTempView("orders")

start = time.time()
result2 = spark.sql("""
    SELECT
        Channel,
        order_size,
        COUNT(*) AS order_count,
        ROUND(SUM(Total), 2) AS total_revenue,
        ROUND(AVG(Total), 2) AS avg_order_total
    FROM orders
    GROUP BY Channel, order_size
    ORDER BY Channel, order_size
""")
result2.collect()
elapsed2 = round(time.time() - start, 4)
result2.show(truncate=False)
print(f"Query time (Parquet): {elapsed2}s\n")

print("="*60)
print("Demo complete.")
print("="*60)

spark.stop()
