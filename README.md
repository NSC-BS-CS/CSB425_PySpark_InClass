# Week 4 In-Class Exercise — PySpark with Dockerized Spark

Complete `exercise.py` by filling in all six TODOs. The Spark cluster is already
running. You just need to write the code and submit the job.

---

## Dataset

`data/raw/coffee_orders.csv` — 100 rows of coffee shop orders.

Columns: Order_ID, Customer_Name, Item, Quantity, Price, Total, Order_Date, Channel

---

## TODOs

**TODO 1** — Create a SparkSession with appName "CoffeeExercise", master
`spark://spark-master:7077`, and log level WARN.

**TODO 2** — Read the CSV from `/opt/spark/data/raw/coffee_orders.csv` with
`header=True` and `inferSchema=True`. Print the schema and row count.

**TODO 3** — Add three columns using `withColumn()`:
- `order_size`: "small" if Total < 10, "medium" if Total < 30, "large" otherwise
- `revenue_per_item`: Total / Quantity rounded to 2 decimal places (use `spark_round`)
- `processed_at`: current timestamp

**TODO 4** — Write the transformed DataFrame to Parquet and ORC with
`mode="overwrite"`:
- Parquet → `/opt/spark/data/output/parquet`
- ORC → `/opt/spark/data/output/orc`

**TODO 5** — Register a temp view named "orders" and run a Spark SQL query
returning order count, total revenue, and average order total grouped by Channel.
Time the query using `time.time()` before and after `.collect()` and print the
elapsed time.

**TODO 6** — Call `spark.stop()`.

---

## Running Your Script

Update the last line of the `spark-submit` command in `docker-compose.yaml` to
point to `exercise.py`, then run:

```
docker compose run spark-submit
```

---

## Submission

Push the following to a **private** GitHub repo and submit the URL. Add the
professor and TAs as collaborators.

- `exercise.py` — completed with all six TODOs
- `screenshot.png` — terminal showing the SQL result table and query time
