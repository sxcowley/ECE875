from pyspark.sql import SparkSession
import time

# Create a SparkSession
spark = SparkSession.builder \
    .appName('Scheduler Test') \
    .getOrCreate()

# Create a large DataFrame
df = spark.range(0, 10000000)

# Perform an operation to force Spark to compute the DataFrame
df = df.withColumn("new_column", df["id"] * 10)

# Define a function that performs a computationally intensive task
def compute(df):
    return df.groupby("new_column").count().collect()

# Run the compute function using the FIFO scheduler (default)
start_time = time.time()
result = compute(df)
print("Time taken with FIFO scheduler: %s seconds" % (time.time() - start_time))

# Switch to the Fair scheduler
spark.conf.set("spark.scheduler.mode", "FAIR")

# Run the compute function using the Fair scheduler
start_time = time.time()
result = compute(df)
print("Time taken with Fair scheduler: %s seconds" % (time.time() - start_time))

# Stop the SparkSession
spark.stop()