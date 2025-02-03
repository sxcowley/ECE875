#While in SANDBOX; issue "source myenv/bin/activate"
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName('LocalModeTest') \
    .master('local') \
    .getOrCreate()

# Define a function that performs a computationally intensive task
def compute(df):
    return df.groupby("new_column").count().collect()

# Create a SparkSession with the FIFO scheduler (default)
spark = SparkSession.builder \
    .appName('Scheduler Test') \
    .getOrCreate()

# Create a large DataFrame
df = spark.range(0, 10000000)

# Perform an operation to force Spark to compute the DataFrame
df = df.withColumn("new_column", df["id"] * 10)

# Run the compute function using the FIFO scheduler
start_time = time.time()
result = compute(df)
print("Time taken with FIFO scheduler: %s seconds" % (time.time() - start_time))

# Stop the current SparkSession
spark.stop()

# Create a new SparkSession with the Fair scheduler
spark = SparkSession.builder \
    .appName('Scheduler Test') \
    .config('spark.scheduler.mode', 'FAIR') \
    .getOrCreate()

# Recreate the DataFrame
df = spark.range(0, 10000000)
df = df.withColumn("new_column", df["id"] * 10)

# Run the compute function using the Fair scheduler
start_time = time.time()
result = compute(df)
print("Time taken with Fair scheduler: %s seconds" % (time.time() - start_time))

# Stop the SparkSession
spark.stop()