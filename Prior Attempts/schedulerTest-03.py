from pyspark.sql import SparkSession
import time

# Define a function that performs a computationally intensive task
def compute(df):
    return df.groupby("new_column").count().collect()

# Create a SparkSession with the FIFO scheduler (default)
spark = SparkSession.builder \
    .appName('Scheduler Test') \
    .getOrCreate()

# Iterate over the range of partition numbers
for num_partitions in range(100, 1001, 100):
    # Create a large DataFrame
    df = spark.range(0, 10000000)

    # Repartition the DataFrame with the current number of partitions
    df = df.repartition(num_partitions)

    # Perform an operation to force Spark to compute the DataFrame
    df = df.withColumn("new_column", df["id"] * 10)

    # Run the compute function and time how long it takes
    start_time = time.time()
    result = compute(df)
    print(f"Time taken with {num_partitions} partitions: {time.time() - start_time} seconds")

# Stop the current SparkSession
spark.stop()

# Create a new SparkSession with the Fair scheduler
spark = SparkSession.builder \
    .appName('Scheduler Test') \
    .config('spark.scheduler.mode', 'FAIR') \
    .getOrCreate()

# Iterate over the range of partition numbers
for num_partitions in range(10, 1001, 100):
    # Recreate the DataFrame
    df = spark.range(0, 10000000)

    # Repartition the DataFrame with the current number of partitions
    df = df.repartition(num_partitions)

    df = df.withColumn("new_column", df["id"] * 10)

    # Run the compute function and time how long it takes
    start_time = time.time()
    result = compute(df)
    print(f"Time taken with {num_partitions} partitions: {time.time() - start_time} seconds")

# Stop the SparkSession
spark.stop()