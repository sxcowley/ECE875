from pyspark.sql import SparkSession
import time
import csv

# Define a function that performs a computationally intensive task
def compute(df):
    return df.groupby("new_column").count().collect()

# Define a list of values to test for the number of partitions and the configuration properties
values_to_test = list(range(100, 501, 100))

# Open a CSV file to write the results
with open('results.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["Scheduler", "Partitions", "Parallelism", "Shuffle Partitions", "Time"])

    # Iterate over the values to test
    for value in values_to_test:
        # Create a SparkSession with the FIFO scheduler (default)
        spark = SparkSession.builder \
            .appName('Scheduler Test') \
            .config('spark.default.parallelism', value) \
            .config('spark.sql.shuffle.partitions', value) \
            .getOrCreate()

        # Create a large DataFrame
        df = spark.range(0, 10000000)

        # Repartition the DataFrame with the current value
        df = df.repartition(value)

        # Perform an operation to force Spark to compute the DataFrame
        df = df.withColumn("new_column", df["id"] * 10)

        # Run the compute function and time how long it takes
        start_time = time.time()
        result = compute(df)
        time_taken = time.time() - start_time
        print(f"Time taken with {value} partitions and parallelism: {time_taken} seconds")

        # Write the results to the CSV file
        writer.writerow(["FIFO", value, value, value, time_taken])

        # Stop the current SparkSession
        spark.stop()

    # Iterate over the values to test
    for value in values_to_test:
        # Create a new SparkSession with the Fair scheduler
        spark = SparkSession.builder \
            .appName('Scheduler Test') \
            .config('spark.scheduler.mode', 'FAIR') \
            .config('spark.default.parallelism', value) \
            .config('spark.sql.shuffle.partitions', value) \
            .getOrCreate()

        # Recreate the DataFrame
        df = spark.range(0, 10000000)

        # Repartition the DataFrame with the current value
        df = df.repartition(value)

        df = df.withColumn("new_column", df["id"] * 10)

        # Run the compute function and time how long it takes
        start_time = time.time()
        result = compute(df)
        time_taken = time.time() - start_time
        print(f"Time taken with {value} partitions and parallelism: {time_taken} seconds")

        # Write the results to the CSV file
        writer.writerow(["Fair", value, value, value, time_taken])

        # Stop the SparkSession
        spark.stop()