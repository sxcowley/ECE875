from pyspark.sql import SparkSession
import time
import csv
import resource
import gc
import psutil

# Define a function that performs a computationally intensive task
def compute(df):
    return df.groupby("new_column").count().collect()

# Define a list of values to test for the number of partitions and the configuration properties
values_to_test = list(range(1, 151, 25))

# Try to open a CSV file to write the results
try:
    with open('results.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Scheduler", "Partitions", "Parallelism", "Shuffle Partitions", "Time", "Memory", "CPU"])

        # Iterate over the values to test
        for partitions in values_to_test:
            for parallelism in values_to_test:
                for shuffle_partitions in values_to_test:
                    for scheduler in ['FIFO', 'FAIR']:
                        # Trigger garbage collection
                        gc.collect()

                        # Create a SparkSession with the specified scheduler
                        spark = SparkSession.builder \
                            .appName('Scheduler Test') \
                            .config('spark.scheduler.mode', scheduler) \
                            .config('spark.default.parallelism', parallelism) \
                            .config('spark.sql.shuffle.partitions', shuffle_partitions) \
                            .getOrCreate()

                        # Create a large DataFrame
                        df = spark.range(0, 10000000)

                        # Repartition the DataFrame with the current value
                        df = df.repartition(partitions)

                        # Perform an operation to force Spark to compute the DataFrame
                        df = df.withColumn("new_column", df["id"] * 10)

                        # Run the compute function and time how long it takes
                        start_time = time.time()
                        start_memory = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                        start_cpu = psutil.cpu_percent(interval=None)
                        result = compute(df)
                        time_taken = time.time() - start_time
                        end_memory = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                        end_cpu = psutil.cpu_percent(interval=None)
                        memory_used = end_memory - start_memory
                        cpu_used = end_cpu - start_cpu
                        print(f"Time taken with {partitions} partitions, {parallelism} parallelism, {shuffle_partitions} shuffle partitions: {time_taken} seconds, Memory used: {memory_used} kilobytes, CPU used: {cpu_used} percent")

                        # Write the results to the CSV file
                        writer.writerow([scheduler, partitions, parallelism, shuffle_partitions, time_taken, memory_used, cpu_used])

                        # Stop the SparkSession
                        spark.stop()
except Exception as e:
    print(f"Failed to open file: {e}")