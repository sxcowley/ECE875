from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName('Hello World') \
    .getOrCreate()

# Create a simple DataFrame
data = [("Hello", "World")]
df = spark.createDataFrame(data, ["Word1", "Word2"])

# Show the DataFrame
df.show()

# Stop the SparkSession
spark.stop()