from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# SparkSession:
# - Entry point to programming with DataFrame and SQL API in Spark.
# - Used for working with structured data, reading files, creating DataFrames, etc.
# - Automatically provides access to SparkContext (for lower-level RDD APIs).
# -----------------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("SparkSessionAndContextDemo") \
    .master("local[*]") \
    .getOrCreate()  # Create or retrieve existing Spark session

# -----------------------------------------------------------------------------------
# SparkContext:
# - Low-level Spark API entry point, used primarily for working with RDDs.
# - Used for distributed data processing and parallel operations.
# - SparkSession wraps around SparkContext to provide a higher-level API.
# -----------------------------------------------------------------------------------
sc = spark.sparkContext

# Print SparkSession and SparkContext details
print("Spark Session:")
print(spark)  # Shows SparkSession details including app name and configuration

print("\nSpark Context:")
print(sc)  # Shows SparkContext details such as master node, application ID, etc.

# -----------------------------------------------------------------------------------
# Using SparkContext to create RDD:
# - RDD (Resilient Distributed Dataset) is the fundamental data structure in Spark.
# - Represents an immutable distributed collection of objects.
# - Useful for low-level transformations and actions.
# -----------------------------------------------------------------------------------
data = [1, 2, 3, 4, 5]
rdd = sc.parallelize(data)  # Convert local list into an RDD
print("\nRDD created using SparkContext:")
print(rdd.collect())  # Action to gather and print all RDD elements

# -----------------------------------------------------------------------------------
# Using SparkSession to create DataFrame:
# - DataFrame is a distributed collection of data organized into named columns.
# - Similar to a table in a relational database.
# - Useful for structured data processing, SQL operations, and schema enforcement.
# -----------------------------------------------------------------------------------
df = spark.createDataFrame([(1, "Alice"), (2, "Bob"), (3, "Charlie")], ["id", "name"])
print("\nDataFrame created using SparkSession:")
df.show()  # Displays DataFrame content in a tabular format

# Stop the SparkSession and release resources
spark.stop()
