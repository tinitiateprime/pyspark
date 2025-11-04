from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Products CSV Reader") \
    .getOrCreate()

# File path
file_path = "/home/jovyan/work/products.csv"

# Read CSV file into DataFrame
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Print header (schema)
print("Schema:")
df.printSchema()

# Print row count
row_count = df.count()
print(f"\nRow Count: {row_count}")

# List column names
print("\nColumns:")
print(df.columns)

# Stop the Spark session
spark.stop()