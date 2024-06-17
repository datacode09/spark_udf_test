from pyspark.sql import SparkSession
from enhancement_workflow import hsbc_indicator_enrichment

# Initialize Spark session
spark = SparkSession.builder.appName("HSBCIndicator").getOrCreate()

# Sample usage
config_path = "hsbc_config.json"
data = [
    ("1", '{"name": "Alice", "age": 30}', "123"),
    ("2", '{"name": "Bob", "age": 25}', "456"),
    ("3", '{"name": "Charlie", "age": 35}', "999")
]

# Perform enrichment
df = hsbc_indicator_enrichment(spark, config_path, data)

# Show the updated DataFrame
df.show(truncate=False)

# Stop the Spark session
spark.stop()
