from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf, lit
from pyspark.sql.types import StringType
import json

# Initialize Spark session
spark = SparkSession.builder.appName("HSBCIndicator").getOrCreate()

# Step 1: Load the hsbc_client_list.csv file into a DataFrame
hsbc_client_list = spark.read.csv("path/to/hsbc_client_list.csv", header=True, inferSchema=True)

# Sample data for demonstration (replace with actual read from CSV)
# hsbc_client_list = spark.createDataFrame([
#     ("123", "Y"),
#     ("456", "N"),
#     ("789", "Y")
# ], ["clnt_no", "clnt_spcfc_cd_val"])

# Step 2: Original DataFrame with client_number and additional_features
data = [("1", '{"name": "Alice", "age": 30}', "123"),
        ("2", '{"name": "Bob", "age": 25}', "456"),
        ("3", '{"name": "Charlie", "age": 35}', "999")]

columns = ["id", "additional_features", "client_number"]
df = spark.createDataFrame(data, columns)

# Step 3: Create a temporary column hsbc_indicator in the original DataFrame
df = df.join(hsbc_client_list, df.client_number == hsbc_client_list.clnt_no, "left_outer") \
       .withColumn("hsbc_indicator", when(col("clnt_spcfc_cd_val").isNotNull(), col("clnt_spcfc_cd_val")).otherwise("N")) \
       .drop("clnt_no", "clnt_spcfc_cd_val")

# Step 4: Define a UDF to add the key-value pair to the JSON string
def add_key_value(json_str, new_key, new_value):
    if not json_str:
        return json.dumps({new_key: new_value})
    json_obj = json.loads(json_str)
    json_obj[new_key] = new_value
    return json.dumps(json_obj)

add_key_value_udf = udf(lambda json_str, new_value: add_key_value(json_str, "hsbc_indicator", new_value), StringType())

# Step 5: Use the UDF to add the hsbc_indicator to the JSON field additional_features
df = df.withColumn("additional_features", add_key_value_udf(col("additional_features"), col("hsbc_indicator"))) \
       .drop("hsbc_indicator")

# Show the updated DataFrame
df.show(truncate=False)

# Stop the Spark session
spark.stop()
