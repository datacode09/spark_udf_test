from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import json

def add_key_value_to_json(df, json_col, new_key, new_value):
    # Define a UDF to add the key-value pair to the JSON string
    def add_key_value(json_str):
        if not json_str:
            return json.dumps({new_key: new_value})
        json_obj = json.loads(json_str)
        json_obj[new_key] = new_value
        return json.dumps(json_obj)
    
    add_key_value_udf = udf(add_key_value, StringType())

    # Apply the UDF to the DataFrame
    updated_df = df.withColumn(json_col, add_key_value_udf(col(json_col)))

    return updated_df

# Example usage
if __name__ == "__main__":
    spark = SparkSession.builder.appName("AddKeyValueToJson").getOrCreate()

    # Sample data
    data = [("1", '{"name": "Alice", "age": 30}'),
            ("2", '{"name": "Bob", "age": 25}'),
            ("3", '{"name": "Charlie", "age": 35}')]

    columns = ["id", "json_data"]

    df = spark.createDataFrame(data, columns)

    # Add new key-value pair
    new_key = "city"
    new_value = "New York"

    updated_df = add_key_value_to_json(df, "json_data", new_key, new_value)
    
    updated_df.show(truncate=False)
    spark.stop()
