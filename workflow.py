import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf
from pyspark.sql.types import StringType
import json as pyjson

def load_config(config_path):
    with open(config_path, 'r') as file:
        config = json.load(file)
    return config

def load_hsbc_client_list(spark, csv_path):
    return spark.read.csv(csv_path, header=True, inferSchema=True)

def add_key_value(json_str, new_key, new_value):
    if not json_str:
        return pyjson.dumps({new_key: new_value})
    json_obj = pyjson.loads(json_str)
    json_obj[new_key] = new_value
    return pyjson.dumps(json_obj)

def hsbc_indicator_enrichment(spark, config_path, data):
    # Load configuration
    config = load_config(config_path)

    # Step 1: Load the hsbc_client_list.csv file into a DataFrame
    hsbc_client_list = load_hsbc_client_list(spark, config['paths']['hsbc_client_list'])

    # Step 2: Original DataFrame with client_number and additional_features
    columns = ["id", "additional_features", "client_number"]
    df = spark.createDataFrame(data, columns)

    # Step 3: Create a temporary column hsbc_indicator in the original DataFrame
    df = df.join(hsbc_client_list, df.client_number == hsbc_client_list.clnt_no, "left_outer") \
           .withColumn("hsbc_indicator", when(col("clnt_spcfc_cd_val").isNotNull(), col("clnt_spcfc_cd_val")).otherwise("N")) \
           .drop("clnt_no", "clnt_spcfc_cd_val")

    # Step 4: Define a UDF to add the key-value pair to the JSON string
    add_key_value_udf = udf(lambda json_str, new_value: add_key_value(json_str, "hsbc_indicator", new_value), StringType())

    # Step 5: Use the UDF to add the hsbc_indicator to the JSON field additional_features
    df = df.withColumn("additional_features", add_key_value_udf(col("additional_features"), col("hsbc_indicator"))) \
           .drop("hsbc_indicator")

    return df
