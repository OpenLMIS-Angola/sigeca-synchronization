from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DateType, StringType as UUIDType
from pyspark.sql.functions import col, lit, current_date, expr
import requests
import json

def load_config(file_path='config.json'):
    with open(file_path) as file:
        return json.load(file)
        

def read_facility_data(spark, config):
    facility_schema = StructType([
        StructField("id", UUIDType(), True),
        StructField("active", BooleanType(), True),
        StructField("code", StringType(), True),
        StructField("comment", StringType(), True),
        StructField("description", StringType(), True),
        StructField("enabled", BooleanType(), True),
        StructField("godowndate", DateType(), True),
        StructField("golivedate", DateType(), True),
        StructField("name", StringType(), True),
        StructField("openlmisaccessible", BooleanType(), True),
        StructField("geographiczoneid", UUIDType(), True),
        StructField("operatedbyid", UUIDType(), True),
        StructField("typeid", UUIDType(), True),
        StructField("extradata", StringType(), True),
        StructField("location", StringType(), True)  # Placeholder for user-defined type
    ])

    return spark.read \
        .format("jdbc") \
        .option("url", config['jdbc_url']) \
        .option("dbtable", "referencedata.facilities") \
        .option("user", config['jdbc_user']) \
        .option("password", config['jdbc_password']) \
        .option("driver", config['jdbc_driver']) \
        .schema(facility_schema) \
        .load()

def transform_facility_data(df):
    # Generate a new UUID for the id column
    generate_uuid = expr("uuid()")

    # Apply transformations to match the target schema
    transformed_df = df.withColumn("id", generate_uuid) \
        .withColumn("reference_id", col("id")) \
        .withColumn("last_updated", current_date()) \
        .withColumn("is_deleted", lit(False)) \
        .select(
            col("id"),
            col("reference_id"),
            col("active"),
            col("code"),
            col("comment"),
            col("geographiczoneid").alias("geographic_zone_id"),
            col("description"),
            col("enabled"),
            col("name"),
            col("last_updated"),
            col("is_deleted")
        )
    
    return transformed_df

def send_facility_data(df, config):
    print(df)
    data = df.toJSON().collect()
    print(len(data))
    # response = requests.post(config['external_api_url'], json={"facilities": data})
    # if not response.ok:
    #     raise Exception(f"Failed to send data: {response.text}")

def main():
    config = load_config()

    spark = SparkSession.builder \
        .appName("FullFacilitySync") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23") \
        .getOrCreate()

    facility_df = read_facility_data(spark, config)
    transformed_df = transform_facility_data(facility_df)
    send_facility_data(transformed_df, config)

    spark.stop()

if __name__ == "__main__":
    main()
