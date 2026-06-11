from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, TimestampType
import datetime

# Initialize Spark Session (Not needed if running directly inside a Fabric/Databricks notebook)
spark = SparkSession.builder \
    .appName("NTT_Data_Medallion_Pipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Mocking a pipeline run ID for the audit columns
PIPELINE_RUN_ID = "RUN_" + datetime.datetime.now().strftime("%Y%m%d%H%M%S")
