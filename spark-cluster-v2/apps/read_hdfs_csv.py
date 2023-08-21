# Import modules
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType
from pyspark.sql.functions import unix_timestamp, from_unixtime

spark = SparkSession.builder.appName("Read CSV from HDFS").getOrCreate()

# Define schema for the csv file
schema = StructType([
    StructField("Date", DateType(), True),
    StructField("Time", TimestampType(), True),
    StructField("Size", IntegerType(), True),
    StructField("Version", StringType(), True),
    StructField("Os", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Ip_id", IntegerType(), True)
])

# Read csv file from HDFS
df = spark.read.csv("hdfs://namenode:9000/data/2023-07-23-r.csv", schema=schema, header=True)

# Print out
print(df.show())

