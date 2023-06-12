import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, to_utc_timestamp, from_utc_timestamp
from pyspark.sql.types import DateType
from datetime import datetime, timedelta

# Get the current timestamp
current_timestamp = datetime.now()

# Calculate the start date for the 90-day window
start_date = current_timestamp - timedelta(days=90)

# Convert the start date to ISO8601 format
start_date_iso8601 = start_date.isoformat()

# Create a GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Create a DynamicFrame from the source table
source_dyf = glueContext.create_dynamic_frame.from_catalog(database="<database_name>", table_name="<table_name>")

# Convert the DynamicFrame to a DataFrame
source_df = source_dyf.toDF()

# Apply filtering and conversion operations
filtered_df = source_df.filter((col("year") > start_date.year) |
                               (col("year") == start_date.year) & (col("month") > start_date.month) |
                               (col("year") == start_date.year) & (col("month") == start_date.month) & (col("day") >= start_date.day) |
                               (col("year") == start_date.year) & (col("month") == start_date.month) & (col("day") == start_date.day) & (col("hour") >= start_date.hour))

filtered_df = filtered_df.filter(~col("column_name").isNull())

filtered_df = filtered_df.withColumn("date_column", to_utc_timestamp(from_utc_timestamp(col("date_column"), "GMT"), "UTC").cast(DateType()))

# Convert the date_column to ISO8601 format
filtered_df = filtered_df.withColumn("date_column_iso8601", col("date_column").cast("string"))

# Write the filtered DataFrame to a new table
glueContext.write_dynamic_frame.from_catalog(
    frame=DynamicFrame.fromDF(filtered_df, glueContext, "filtered_dyf"),
    database="<database_name>",
    table_name="<new_table_name>",
    transformation_ctx="write_to_table"
)
