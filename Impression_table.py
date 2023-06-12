import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_utc_timestamp, from_utc_timestamp
from pyspark.sql.types import StringType
from datetime import datetime, timedelta

# Get the current timestamp
current_timestamp = datetime.now()

# Calculate the start date for the 90-day window
start_date = current_timestamp - timedelta(days=90)

# Convert the start date to ISO8601 format with UTC (no offset)
start_date_iso8601 = start_date.replace(tzinfo=None).isoformat() + "Z"

# Create a GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read the source table into a DataFrame
source_df = glueContext.create_dynamic_frame.from_catalog(database="<database_name>", table_name="<table_name>").toDF()

# Apply filtering and conversion operations
filtered_df = source_df.filter((col("year") > start_date.year) |
                               (col("year") == start_date.year) & (col("month") > start_date.month) |
                               (col("year") == start_date.year) & (col("month") == start_date.month) & (col("day") >= start_date.day) |
                               (col("year") == start_date.year) & (col("month") == start_date.month) & (col("day") == start_date.day) & (col("hour") >= start_date.hour))

filtered_df = filtered_df.filter(~col("column_name").isNull())

filtered_df = filtered_df.withColumn("date_column_iso8601", to_utc_timestamp(from_utc_timestamp(col("date_column"), "GMT"), "UTC").cast(StringType()))

# Create a new table with the filtered data
glueContext.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(filtered_df, glueContext, "filtered_dyf"),
    connection_type="s3",
    connection_options={"path": "s3://<bucket_name>/<target_folder>"},
    format="parquet"
)
