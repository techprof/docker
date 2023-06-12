import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime, timedelta

# Initialize GlueContext and SparkSession
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Retrieve arguments passed from the Glue job
args = getResolvedOptions(sys.argv, ['DATABASE', 'SOURCE_TABLE', 'TARGET_TABLE'])

# Get the source table from the Glue Data Catalog
database = args['DATABASE']
source_table = args['SOURCE_TABLE']
target_table = args['TARGET_TABLE']

# Calculate the date 90 days ago
end_date = datetime.now().date()
start_date = end_date - timedelta(days=90)

# Read the source table data from Glue Data Catalog
source_data_frame = glueContext.create_dynamic_frame.from_catalog(database=database, table_name=source_table)

# Convert the source dynamic frame to a Spark DataFrame
source_df = source_data_frame.toDF()

# Apply the filter to get data within the 90-day range and remove null data
filtered_df = source_df.filter((col("year") > start_date.year) |
                               (col("year") == start_date.year) & (col("month") > start_date.month) |
                               (col("year") == start_date.year) & (col("month") == start_date.month) &
                               (col("day") >= start_date.day))

filtered_df = filtered_df.dropna()

# Create a dynamic frame from the filtered Spark DataFrame
target_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(database=database, table_name=source_table)

# Write the dynamic frame to a new table in the Glue Data Catalog
glueContext.write_dynamic_frame.from_catalog(dynamic_frame=target_dynamic_frame,
                                             database=database,
                                             table_name=target_table,
                                             transformation_ctx="target")

# Print the schema of the new table
print(f"New table schema: {target_dynamic_frame.schema()}")
