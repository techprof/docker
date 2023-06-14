import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize contexts and job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(sys.argv[0], args)

# Read source data
source_table = glueContext.create_dynamic_frame.from_catalog(database="your_database", table_name="your_table_name")

# Filter data and remove null values
filtered_data = Filter.apply(frame=source_table, f=lambda x: x["year"] >= "2023" and x["month"] >= "03" and x["day"] >= "01" and x["hour"] >= "00" and x["year"] <= "2023" and x["month"] <= "05" and x["day"] <= "31" and x["hour"] <= "23" and x["year"] is not None and x["month"] is not None and x["day"] is not None and x["hour"] is not None)

# Repartition data based on day
repartitioned_data = Repartition.apply(frame=filtered_data, num_partitions=31, partition_func=lambda x: int(x["day"]))

# Convert dynamic frame to Spark DataFrame
converted_data = repartitioned_data.toDF()

# Write the transformed data to a new table
converted_data.write.partitionBy("day").format("parquet").mode("overwrite").saveAsTable("your_new_table_name")

job.commit()
