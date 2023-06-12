import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize GlueContext and SparkContext
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Create a dynamic frame from the existing table
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="your_database_name",
    table_name="your_table_name"
)

# Convert the dynamic frame to a Spark DataFrame
data_frame = dynamic_frame.toDF()

# Filter out null data
filtered_data_frame = data_frame.filter(data_frame.your_column_name.isNotNull())

# Create a new dynamic frame from the filtered DataFrame
new_dynamic_frame = DynamicFrame.fromDF(filtered_data_frame, glueContext, "new_dynamic_frame")

# Write the new dynamic frame to a new Athena table
glueContext.write_dynamic_frame.from_catalog(
    frame=new_dynamic_frame,
    database="your_database_name",
    table_name="new_table_name",
    transformation_ctx="write_dynamic_frame"
)

job.commit()
