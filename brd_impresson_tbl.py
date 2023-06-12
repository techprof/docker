# Import the necessary libraries
import awsglue
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Set the job name
job_name = "QueryTable"

# Get the Glue context
glue_context = GlueContext(getResolvedOptions())

# Create a job
job = Job(glue_context)
job.init(job_name, "QueryTable")

# Get the input table
input_table = "s3://bucket/path/to/table"

# Create a query
query = """
SELECT *
FROM {}
WHERE date_partitioned > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
AND NOT EXISTS (SELECT 1
                FROM {}
                WHERE {} IS NULL)
""".format(input_table, input_table, "date_partitioned")

# Run the query
df = glue_context.create_dynamic_frame.from_options(query=query, options={"database": "default"})

# Create a new Athena table
table_name = "new_table"
df.write.format("athena").mode("overwrite").save(table_name)

# Close the job
job.close()
