import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read data from input S3
df = spark.read.option("header", "true").csv("s3://input-bucket/data/")

# Simple transformation: select only 2 columns
df = df.select("id", "name")

# Write to output S3
df.write.mode("overwrite").parquet("s3://output-bucket/processed/")

job.commit()
