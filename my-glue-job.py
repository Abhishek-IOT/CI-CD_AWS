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

# ✅ Read CSV files from input folder
df = spark.read.option("header", "true").csv("s3://aws-glue-assets-640958509818-us-east-1/input/")

# ✅ Example transformation: select only id and name
df = df.select("id", "name")

# ✅ Write to processed/output folder as parquet
df.write.mode("overwrite").parquet("s3://aws-glue-assets-640958509818-us-east-1/output/")
print("Job has been executed Successfully")
job.commit()

