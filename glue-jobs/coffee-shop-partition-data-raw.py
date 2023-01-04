import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import col

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()


# partition pastry_inventory data with 'TranactionDate' column
raw_pastry_inventory = (
    glueContext.create_dynamic_frame.from_catalog(database="db-raw-coffee-shop", table_name="pastry_inventory").withColumn('date', col('transactiondate'))
    )

# save to the clean-data-catalog  
glueContext.write_dynamic_frame.from_options(frame = raw_pastry_inventory,
            connection_type = "s3",
            connection_options = {"path": "s3://clean-coffee-shop/pastry-inventory/", "partitionKeys": ["date"]},
            format = "parquet")
  

################################################################################################################################################################################    


# partition sales_reciept data with 'transactiondate' column
raw_sales_reciept = (
    glueContext.create_dynamic_frame.from_catalog(database="db-raw-coffee-shop", table_name="sales_reciept").withColumnRenamed('transactiondate', 'date')
    )

# save to the clean-data-catalog  
glueContext.write_dynamic_frame.from_options(frame = raw_sales_reciept,
            connection_type = "s3",
            connection_options = {"path": "s3://clean-coffee-shop/sales-reciept/", "partitionKeys": ["date"]},
            format = "parquet")


################################################################################################################################################################################           
       
            
# partition sales_target data with 'TranactionDate' column
raw_sales_target = (
    glueContext.create_dynamic_frame.from_catalog(database="db-raw-coffee-shop", table_name="sales_target")
    )

# save to the clean-data-catalog  
glueContext.write_dynamic_frame.from_options(frame = raw_sales_target,
            connection_type = "s3",
            connection_options = {"path": "s3://clean-coffee-shop/sales-target/"},
            format = "parquet")
