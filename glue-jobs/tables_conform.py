import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

from pyspark.sql.functions import rand,col,max,when,datediff,greatest, md5
from pyspark.sql.types import StringType
from awsglue.dynamicframe import DynamicFrame

################################################################################################################################################################################

# creating staff frame
staff = (glueContext.create_dynamic_frame.from_catalog(
    database="db-clean-coffee-shop",
    table_name="sales_reciept"
    ).toDF().select(
        col('staffno').cast(StringType()), 
        col('stafffirstname'), 
        col('stafflastname'), 
        col('staffposition'), 
        col('staffstartdate'))) # toDF - as data frame
job.commit()

staff = staff.distinct()
staff = staff.withColumn('staff_id', md5(col('staffno')))

# converting to dynamic frame and saving to conform-coffee-shop-ms data catalog
staff = DynamicFrame.fromDF(staff, glueContext, "staff")

glueContext.purge_s3_path("s3://conform-coffee-shop-ms/staff/", {"retentionPeriod": 0})

glueContext.write_dynamic_frame.from_options(
    frame = staff,
    connection_type = "s3",
    connection_options = {"path": "s3://conform-coffee-shop-ms/staff/"}, 
    format = "parquet")
job.commit()

################################################################################################################################################################################

# creating customer frame
customer = (glueContext.create_dynamic_frame.from_catalog(
    database="db-clean-coffee-shop",
    table_name="sales_reciept"
    ).toDF().select(
        col('customerno').cast(StringType()), 
        col('customerfirstname'), 
        col('customersurname'), 
        col('customeremail'), 
        col('customersince'), 
        col('loyaltycardnumber'), 
        col('dateofbirth'), 
        col('gender'), 
        col('birthyear'))) # toDF - as data frame
job.commit()

customer = customer.distinct()
customer = customer.withColumn('customer_id', md5(col('customerno')))

# converting to dynamic frame and saving to conform-coffee-shop-ms data catalog
customer = DynamicFrame.fromDF(customer, glueContext, "customer")

glueContext.purge_s3_path("s3://conform-coffee-shop-ms/customer/", {"retentionPeriod": 0})

glueContext.write_dynamic_frame.from_options(
    frame = customer,
    connection_type = "s3",
    connection_options = {"path": "s3://conform-coffee-shop-ms/customer/"}, 
    format = "parquet")
job.commit()

################################################################################################################################################################################

# creating product frame
product = (glueContext.create_dynamic_frame.from_catalog(
    database="db-clean-coffee-shop",
    table_name="sales_reciept"
    ).toDF().select(
        col('productno').cast(StringType()), 
        col('productgroup'), 
        col('productcategory'), 
        col('producttype'), 
        col('productname'), 
        col('productdescription'), 
        col('volume'), 
        col('unitofmeasure'), 
        col('currentwholesaleprice'), 
        col('currentretailprice'), 
        col('istaxexempt'), 
        col('ispromoitem'), 
        col('isnewproduct'))) # toDF - as data frame
job.commit()

product = product.distinct()
product = product.withColumn('product_id', md5(col('productno')))

# converting to dynamic frame and saving to conform-coffee-shop-ms data catalog
product = DynamicFrame.fromDF(product, glueContext, "product")

glueContext.purge_s3_path("s3://conform-coffee-shop-ms/product/", {"retentionPeriod": 0})

glueContext.write_dynamic_frame.from_options(
    frame = product,
    connection_type = "s3",
    connection_options = {"path": "s3://conform-coffee-shop-ms/product/"}, 
    format = "parquet")
job.commit()

################################################################################################################################################################################

# creating sales_outlet frame
sales_outlet = (glueContext.create_dynamic_frame.from_catalog(
    database="db-clean-coffee-shop",
    table_name="sales_reciept"
    ).toDF().select(
        col('salesoutletno').cast(StringType()), 
        col('salesoutlettype'), 
        col('storesquarefeet'), 
        col('storeaddress'), 
        col('storecity'), 
        col('storestateprovince'), 
        col('storetelephone'), 
        col('storepostalcode'), 
        col('storelongitude'), 
        col('storelatitude'), 
        col('neighorhood').alias('neighbourhood'), 
        col('storemanagername'))) # toDF - as data frame
job.commit()

sales_outlet = sales_outlet.distinct()
sales_outlet = sales_outlet.withColumn('salesoutlet_id', md5(col('salesoutletno')))

# converting to dynamic frame and saving to conform-coffee-shop-ms data catalog
sales_outlet = DynamicFrame.fromDF(sales_outlet, glueContext, "sales_outlet")

glueContext.purge_s3_path("s3://conform-coffee-shop-ms/sales_outlet/", {"retentionPeriod": 0})

glueContext.write_dynamic_frame.from_options(
    frame = sales_outlet,
    connection_type = "s3",
    connection_options = {"path": "s3://conform-coffee-shop-ms/sales_outlet/"}, 
    format = "parquet")
job.commit()

################################################################################################################################################################################

# creating date frame
date = (glueContext.create_dynamic_frame.from_catalog(
    database="db-clean-coffee-shop",
    table_name="sales_reciept"
    ).toDF().select(
        col('date'), 
        col('dayofweekno'), 
        col('dayofweekname'), 
        col('dayofmonthno'), 
        col('weekno'), 
        col('weekname'), 
        col('monthno'), 
        col('monthname'), 
        col('quarterno'), 
        col('quartername'), 
        col('year'))) # toDF - as data frame
job.commit()

date = date.distinct()
date = date.withColumn('date_id', md5(col('date')))
date = date.withColumn('month_id', md5(col('monthname')))

# converting to dynamic frame and saving to conform-coffee-shop-ms data catalog
date = DynamicFrame.fromDF(date, glueContext, "date")

glueContext.purge_s3_path("s3://conform-coffee-shop-ms/date/", {"retentionPeriod": 0})

glueContext.write_dynamic_frame.from_options(
    frame = date,
    connection_type = "s3",
    connection_options = {"path": "s3://conform-coffee-shop-ms/date/"}, 
    format = "parquet")
job.commit()