import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import rand,col,max,when,datediff,greatest, md5, split, regexp_replace, monotonically_increasing_id
from pyspark.sql.types import StringType
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

################################################################################################################################################################################

# creating frames of product, date, sales_outlet, customer and staff
product = (
    glueContext.create_dynamic_frame.from_catalog(
        database="db-conform-coffee-shop",
        table_name="product"
    ).toDF().select(
        col('productno'), 
        col('product_id')
        ))

date = (
    glueContext.create_dynamic_frame.from_catalog(
        database="db-conform-coffee-shop",
        table_name="date"
    ).toDF().select(
        col('date'),
        col('date_id'),
        col('monthname'),
        col('month_id')
        ))

sales_outlet = (
    glueContext.create_dynamic_frame.from_catalog(
        database="db-conform-coffee-shop",
        table_name="sales_outlet"
    ).toDF().select(
        col('salesoutletno'), 
        col('salesoutlet_id')
        ))

customer = (
    glueContext.create_dynamic_frame.from_catalog(
        database="db-conform-coffee-shop",
        table_name="customer"
    ).toDF().select(
        col('customerno'), 
        col('customer_id')
        ))

staff = (
    glueContext.create_dynamic_frame.from_catalog(
        database="db-conform-coffee-shop",
        table_name="staff"
    ).toDF().select(
        col('staffno'), 
        col('staff_id')
        ))

################################################################################################################################################################################

# creating sales_reciept table
sales_reciept = (
    glueContext.create_dynamic_frame.from_catalog(
        database="db-clean-coffee-shop",
        table_name="sales_reciept"
    ).toDF().select(
        col('transactionno').cast(StringType()),
        col('transactiontime'),
        col('date'),
        col('salesoutletno'),
        col('staffno'),
        col('customerno'),
        col('productno'),
        col('quantity'),
        col('unitprice'),
        col('saleamount')
        ))

sales_reciept = sales_reciept.distinct()

################################################################################################################################################################################

# joining tables
sales_reciept = (
    sales_reciept
    .join(date, date.date == sales_reciept.date, "left")
    .join(sales_outlet, sales_outlet.salesoutletno == sales_reciept.salesoutletno, "left")
    .join(staff, staff.staffno == sales_reciept.staffno, "left")
    .join(customer, customer.customerno == sales_reciept.customerno, "left")
    .join(product, product.productno == sales_reciept.productno, "left")
    .select(
        col('transactionno').cast(StringType()),
        col('transactiontime'),
        col('date_id'),
        col('salesoutlet_id'),
        col('staff_id'),
        col('customer_id'),
        col('product_id'),
        col('quantity'),
        col('unitprice'),
        col('saleamount')
    )
)

sales_reciept = sales_reciept.withColumn('transaction_id', md5(col('transactionno')))

################################################################################################################################################################################

# saving to the conform-data-catalog  
sales_reciept = DynamicFrame.fromDF(sales_reciept, glueContext, "sales_reciept")
glueContext.purge_s3_path("s3://conform-coffee-shop-ms/sales_reciept/", {"retentionPeriod": 0})
glueContext.write_dynamic_frame.from_options(frame = sales_reciept,
          connection_type = "s3",
          connection_options = {"path": "s3://conform-coffee-shop-ms/sales_reciept/"},
          format = "parquet")
job.commit()


################################################################################################################################################################################

# creating sales_target table
sales_target = (
    glueContext.create_dynamic_frame.from_catalog(
        database="db-clean-coffee-shop",
        table_name="sales_target"
    ).toDF().select(
        col('salesoutletno').cast(StringType()),
        col('monthyear'),
        col('beans'),
        col('beverage'),
        col('food'),
        col('merchendise'),
        col('totalgoal')
        ))

sales_target = sales_target.distinct()

sales_target = sales_target.withColumn('monthname', split(sales_target['monthyear'], ' ').getItem(0))\
                           .withColumn('year', split(sales_target['monthyear'], ' ').getItem(1))

################################################################################################################################################################################

# joining tables                          
sales_target = (
    sales_target
    .join(date, date.monthname == sales_target.monthname, "left")
    .join(sales_outlet, sales_outlet.salesoutletno == sales_target.salesoutletno, "left")
    .select(
        col('salesoutlet_id').cast(StringType()),
        col('monthyear'),
        col('month_id'),
        col('year'),
        col('beans'),
        col('beverage'),
        col('food'),
        col('merchendise'),
        col('totalgoal'),
        monotonically_increasing_id().alias('salestargetno').cast(StringType())
        )
)

sales_target = sales_target.withColumn('sales_target_id', md5(col('salestargetno')))

################################################################################################################################################################################

# saving to the conform-data-catalog
sales_target = DynamicFrame.fromDF(sales_target, glueContext, "sales_target")
glueContext.purge_s3_path("s3://conform-coffee-shop-ms/sales_target/", {"retentionPeriod": 0})
glueContext.write_dynamic_frame.from_options(frame = sales_target,
          connection_type = "s3",
          connection_options = {"path": "s3://conform-coffee-shop-ms/sales_target/"},
          format = "parquet")
job.commit()


################################################################################################################################################################################

# creating pastry_inventory table
pastry_inventory = (
    glueContext.create_dynamic_frame.from_catalog(
        database="db-clean-coffee-shop",
        table_name="pastry_inventory"
    ).toDF().select(
        col('salesoutletno').cast(StringType()),
        col('productno'),
        col('stoctquantity'),
        col('soldquantity'),
        col('waste'),
        col('wastepercentage'),
        col('date')
        ))

pastry_inventory = pastry_inventory.distinct()

################################################################################################################################################################################

# joining tables
pastry_inventory = (
    pastry_inventory.join(product, product.productno == pastry_inventory.productno, "left")
    .join(date, date.date == pastry_inventory.date, "left")
    .join(sales_outlet, sales_outlet.salesoutletno == pastry_inventory.salesoutletno, "left")
    .select(
        col('salesoutlet_id'),
        col('product_id'),
        col('stoctquantity'),
        col('soldquantity'),
        col('waste'),
        col('wastepercentage'),
        col('date_id'),
        monotonically_increasing_id().alias('inventoryno').cast(StringType())
    )
)

pastry_inventory = pastry_inventory.withColumn('inventory_id', md5(col('inventoryno')))

################################################################################################################################################################################

# saving to the conform-data-catalog 
pastry_inventory = DynamicFrame.fromDF(pastry_inventory, glueContext, "pastry_inventory")
glueContext.purge_s3_path("s3://conform-coffee-shop-ms/pastry_inventory/", {"retentionPeriod": 0})
glueContext.write_dynamic_frame.from_options(frame = pastry_inventory,
          connection_type = "s3",
          connection_options = {"path": "s3://conform-coffee-shop-ms/pastry_inventory/"},
          format = "parquet")
job.commit()