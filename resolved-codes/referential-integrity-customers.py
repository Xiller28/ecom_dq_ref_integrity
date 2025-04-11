import spark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RefIntegrityCheck").getOrCreate()

orders = spark.read.option("multiline","true").json(r"D:\Data-Engineering\ecom-medallion\data\dq_rules_sample_datasets\04_referential_integrity\r1_orders_file_Each_customer_id_in_Orders_Table_should_exist_in_Customers_Table")
customers = spark.read.option("multiline","true").json(r"D:\Data-Engineering\ecom-medallion\data\dq_rules_sample_datasets\04_referential_integrity\r1_customer_file_Each_customer_id_in_Orders_Table_should_exist_in_Customers_Table")

# Check invalid customer_id
invalid_customers = orders.join(customers, on='customer_id', how='left_anti')
invalid_customers.show()

invalid_customers.write.mode("overwrite").json(r"D:\Data Engineering\ecom-dq\dq-files\invalid_data\invalid_customers")
# Check invalid product_id
# invalid_products = orders.join(products, on='product_id', how='left_anti')
# invalid_products.show()
