import spark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RefIntegrityCheck").getOrCreate()

orders = spark.read.option("multiline", "true").json(r"D:\Data-Engineering\ecom-medallion\data\dq_rules_sample_datasets\04_referential_integrity\r2_orders_file_Each_product_id_in_Orders_Table_should_exist_in_Products_Table")
products = spark.read.option("multiline", "true").json(r"D:\Data-Engineering\ecom-medallion\data\dq_rules_sample_datasets\04_referential_integrity\r2_products_file_Each_product_id_in_Orders_Table_should_exist_in_Products_Table")

invalid_products = orders.join(products, on="product_id", how='left_anti')
invalid_products.show()

invalid_products.write.mode("overwrite").json(r"D:\Data Engineering\ecom-dq\dq-files\invalid_data\invalid_products")



