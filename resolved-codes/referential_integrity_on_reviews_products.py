import spark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RefIntegrityCheck").getOrCreate()

reviews = spark.read.option("multiline","true").json(r"D:\Data-Engineering\ecom-medallion\data\dq_rules_sample_datasets\04_referential_integrity\r3_reviews_file_customer_id_and_product_id_in_Customer_Reviews_should_have_corresponding_entries_in_Customers_and_Products_Tables")
products = spark.read.option("multiline","true").json(r"D:\Data-Engineering\ecom-medallion\data\dq_rules_sample_datasets\04_referential_integrity\r3_products_file_customer_id_and_product_id_in_Customer_Reviews_should_have_corresponding_entries_in_Customers_and_Products_Tables")

invalid_products = reviews.join(products, on="product_id", how="leftanti")
invalid_products.show()

invalid_products.write.mode("overwrite").json(r"D:\Data Engineering\ecom-dq\dq-files\invalid_data\invalid_products_reviews")