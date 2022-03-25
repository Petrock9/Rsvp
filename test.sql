from pyspark.sql.functions import *

df1 = spark.read.table("retail_db.products")
df1.printSchema()

df2 = spark.read.table("retail_db.order_items")

df3 = df2.groupBy("order_item_product_id").agg(sum("order_item_subtotal").alias("Revenue"))

df4 = df3.sort(desc("Revenue")).limit(10)

df5 = df4.join(df1, df4.order_item_product_id == df1.product_id)

df5 = df5.select("product_id", "product_name","Revenue").sort(desc("Revenue"))

out_folder = "/user/petrock/spark-handson/q5"

df5.write.format("csv").option("header" , True).option("sep", ":").mode("overwrite").save(out_folder)

df7 = spark.read.format("csv").option("header" , True).option("sep", ":").option("inferSchema", True).load(out_folder)