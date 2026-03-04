# Databricks notebook source
df = spark.table("books_clean")

# COMMAND ----------

from pyspark.sql.functions import col, lit, year, current_date

trans_df = df.withColumn(
    "book_age",
    year(current_date()) - col("publication_year")
)

# COMMAND ----------

display(trans_df)

# COMMAND ----------

trans_df.write.mode("overwrite").saveAsTable("books_transformed")

# COMMAND ----------

spark.table("books_transformed").show()