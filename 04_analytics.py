# Databricks notebook source
df = spark.table("books_transformed")

# COMMAND ----------

df.groupBy("genre").count().show()

# COMMAND ----------

df.groupBy("genre") \
  .sum("total_copies") \
  .withColumnRenamed("sum(total_copies)", "total_copies_per_genre") \
  .show()

# COMMAND ----------

df.orderBy(df.book_age.desc()).show(5)