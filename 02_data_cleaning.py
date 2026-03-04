# Databricks notebook source
df = spark.table("books_raw")

# COMMAND ----------

df.show(5)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

clean_df = df.dropna()

# COMMAND ----------

clean_df.write.mode("overwrite").saveAsTable("books_clean")

# COMMAND ----------

spark.table("books_clean").show()