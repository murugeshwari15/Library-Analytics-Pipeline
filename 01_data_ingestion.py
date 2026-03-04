# Databricks notebook source
df = spark.table("books_raw")
df.show()

# COMMAND ----------

df = spark.table("books_raw")

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()