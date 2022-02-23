# Databricks notebook source
# MAGIC %md #### In-class ETL workshop

# COMMAND ----------

# MAGIC %md ##### Read dataset from S3

# COMMAND ----------

from pyspark.sql.functions import datediff, current_date, avg
from pyspark.sql.types import IntegerType

# COMMAND ----------

df_laptimes = spark.read.csv('s3://columbia-gr5069-main/raw/lap_times.csv', header=True)

# COMMAND ----------

df_drivers = spark.read.csv('s3://columbia-gr5069-main/raw/drivers.csv', header=True)

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

# MAGIC %md ##### Transform data

# COMMAND ----------

df_drivers = df_drivers.withColumn('age', datediff(current_date(), df_drivers.dob)/365)
display(df_drivers)

# COMMAND ----------

df_drivers = df_drivers.withColumn('age', df_drivers.age.cast(IntegerType()))

# COMMAND ----------

df_drivers_laptime = df_drivers.select('driverId','forename','surname','nationality','age').join(df_laptimes, on='driverId')

# COMMAND ----------

df_laptimes.count()

# COMMAND ----------

df_drivers.count()

# COMMAND ----------

df_drivers_laptime.count()

# COMMAND ----------

# MAGIC %md ##### Aggregate by age

# COMMAND ----------

df_drivers_laptime = df_drivers_laptime.groupby('surname','age').agg(avg('milliseconds'))

# COMMAND ----------

# MAGIC %md ##### Loading data into S3

# COMMAND ----------

df_drivers_laptime.write.csv('s3://cn-gr5069/processed/in-class-workshop/drivers_laptime.csv')

# COMMAND ----------


