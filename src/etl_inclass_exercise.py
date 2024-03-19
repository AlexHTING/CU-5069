# Databricks notebook source
# MAGIC %md ### Workshop for ETL ###

# COMMAND ----------

from pyspark.sql.functions import datediff, current_date
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import avg

# COMMAND ----------

df_laptimes = spark.read.csv('s3://columbia-gr5069-main/raw/lap_times.csv',header=True)

# COMMAND ----------

display(df_laptimes)

# COMMAND ----------

df_driver = spark.read.csv('s3://columbia-gr5069-main/raw/drivers.csv', header=True)
df_driver.count()

# COMMAND ----------

display(df_driver)

# COMMAND ----------

df_driver = df_driver.withColumn("age" , datediff(current_date(),df_driver.dob)/365)

# COMMAND ----------

df_driver = df_driver.withColumn("age" , df_driver["age"].cast(IntegerType()))

# COMMAND ----------

display(df_driver)

# COMMAND ----------

df_lap_drivers = df_driver.select('driverID','nationality','age','forename','surname','url').join(df_laptimes, on=["driverID"])

# COMMAND ----------

display(df_lap_drivers)

# COMMAND ----------

# MAGIC %md
# MAGIC Aggregate By Age

# COMMAND ----------

df_lap_drivers=df_lap_drivers.groupBy('nationality','age').agg(avg('milliseconds'))
display(df_lap_drivers)

# COMMAND ----------

# MAGIC %md
# MAGIC storing data into s3

# COMMAND ----------

df_lap_drivers.write.csv('s3://hn2401-gr5069/processed/in_class_exercise/laptimes_by_drivers.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC Done
