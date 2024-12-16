# Databricks notebook source
from pyspark.sql import SparkSession
spark = (SparkSession.builder.appName("usecase").master("local[*]").getOrCreate())
spark

# COMMAND ----------

df = spark.read.option("multiline","True").json("/FileStore/tables/sample_logs.json")

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import to_date

df_date = df.withColumn("Timestamp",to_date("timestamp"))
df_date.display()

# COMMAND ----------

df_filter_error = df_date.filter("log_level == 'ERROR'")
df_filter_error.display()

# COMMAND ----------

from pyspark.sql.functions import current_date,date_sub,col
df_filter_date = df_filter_error.filter(col("timestamp") >= date_sub(current_date(),7))
df_filter_date.display()

# COMMAND ----------

df_group = df_filter_date.groupBy("server_id").count()
df_group.display()

# COMMAND ----------

from pyspark.sql.functions import desc
df_order = df_group.orderBy(col("count").desc())
df_order.display()

# COMMAND ----------

df_top_3 = df_order.limit(3)
df_top_3.display()

# COMMAND ----------

df_order2 = df_filter_date.groupBy("server_id","timestamp").count().orderBy(col("timestamp").desc(),col("server_id"))
df_order2.display()

# COMMAND ----------

from pyspark.sql.functions import avg
df_average = df_order2.groupBy("server_id").agg(avg("count").alias("avg_count")).orderBy(col("server_id"))
df_average.display()

# COMMAND ----------

df_order3 = df.groupBy("log_level","message").count()
df_order3.display()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank
df_window = Window.partitionBy("log_level").orderBy(col("count").desc())
df_rank = rank().over(df_window)
df_summary = df_order3.withColumn("rank",df_rank)
df_summary.display()

# COMMAND ----------

top_messages = df_summary.filter(col("rank")==1).select("log_level","message","count")
top_messages.show(truncate=False)

# COMMAND ----------


