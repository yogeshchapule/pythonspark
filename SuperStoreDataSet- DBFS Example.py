# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

import pyspark
from pyspark.sql.functions import *

# COMMAND ----------

            # File location and type
# file_location = "/FileStore/tables/yoesh/superstore.csv"
# file_type = "csv"
# # CSV options
# infer_schema = "false"
# first_row_is_header = "false"
# delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
# df = spark.read.format(file_type) \
#   .option("inferSchema", infer_schema) \
#   .option("header", first_row_is_header) \
#   .option("sep", delimiter) \
#   .load(file_location
df=spark.read.option("inferSchema","true").option("sep",",").csv("dbfs:/FileStore/tables/yoesh/superstore.csv",header=True)

display(df)

# COMMAND ----------

df.select('PostalCode').where(df['PostalCode']>40000).distinct().show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.dtypes

# COMMAND ----------

df.head()

# COMMAND ----------

df.tail(1)

# COMMAND ----------

df.first()

# COMMAND ----------

df.take(1)

# COMMAND ----------

# df1=df.select("Country","State").where("Country='India'").distinct()
df2=df.select(df['Country'],df['State']).show()

# COMMAND ----------

df2.collect()


# COMMAND ----------

df.filter()

# COMMAND ----------

df3=df.select("country","state").where("Country='India'").distinct().show()


# COMMAND ----------

df3=df.select('country','state').dropDuplicates(["state"])
df3.show()

# COMMAND ----------

df.show(3)


# COMMAND ----------

df5=df.groupBy('country').agg(count("country").alias("countState")).first()
df5


# COMMAND ----------

# Create a view or table

# emp_table_name = "superstore_csv"

# df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# %sql

# /* Query the created temp table in a SQL cell */

# select * from `superstore_csv`

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

# permanent_table_name = "superstore_csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

df.show(5)

# COMMAND ----------


