# Databricks notebook source
# MAGIC %md
# MAGIC ## silver layer script

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data using app

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import*

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.tejaswinifirststorage.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.tejaswinifirststorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.tejaswinifirststorage.dfs.core.windows.net", "287c86db-9d71-471d-b890-d13ef3ff93aa")
spark.conf.set("fs.azure.account.oauth2.client.secret.tejaswinifirststorage.dfs.core.windows.net", "jDF8Q~xPZM9Cs1bM_eX2bnvCusoQyKwDF~9.eaQk")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.tejaswinifirststorage.dfs.core.windows.net", "https://login.microsoftonline.com/dc07e55d-3978-4a7d-a43a-835c020b469f/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Loading

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Read calender data

# COMMAND ----------

df_cal = (spark.read.format('csv')
    .option("header",True)
    .option("inferschema", True)
    .load('abfss://bronze@tejaswinifirststorage.dfs.core.windows.net/AdventureWorks_Calendar'))

# COMMAND ----------

df_cus = (spark.read.format('csv')
    .option("header",True)
    .option("inferschema", True)
    .load('abfss://bronze@tejaswinifirststorage.dfs.core.windows.net/AdventureWorks_Customers'))

# COMMAND ----------

df_procat = (spark.read.format('csv')
    .option("header",True)
    .option("inferschema", True)
    .load('abfss://bronze@tejaswinifirststorage.dfs.core.windows.net/AdventureWorks_Product_Categories'))

# COMMAND ----------

df_pro = (spark.read.format('csv')
    .option("header",True)
    .option("inferschema", True)
    .load('abfss://bronze@tejaswinifirststorage.dfs.core.windows.net/AdventureWorks_Products'))

# COMMAND ----------

df_ret = (spark.read.format('csv')
    .option("header",True)
    .option("inferschema", True)
    .load('abfss://bronze@tejaswinifirststorage.dfs.core.windows.net/AdventureWorks_Returns'))

# COMMAND ----------

df_prod = (spark.read.format('csv')
    .option("header",True)
    .option("inferschema", True)
    .load('abfss://bronze@tejaswinifirststorage.dfs.core.windows.net/AdventureWorks_Sales*'))

# COMMAND ----------

df_ter = (spark.read.format('csv')
    .option("header",True)
    .option("inferschema", True)
    .load('abfss://bronze@tejaswinifirststorage.dfs.core.windows.net/AdventureWorks_Territories'))

# COMMAND ----------

df_subcat = (spark.read.format('csv')
    .option("header",True)
    .option("inferschema", True)
    .load('abfss://bronze@tejaswinifirststorage.dfs.core.windows.net/AdventureWorks_Product_Subcategories'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ### calender

# COMMAND ----------

df_cal.display()

# COMMAND ----------

df_cal = df.withColumn('month',month(col('Date')))\
    .withColumn('year',year(col('Date')))
df_cal.display()

# COMMAND ----------

df_cal.write.format('parquet')\
    .mode('append')\
     .option("path",'abfss://silver@tejaswinifirststorage.dfs.core.windows.net/AdventureWorks_Calender')\
     .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### customers
# MAGIC

# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_cus.withColumn('fullname',concat(col('prefix'),lit(''),col('firstname'),lit(''),col('lastname'))).display()

# COMMAND ----------

df_cus = df_cus.withColumn('full name',concat_ws('',col('prefix'),col('firstname'),col('lastname')))

# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_cus.write.format('parquet')\
    .mode('append')\
     .option("path",'abfss://silver@tejaswinifirststorage.dfs.core.windows.net/AdventureWorks_Customer')\
     .save()

# COMMAND ----------

# MAGIC %md
# MAGIC subcategories

# COMMAND ----------

df_subcat.display()

# COMMAND ----------

df_cus.write.format('parquet')\
    .mode('append')\
     .option("path",'abfss://silver@tejaswinifirststorage.dfs.core.windows.net/AdventureWorks_Product_Subcategories')\
     .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### products
# MAGIC

# COMMAND ----------

df_pro.display()

# COMMAND ----------

from pyspark.sql.functions import split, col
df_pro=df_pro.withColumn('productsku',split(col('Productsku'),'-')[0])\
             .withColumn('productName',split(col('ProductName'),' ')[0])  


# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro.write.format('parquet')\
    .mode('append')\
     .option("path",'abfss://silver@tejaswinifirststorage.dfs.core.windows.net/AdventureWorks_Products')\
     .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Returns

# COMMAND ----------

df_ret.display()

# COMMAND ----------

df_ret.write.format('parquet')\
    .mode('append')\
     .option("path",'abfss://silver@tejaswinifirststorage.dfs.core.windows.net/AdventureWorks_Returns')\
     .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Territories

# COMMAND ----------

df_ter.display()

# COMMAND ----------

df_ter.write.format('parquet')\
    .mode('append')\
     .option("path",'abfss://silver@tejaswinifirststorage.dfs.core.windows.net/AdventureWorks_Territories')\
     .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sales
# MAGIC

# COMMAND ----------

df_prod.display()

# COMMAND ----------

df_prod = df_prod.withColumn('stockdate',to_timestamp('stockdate'))

# COMMAND ----------

df_prod = df_prod.withColumn('orderNumber',regexp_replace(col('orderNumber'),'s','T'))

# COMMAND ----------

df_prod = df_prod.withColumn('multiply',col('OrderLineItem')*col('OrderLineItem'))

# COMMAND ----------

df_prod.display()

# COMMAND ----------

df_prod.write.format('parquet')\
    .mode('append')\
     .option("path",'abfss://silver@tejaswinifirststorage.dfs.core.windows.net/AdventureWorks_Sales*')\
     .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### sales analysis

# COMMAND ----------

df_prod.display()

# COMMAND ----------

df_prod.groupBy('OrderDate').agg(count('OrderNumber').alias('total_order')).display()

# COMMAND ----------

df_procat.display()

# COMMAND ----------

df_ter.display()

# COMMAND ----------

