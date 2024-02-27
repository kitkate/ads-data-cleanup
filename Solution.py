# Databricks notebook source
# MAGIC %md
# MAGIC # Coding Challenge - Data Engineering Deliverables
# MAGIC ## Initialization
# MAGIC
# MAGIC In order to run this notebook \<storage-account-name\>, \<storage-key\>, \<container-name\>, \<path-to-ads.json\>, \<path-to-views.json\> would be needed.
# MAGIC
# MAGIC * **\<storage-account-name\>** is the name of your storage account.
# MAGIC * **\<storage-key\>** is the an access key for the storage account that we can be find from Azure. Navigate to your storage account in the Azure Portal and click on ‘Access keys’ under ‘Settings’. Copy one of the keys -  this is the access key.
# MAGIC * **\<container-name\>** is the name of the container inside the storage account where the data is stored.
# MAGIC * **\<path-to-ads.json\>** is the path to the ads.json file
# MAGIC * **\<path-to-views.json\>** is the path to the views.json file
# MAGIC
# MAGIC After the initialization is ran the data will be in dataframes **df_ads** and **df_views**

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.<storage-account-name>.dfs.core.windows.net",
    "<storage-key>",
)
dbutils.fs.ls("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/")
df_ads = spark.read.load(
    "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-ads.json>", format="json"
)
df_views = spark.read.load(
    "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-views.json>", format="json"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## A cleanup function for ads
# MAGIC
# MAGIC The function **cleanup.cleanup_colors** takes a df and returns a df with fixed colors. The first table shows the original colors names and the second the fixed ones.

# COMMAND ----------

import cleanup

df_ads.select("id", "attributes.exteriorColor").show(df_ads.count())

cleanup.cleanup_colors(df_ads).select("id", "attributes.exteriorColor").show(
    df_ads.count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The top 3 most expensive cars (gross) per make/model
# MAGIC
# MAGIC It seems that the **version** in adds.json corresponds to the ad version and not to the car version. And the **id** corresponds to the particular car and not the ad because there are entries with duplicate ids and different version. Upon closer inspection became clear that the entries with same id were differing only by version. The output is orginized with all the relevant information about the cars plus **row** which is the number in the most expensive list.

# COMMAND ----------

import query

query.top_3_cars_by_price_desc_per_make_model(df_ads).show(df_ads.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## The top 3 most viewed ads per color
# MAGIC
# MAGIC Ads are defined uniquely by **id** and **version**. The number of times they have been seen is **count**. Row is the row number in the most views ad per color list.

# COMMAND ----------

import query

df_top_3_ads_per_color = query.top_3_most_viewed_ads_per_color(df_ads, df_views)
df_top_3_ads_per_color.show(df_top_3_ads_per_color.count())


# COMMAND ----------

# MAGIC %md
# MAGIC ## View times
# MAGIC
# MAGIC For each ad  the oldest view, the most-recent view and the time inbetween the 2 are extracted. The oldest and most-recent are given as `TimestampType`, but the time inbetween is given in second (it does not make sense to be `TimestampType`).
# MAGIC
# MAGIC The information is exported as csv to `/FileStore/add_view_times.csv` . It can be downloaded at `<this-databricks-url>/files/add_view_times.csv`

# COMMAND ----------

import query

df_times = query.get_oldest_most_recent_time_between(df_views)
df_times.coalesce(1).write.options(header='True').mode('overwrite').csv("/tmp/add_view_times")

dbutils.fs.rm("dbfs:/FileStore/add_view_times.csv")
for file_info in dbutils.fs.ls("/tmp/add_view_times/"):
    if file_info.name.endswith(".csv"):
        dbutils.fs.mv(file_info.path, "dbfs:/FileStore/add_view_times.csv")
        break
dbutils.fs.rm("dbfs:/tmp/add_view_times", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The inconsistency
# MAGIC
# MAGIC The **id** in ads.json does not pertain to the ads but to the cars. The unique identifier for ads as it stand now consists of both **id** and **version**. To mitigate this a new column might be created - **ad_id** consisting of the concatenated values of **id** and **version** with an **_** between them. Additionally the current column **id** should be renamed as **car_id** as to not be confused.
