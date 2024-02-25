from pyspark.sql.functions import regexp_replace, col

def cleanup_colors(df):
    return df.withColumn("attributes", 
                         col("attributes").withField("exteriorColor",regexp_replace("attributes.exteriorColor", "_", "")))
