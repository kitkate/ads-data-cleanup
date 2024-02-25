from pyspark.sql.functions import col, filter, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

def top_3_cars_by_price_desc_per_make_model(df):
    df_prices = df.select("make", "model", "version", col("price.consumerValue.gross").alias("price"))
    df_prices_cleaned = df_prices.withColumn("price", df_prices["price"].cast(DoubleType()))

    windowDept = Window.partitionBy("make", "model").orderBy(col("price").desc())
    return df_prices_cleaned.withColumn("row",row_number().over(windowDept)) \
                            .filter(col("row") <= 3)