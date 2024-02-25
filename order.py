from pyspark.sql.functions import col, filter
from pyspark.sql.types import DoubleType

def order_cars_by_price_desc_per_make_model(df):
    df_prices = df.select("make", "model", "version", col("price.consumerValue.gross").alias("price"))
    df_prices_cleaned = df_prices.withColumn("price", df_prices["price"].cast(DoubleType()))

    df_cars_by_price_list = []
    for make in df_prices_cleaned.select("make").distinct().collect():
        df_make = df_prices_cleaned.filter(df_prices_cleaned.make == make["make"])
        for model in df_make.select("model").distinct().collect():
            if model["model"] is None:
                df_make_model = df_make.where(col("model").isNull())
            else:
                df_make_model = df_make.filter(df_make.model == model["model"])
            df_cars_by_price_list.append(df_make_model.sort(df_make_model.price.desc()))
    return df_cars_by_price_list