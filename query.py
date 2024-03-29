from pyspark.sql.functions import col, filter, row_number, min, max, to_timestamp
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
import cleanup


def top_3_cars_by_price_desc_per_make_model(df):
    df_cleaned_color = cleanup.cleanup_colors(df)
    df_prices = df_cleaned_color.select(
        "id",
        "make",
        "model",
        col("attributes.mileage").alias("mileage"),
        col("attributes.exteriorColor").alias("color"),
        col("price.consumerValue.gross").alias("price"),
    ).withColumn("price", col("price").cast(DoubleType()))

    w_make_model = Window.partitionBy("make", "model").orderBy(col("price").desc())
    return (
        df_prices.drop_duplicates()
        .withColumn("row", row_number().over(w_make_model))
        .filter(col("row") <= 3)
    )


def top_3_most_viewed_ads_per_color(df_ads, df_views):
    df_views_per_ad = (
        df_ads.join(
            df_views,
            (df_ads.id == df_views.ad.id) & (df_ads.version == df_views.ad.version),
        )
        .groupBy(df_ads.id, df_ads.version)
        .count()
    )

    df_cleaned_colors = cleanup.cleanup_colors(df_ads)
    w_color = Window.partitionBy("attributes.exteriorColor").orderBy(
        col("count").desc()
    )
    return (
        df_cleaned_colors.join(df_views_per_ad, ["id", "version"])
        .withColumn("row", row_number().over(w_color))
        .filter(col("row") <= 3)
        .select(
            col("attributes.exteriorColor").alias("color"),
            "id",
            "version",
            "count",
            "row",
        )
    )


def get_oldest_most_recent_time_between(df):
    return (
        df.groupBy("ad.id")
        .agg(max("event.time").alias("most_recent"), min("event.time").alias("oldest"))
        .withColumn("time_active_seconds", col("most_recent") - col("oldest"))
        .withColumn("most_recent", to_timestamp("most_recent"))
        .withColumn("oldest", to_timestamp("oldest"))
    )