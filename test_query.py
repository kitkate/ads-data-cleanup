import unittest
import query


class TestQuery(unittest.TestCase):
    def test_top_3_cars_by_price_desc_per_make_model(self):
        data = [
            (("____BLACK", "48573"), 1, "VW", "Arteon", (("2.00", "2"), "BGN"), "1"),
            (("____WHITE", "48573"), 2, "VW", "Arteon", (("100.00", "2"), "BGN"), "2"),
            (("____BLUE", "48573"), 3, "VW", "Arteon", (("3.00", "2"), "BGN"), "3"),
        ]
        schema = "attributes struct<exteriorColor:string, mileage:string>, id string, make string, model string, price struct<consumerValue:struct<gross:string, net:string>, currency:string>, version string"
        df = spark.createDataFrame(data, schema=schema)

        df_ordered = query.top_3_cars_by_price_desc_per_make_model(df)
        self.assertEqual(
            df_ordered.collect()[0]["price"],
            100.0,
            "Cars not ordered correctly by price",
        )
        self.assertEqual(
            df_ordered.collect()[1]["price"],
            3.0,
            "Cars not ordered correctly by price"
        )
        self.assertEqual(
            df_ordered.collect()[2]["price"],
            2.0,
            "ACars not ordered correctly by price",
        )

    def test_top_3_most_viewed_ads_per_color(self):
        data_ads = [
            (("____BLACK", "48573"), 1, "VW", "Arteon", (("2.00", "2"), "BGN"), "1"),
            (("____BLACK", "48573"), 1, "VW", "Arteon", (("100.00", "2"), "BGN"), "2"),
            (("____WHITE", "48573"), 2, "VW", "Arteon", (("4.00", "2"), "BGN"), "1"),
            (("____WHITE", "48573"), 3, "VW", "Arteon", (("3.00", "2"), "BGN"), "1"),
        ]
        schema_ads = "attributes struct<exteriorColor:string, mileage:string>, id string, make string, model string, price struct<consumerValue:struct<gross:string, net:string>, currency:string>, version string"
        df_ads = spark.createDataFrame(data_ads, schema=schema_ads)

        data_views = [
            (
                ("ad_view_events.AdView", "1", 1234567890),
                ("1", "A"),
                ("10", "1.2.3.4"),
                ("1", "1"),
            ),
            (
                ("ad_view_events.AdView", "2", 1234567891),
                ("2", "AB"),
                ("10", "1.2.4.3"),
                ("1", "1"),
            ),
            (
                ("ad_view_events.AdView", "3", 1234567892),
                ("3", "ABC"),
                ("10", "1.3.2.4"),
                ("1", "2"),
            ),
            (
                ("ad_view_events.AdView", "4", 1234567893),
                ("4", "ABCD"),
                ("10", "1.3.4.2"),
                ("2", "1"),
            ),
            (
                ("ad_view_events.AdView", "5", 1234567894),
                ("5", "ABCDE"),
                ("10", "1.4.2.3"),
                ("2", "1"),
            ),
            (
                ("ad_view_events.AdView", "6", 1234567895),
                ("6", "ABCDEF"),
                ("10", "1.4.3.2"),
                ("3", "1"),
            ),
        ]
        schema_views = "event struct<ns:string, id:string, time:long>, user struct<id:string, cookie:string>, client struct<apiVersion:string, ip:string>, ad struct<id:string, version:string>"
        df_views = spark.createDataFrame(data_views, schema=schema_views)

        df_ordered = query.top_3_most_viewed_ads_per_color(df_ads, df_views)
        self.assertEqual(
            df_ordered.collect()[0]["color"],
            "BLACK",
            "The first color in the order is not corect",
        )
        self.assertEqual(
            df_ordered.collect()[0]["id"],
            "1",
            "The id of the ad with most views is not correct",
        )
        self.assertEqual(
            df_ordered.collect()[0]["version"],
            "1",
            "The version of the car with most views is not correct",
        )
        self.assertEqual(
            df_ordered.collect()[0]["count"],
            2,
            "The number of views for the first ad is not correct",
        )

        self.assertEqual(
            df_ordered.collect()[1]["color"],
            "BLACK",
            "The first color in the order is not corect",
        )
        self.assertEqual(
            df_ordered.collect()[1]["id"],
            "1",
            "The id of the ad with second most views is not correct",
        )
        self.assertEqual(
            df_ordered.collect()[1]["version"],
            "2",
            "The version of the car with second most views is not correct",
        )
        self.assertEqual(
            df_ordered.collect()[1]["count"],
            1,
            "The number of views for the second ad is not correct",
        )

        self.assertEqual(
            df_ordered.collect()[2]["color"],
            "WHITE",
            "The second color in the order is not corect",
        )
        self.assertEqual(
            df_ordered.collect()[2]["id"],
            "2",
            "The id of the ad with most views is not correct",
        )
        self.assertEqual(
            df_ordered.collect()[2]["version"],
            "1",
            "The version of the car with most views is not correct",
        )
        self.assertEqual(
            df_ordered.collect()[2]["count"],
            2,
            "The number of views for the first ad is not correct",
        )

        self.assertEqual(
            df_ordered.collect()[3]["color"],
            "WHITE",
            "The second color in the order is not corect",
        )
        self.assertEqual(
            df_ordered.collect()[3]["id"],
            "3",
            "The id of the ad with second most views is not correct",
        )
        self.assertEqual(
            df_ordered.collect()[3]["version"],
            "1",
            "The version of the car with second most views is not correct",
        )
        self.assertEqual(
            df_ordered.collect()[3]["count"],
            1,
            "The number of views for the second ad is not correct",
        )

    def test_get_oldest_most_recent_time_between(self):
        data_views = [
            (
                ("ad_view_events.AdView", "1", 1643501252),
                ("1", "A"),
                ("10", "1.2.3.4"),
                ("1", "1"),
            ),
            (
                ("ad_view_events.AdView", "2", 1643501253),
                ("2", "AB"),
                ("10", "1.2.4.3"),
                ("1", "1"),
            ),
        ]
        schema_views = "event struct<ns:string, id:string, time:long>, user struct<id:string, cookie:string>, client struct<apiVersion:string, ip:string>, ad struct<id:string, version:string>"
        df_views = spark.createDataFrame(data_views, schema=schema_views)

        df_result = query.get_oldest_most_recent_time_between(df_views)
        self.assertEqual(
            df_result.collect()[0]["time_active_seconds"],
            1,
            "The difference between the most recent and oldest views is not correct",
        )


if __name__ == "__main__":
    test_runner = unittest.main(argv=[""], exit=False)
    test_runner.result.printErrors()

    if not test_runner.result.wasSuccessful():
        raise Exception(
            f"{len(test_runner.result.failures)} of {test_runner.result.testsRun} tests failed."
        )