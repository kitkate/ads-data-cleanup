import unittest
import order

class TestOrder(unittest.TestCase):

    def test_top_3_cars_by_price_desc_per_make_model(self):
        data = [(("____BLACK", "48573"), 1, "VW", "Arteon", (("2.00", "2"), "BGN"), "1"),
                (("____WHITE", "48573"), 2, "VW", "Arteon", (("100.00", "2"), "BGN"), "2"),
                (("____BLUE", "48573"), 3, "VW", "Arteon", (("3.00", "2"), "BGN"), "3")]
        schema = "attributes struct<exteriorColor:string, mileage:string>, id string, make string, model string, price struct<consumerValue:struct<gross:string, net:string>, currency:string>, version string" 
        df = spark.createDataFrame(data, schema=schema)
        
        
        df_ordered = order.top_3_cars_by_price_desc_per_make_model(df)
        self.assertEqual(df_ordered.collect()[0]["price"], 100.0, "Cars not ordered correctly by price")
        self.assertEqual(df_ordered.collect()[1]["price"], 3.0, "Cars not ordered correctly by price")
        self.assertEqual(df_ordered.collect()[2]["price"], 2.0, "ACars not ordered correctly by price")


if __name__ == "__main__":
    test_runner = unittest.main(argv=[''], exit=False)
    test_runner.result.printErrors()

    if not test_runner.result.wasSuccessful():
        raise Exception(
            f"{len(test_runner.result.failures)} of {test_runner.result.testsRun} tests failed.")
