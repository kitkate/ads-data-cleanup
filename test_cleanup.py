import unittest
import cleanup


class TestCleanup(unittest.TestCase):

    def test_cleanup_colors(self):
        data = [(("____BLACK", "48573"), 1, "VW", "Arteon", (("1", "2"), "BGN"), "1")]
        schema = "attributes struct<exteriorColor:string, mileage:string>, id string, make string, model string, price struct<consumerValue:struct<gross:string, net:string>, currency:string>, version string" 
        df = spark.createDataFrame(data, schema=schema)
        
        df_cleaned = cleanup.cleanup_colors(df)
        self.assertEqual(df_cleaned.collect()[0][0][0], "BLACK", "Ads color not cleaned correctly")


if __name__ == "__main__":
    test_runner = unittest.main(argv=[''], exit=False)
    test_runner.result.printErrors()

    if not test_runner.result.wasSuccessful():
        raise Exception(
            f"{len(test_runner.result.failures)} of {test_runner.result.testsRun} tests failed.")
