from library.lib import init_spark, read_csv, spark_sql_query, transform
from pyspark.sql import SparkSession, Row


def test_init_spark():
    spark = init_spark(app_name="PySpark Data Processing")
    assert isinstance(spark, SparkSession), "Test failed."
    print("Spark initiation test passed successfully.")


def test_read_csv():
    spark = init_spark(app_name="PySpark Data Processing")
    csv_file_path = "penguins.csv"
    df = read_csv(spark, csv_file_path)
    print(df)
    assert df.count() > 0, "Test failed."
    print("CSV file reading test passed successfully.")


def test_spark_sql_query():
    # Create SparkSession for testing
    spark = SparkSession.builder.appName("Spark SQL Query Test").getOrCreate()
    csv_file_path = "penguins.csv"
    df = read_csv(spark, csv_file_path)
    result_df = spark_sql_query(spark, df)

    # Generate the expected result
    expected_data = [
        Row(species="Gentoo", max_bill_length=59.6, min_bill_length=40.9),
        Row(species="Adelie", max_bill_length=46.0, min_bill_length=32.1),
        Row(species="Chinstrap", max_bill_length=58.0, min_bill_length=40.9),
    ]
    expected_df = spark.createDataFrame(expected_data)

    # Compare the result DataFrame with the expected DataFrame
    assert result_df.collect() == expected_df.collect(), "Test failed."
    print("Spark SQL query test passed successfully.")


def test_transform():
    # Create SparkSession for testing
    spark = SparkSession.builder.appName("Add Bill Length Category Test").getOrCreate()

    # Sample data for testing
    sample_data = [
        Row(bill_length_mm=35.0),
        Row(bill_length_mm=45.0),
        Row(bill_length_mm=55.0),
    ]
    df = spark.createDataFrame(sample_data)

    # Call the add_bill_length_category function
    result_df = transform(spark, df)

    # Verify the values in the bill_length_category column of the result DataFrame
    categories = [row["bill_length_category"] for row in result_df.collect()]
    expected_categories = ["Short", "Medium", "Long"]

    assert categories == expected_categories, "Test failed!"

    print("Transform test passed successfully.")

    # Stop the SparkSession after the test
    spark.stop()


if __name__ == "__main__":
    test_init_spark()
    test_read_csv()
    test_spark_sql_query()
    test_transform()
