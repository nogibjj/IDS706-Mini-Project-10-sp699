from library.lib import init_spark, read_csv, spark_sql_query, transform

if __name__ == "__main__":
    spark = init_spark(app_name="PySpark Data Processing")

    csv_file_path = "penguins.csv"
    df = read_csv(spark, csv_file_path)

    # Displaying the original data
    print("Original Data:")
    df.show()

    # Displaying data after using Spark SQL query
    print("Data After Spark SQL Query:")
    spark_sql_query(spark, df)

    # Displaying data after adding the bill_length_category
    df_with_category = transform(spark, df)
    print("Data After Adding Bill Length Category:")
    df_with_category.show()