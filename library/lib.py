from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

def init_spark(app_name: str, memory: str = "2g") -> SparkSession:
    # Initializing Spark session with given app name and memory
    session = SparkSession.builder.appName(app_name) \
        .config("session.executor.memory", memory) \
        .getOrCreate()
    return session

def read_csv(session: SparkSession, file_path: str) -> DataFrame:
    # Reading CSV file with header and inferring schema
    data_file = session.read.csv(file_path, header=True, inferSchema=True)
    return data_file

def spark_sql_query(spark: SparkSession, data: DataFrame):
    # Creating a temporary view for querying
    data.createOrReplaceTempView("penguins")
    
    # Executing query using Spark SQL
    result = spark.sql("""
        SELECT species, 
               MAX(bill_length_mm) as max_bill_length, 
               MIN(bill_length_mm) as min_bill_length
        FROM penguins
        GROUP BY species
    """)
    result.show()
    return result

def transform(spark: SparkSession, data: DataFrame) -> DataFrame:
    # Adding 'bill_length_category' column based on 'bill_length_mm' column values
    conditions = [
        (F.col("bill_length_mm") < 40, "Short"),
        ((F.col("bill_length_mm") >= 40) & (F.col("bill_length_mm") < 50), "Medium"),
        (F.col("bill_length_mm") >= 50, "Long")
    ]

    return data.withColumn("bill_length_category", F.when(conditions[0][0], conditions[0][1])
                                                    .when(conditions[1][0], conditions[1][1])
                                                    .otherwise(conditions[2][1]))
