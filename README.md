![PySpark Data Processing](https://github.com/nogibjj/IDS706-Mini-Project-10-sp699/actions/workflows/cicd.yml/badge.svg)
# IDS-706-Data-Engineering :computer:

## Mini Project 10 :page_facing_up: 

## :ballot_box_with_check: Requirements
* Use PySpark to perform data processing on a large dataset.</br>
* Include at least one Spark SQL query and one data transformation.</br>

## :ballot_box_with_check: To-do List
* __Data processing functionality__: Learn data processing functionality using PySpark.</br>
* __Use of Spark SQL and transformations__: Use Spark SQL and transform the dataset by adding columns or rows as required.

## :ballot_box_with_check: Dataset
`penguins.csv`
  <img src="https://github.com/nogibjj/Suim-Park-Mini-Project-2/assets/143478016/fe1c7646-539f-4bd5-ba5f-c67f47cbc4c9.png" width="600" height="400"/>
  - Data were collected and made available by __Dr. Kristen Gorman__ and the __Palmer Station__, Antarctica LTER, a member of the Long Term Ecological Research Network. It shows three different species of penguins observed in the Palmer Archipelago, Antarctica.
  - [penguins.csv](https://github.com/nogibjj/IDS706-Mini-Project-10-sp699/raw/main/penguins.csv)
* `Description of variables`</br>
  <img src="https://github.com/nogibjj/Suim-Park-Mini-Project-2/assets/143478016/6b0020de-5499-43ea-b6d6-a67f52aa8d58.png" width="350" height="450"/></br>
  - In this dataset, we can observe several important variables, among which the unfamiliar 'bill_length_mm,' 'bill_depth_mm,' and 'flipper_length_mm' can be understood through the following figures.

## :ballot_box_with_check: Main Progress
#### `Section 1` Use Spark SQL and transform
##### Data can be processed and functionality executed through Spark SQL or transformations.
* `lib.py`
1. __Spark SQL__: This query displays the maximum and minimum values of the bill length by species.
```Python
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
```
2. __Transform__: The transform function categorizes the bill length as follows: lengths below 40mm are classified as 'Short', those between 40mm and 50mm are deemed 'Medium', and lengths exceeding 50mm are categorized as 'Long'.

```Python
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
```

#### `Section 2` See the pipeline of CI/CD
##### Observe the CI/CD pipeline in action, which includes steps for installation, formatting, linting, and testing.
1. __`make format`__
<img src="https://github.com/nogibjj/IDS706-Mini-Project-10-sp699/assets/143478016/aca85b86-5f39-4896-bcac-5f93f85188a2.png" width="400" height="100"/>

2. __`make lint`__
<img src="https://github.com/nogibjj/IDS706-Mini-Project-10-sp699/assets/143478016/86517b6a-14f5-45ff-b353-6963b1526330.png" width="400" height="130"/>

3. __`make test`__
