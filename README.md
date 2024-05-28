# pyspark-coding-questions
Pyspark coding paractice questions


#### Mixed Delimiter problem

```
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

spark = SparkSession.builder.appName("MixedDelimiterExample").getOrCreate()

data = ["1,Alice\t30|New york"]

df = spark.createDataFrame(data, "string")

split_col = split(df["value"], ',|\t|\|')

df = df.withColumn('id', split_col.getItem(0))\
      .withColumn('name',split_col.getItem(1))\
      .withColumn('age',split_col.getItem(2))\
      .withColumn('city',split_col.getItem(3))


df.select('id', 'name', 'age', 'city').show()
```

#### Missling elements

```
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder.appName("Find Missing Numbers").getOrCreate()


data = [(1,), (2,),(3,), (4,), (6,), (8,),(10,)]
data1 = [1, 2, 3, 4, 6, 8, 10]

df_numbers = spark.createDataFrame(data, ["Number"])

df_numbers.show()

full_range = spark.range(1,11).toDF("Number")

print(full_range.show())

missing_numbers = full_range.join(df_numbers, "Number", "left_anti")

missing_numbers.show()

```
