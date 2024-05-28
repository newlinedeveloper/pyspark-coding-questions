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

#### Top 3 Movies by Ratings

```
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = SparkSession.builder.appName("Top Movies").getOrCreate()

data_movies = [(1, "Movie A"), (2, "Movie B"), (3, "Movie C"), (4, "Movie D"), (5, "Movie E")]

data_ratings = [(1, 101, 4.5), (1, 102, 4.0), (2, 103, 5.0), 
                (2, 104, 3.5), (3, 105, 4.0), (3, 106, 4.0), 
                (4, 107, 3.0), (5, 108, 2.5), (5, 109, 3.0)]


columns_movies = ["MovieID", "MovieName"]
columns_ratings = ["MovieID", "UserID", "Rating"]

df_movies = spark.createDataFrame(data_movies, columns_movies)
df_ratings = spark.createDataFrame(data_ratings, columns_ratings)


avg_ratings = df_ratings.groupBy("MovieID").agg(avg("Rating").alias("AvgRating"))

avg_ratings.show()

top_movies = avg_ratings.join(df_movies, "MovieID").orderBy("AvgRating", ascending=False).limit(3)

top_movies.show()

```


#### Rolling Average calculation

```
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg, to_date


spark = SparkSession.builder.appName("RollingAverageCalculation").getOrCreate()

data = [Row(Date='2023-01-01', ProductID=100, QuantitySold=10),
        Row(Date='2023-01-02', ProductID=100, QuantitySold=15),
        Row(Date='2023-01-03', ProductID=100, QuantitySold=20),
        Row(Date='2023-01-04', ProductID=100, QuantitySold=25),
        Row(Date='2023-01-05', ProductID=100, QuantitySold=30),
        Row(Date='2023-01-06', ProductID=100, QuantitySold=35),
        Row(Date='2023-01-07', ProductID=100, QuantitySold=40),
        Row(Date='2023-01-08', ProductID=100, QuantitySold=45)]

df_sales = spark.createDataFrame(data)

df_sales = df_sales.withColumn("Date", to_date(col("Date")))

windowSpec = Window.partitionBy("ProductID").orderBy("Date").rowsBetween(-6,0)

rollingAvg = df_sales.withColumn('7DayAvg', avg("QuantitySold").over(windowSpec))

rollingAvg.show()

```



### UDF - Age Categorization

```
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


spark = SparkSession.builder.appName("AgeCategorization").getOrCreate()


data = [Row(UserID=4001, Age=17),
        Row(UserID=4002, Age=45),
        Row(UserID=4003, Age=65),
        Row(UserID=4004, Age=30),
        Row(UserID=4005, Age=80)]


df = spark.createDataFrame(data)

def categorize_age(age):
  if age < 18:
    return 'Youth'
  elif age < 60:
    return 'Adult'
  else:
    return 'Senior'


age_udf = udf(categorize_age, StringType())

df = df.withColumn('AgeGroup', age_udf(df['Age']))

df.show()

```


#### Unique Website Visitors per day

```
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct


spark = SparkSession.builder.appName("uniqueVisitorsPerDay").getOrCreate()

visitor_data = [Row(Date='2023-01-01', VisitorID=101),
                Row(Date='2023-01-01', VisitorID=102),
                Row(Date='2023-01-01', VisitorID=101),
                Row(Date='2023-01-02', VisitorID=103),
                Row(Date='2023-01-02', VisitorID=101)]


df_visitors = spark.createDataFrame(visitor_data)

unique_visitors = df_visitors.groupBy("Date").agg(countDistinct('VisitorID').alias('UniqueVisitors'))

unique_visitors.show()


```

#### Determine the first purchase date for each user.

```
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import min

# Initialize Spark session
spark = SparkSession.builder.appName("FirstPurchaseDate").getOrCreate()

# Sample data
purchase_data = [
    Row(UserID=1, PurchaseDate='2023-01-05'),
    Row(UserID=1, PurchaseDate='2023-01-10'),
    Row(UserID=2, PurchaseDate='2023-01-03'),
    Row(UserID=3, PurchaseDate='2023-01-12')
]

# Create DataFrame
df_purchases = spark.createDataFrame(purchase_data)

df_purchases = df_purchases.withColumn("PurchaseDate", col("PurchaseDate").cast("date"))

first_purchase = df_purchases.groupBy("UserID").agg(min("PurchaseDate").alias("FirstPurchaseDate"))

first_purchase.show()

```

#### Generate a sequential number for each row within each group, ordered by date.

```
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("RowNumberPerGroup").getOrCreate()

# Sample data
group_data = [
    Row(GroupID='A', Date='2023-01-01'),
    Row(GroupID='A', Date='2023-01-02'),
    Row(GroupID='B', Date='2023-01-01'),
    Row(GroupID='B', Date='2023-01-03')
]

# Create DataFrame
df_group = spark.createDataFrame(group_data)

df_group = df_group.withColumn("Date", col("Date").cast("date"))

window_spec = Window.partitionBy("GroupID").orderBy("Date")

df_group = df_group.withColumn("seqNum", row_number().over(window_spec))

df_group.show()

```
