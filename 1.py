from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

#init
spark = SparkSession.builder \
    .appName("LabPractice") \
    .getOrCreate()

#reading
courses_df = spark.read.csv(
    "university_courses.csv",
    header=True,
    inferSchema=True
)

books_df = spark.read \
    .option("multiline", "true") \
    .json("zeba_books_inventory.json")

#printing

courses_df.printSchema()
books_df.printSchema()


#showing 1st 5 rows

courses_df.show(5)
books_df.show(5)

#counting

courses_df.count()
books_df.count()

#Select

courses_df.select("course_name").show()
books_df.select("title", "price").show()


#unique
courses_df.select("department").distinct().show()
books_df.select("format").distinct().show()

#filter
courses_df.filter(col("department") == "IT").show()
books_df.filter(col("format") == "Paperback").show()

courses_df.filter(col("enrollment") > 50).show()
books_df.filter(col("price") < 20).show()

books_df.filter(col("title").contains("Rust")).show()
courses_df.filter(col("credits") == 4).show()

courses_df.filter(col("department") != "History").show()

books_df.filter(
    (col("format") == "Paperback") |
    (col("format") == "Hardcover")
).show()

courses_df.filter(col("enrollment").between(30,60)).show()
books_df.filter(col("price") > 25).show()

courses_df.filter(col("course_id").startswith("C")).show()
courses_df.where(col("department") == "English").show()

#selectExpr
courses_df.selectExpr(
    "course_name",
    "enrollment * 2 as double_enrollment"
).show()

#rename
courses_df = courses_df.withColumnRenamed(
    "enrollment",
    "student_count"
)

books_df = books_df.withColumnRenamed(
    "price",
    "unit_price"
)

#add is_expensive
books_df = books_df.withColumn(
    "is_expensive",
    col("unit_price") > 20
)

#total
courses_df = courses_df.withColumn(
    "total_credits_sum",
    col("student_count") * col("credits")
)

#drop
courses_df.drop("credits").show()

#sort
# enrollment ASC
courses_df.sort("student_count").show()
#DESC
courses_df.sort(col("student_count").desc()).show()

books_df.sort(col("unit_price").desc()).show()

courses_df.sort(
    col("department").asc(),
    col("student_count").desc()
).show()

#top3
books_df.orderBy(col("unit_price").desc()).show(3)

#bottom2
courses_df.orderBy(col("student_count")).show(2)

courses_df.groupBy("department").count().show()
books_df.groupBy("format").count().show()

#total
courses_df.agg(sum("student_count")).show()

#avg
courses_df.groupBy("department") \
    .avg("student_count") \
    .show()

books_df.agg(avg("unit_price")).show()

#max
books_df.agg(max("unit_price")).show()

#min
courses_df.agg(min("credits")).show()


courses_df.filter(col("department") == "IT") \
    .agg(sum("student_count")) \
    .show()

books_df.select(sum("stock.quantity")).show()

#avg + sum
courses_df.agg(
    sum("student_count"),
    avg("student_count")
).show()

#highest avg

courses_df.groupBy("department") \
    .avg("student_count") \
    .orderBy(col("avg(student_count)").desc()) \
    .show(1)


#select
books_df.select("stock.warehouse").show()
books_df.select("stock.quantity").show()

#flatten JSON
books_flat = books_df.select(
    "*",
    col("stock.warehouse").alias("warehouse"),
    col("stock.quantity").alias("quantity")
).drop("stock")

books_flat.filter(col("warehouse") == "Almaty").show()

books_flat.filter(col("quantity") < 50).show()

books_flat.select("title","warehouse").show()

books_flat.groupBy("warehouse") \
    .agg(sum("quantity")) \
    .show()

books_flat.filter(col("format") == "Digital").show()

courses_df.createOrReplaceTempView("courses_view")

spark.stop()

