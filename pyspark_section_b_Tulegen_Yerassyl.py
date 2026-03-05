from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# Taskk 1
spark = SparkSession.builder.appName('SectionB_Sprint').getOrCreate()

# Taskk 2
books_df = spark.read.csv('books.csv', header=True, inferSchema=True)

# Taskk 3
categories_df = spark.read.csv('categories.csv', header=True, inferSchema=True)

# Taskk 4
books_df.printSchema()

# Taskk 5
categories_df.printSchema()

# Taskk 6
print(books_df.count())

# Taskk 7
print(categories_df.count())

# Taskk 8
books_df.show(10)

# Taskk 9
print(books_df.tail(2))

# Taskk 10
books_df.select('author_name').show()

# Taskk 11
books_df.select('title', 'price', 'rating').show()

# Taskk 12
books_df = books_df.withColumnRenamed('price', 'Cost')

# Taskk 13
books_df = books_df.withColumnRenamed('book_id', 'Product_ID')

# Taskk 14
books_df = books_df.withColumn('Store_Location', F.lit('Almaty'))

# Taskk 15
books_df = books_df.withColumn('Price_VAT', F.col('Cost') * 1.12)

# Taskk 16
books_df = books_df.withColumn('Price_Discount', F.col('Cost') * 0.9)

# Taskk 17
no_author_df = books_df.drop('author_name')

# Taskk 18
print(books_df.schema['rating'].dataType)

# Taskk 19
books_df = books_df.withColumn('rating', F.col('rating').cast('float'))

# Taskk 20
books_df.select('author_name').distinct().show()

# Taskk 21
print(books_df.select('category_id').distinct().count())

# Taskk 22
books_df.select('category_id').distinct().show()

# Taskk 23
books_df.filter(F.col('Cost') == 45.5).show()

# Taskk 24
books_df.filter(F.col('author_name') == 'Tolstoy').show()

# Taskk 25
null_ratings = books_df.filter(F.col('rating').isNull())

# Taskk 26
print(null_ratings.count())

# Taskk 27
books_df.filter(F.col('rating').isNotNull()).show()

# Taskk 28
books_df = books_df.na.fill(3.5, ['rating'])

# Taskk 29
books_df = books_df.na.drop(subset=['title'])

# Taskk 30
books_df.createOrReplaceTempView('books_sql')

# Taskk 31
books_df.filter(F.col('Cost') > 60).show()

# Taskk 32
books_df.filter(F.col('Cost') < 25).show()

# Taskk 33
books_df.filter(F.col('rating').between(3.0, 4.0)).show()

# Taskk 34
books_df.filter((F.col('author_name') == 'Dostoevsky') & (F.col('rating') < 4.0)).show()

# Taskk 35
books_df.filter(F.col('Cost').between(50, 100)).show()

# Taskk 36
books_df.filter(F.col('author_name').isin(['Iqbal', 'Rumi'])).show()

# Taskk 37
books_df.filter(F.col('title').startswith('The')).show()

# Taskk 38
books_df.filter(F.col('title').contains('Volume')).show()

# Taskk 39
books_df.filter(F.col('author_name').endswith('v')).show()

# Taskk 40
books_df.sort(F.col('rating').asc()).show()
# Taskk 41
books_df.sort(F.col('rating').desc()).show()

# Taskk 42
books_df.sort(F.col('category_id').asc(), F.col('Cost').desc()).show()

# Taskk 43
books_df.sort(F.col('rating').desc()).limit(15).show()

# Taskk 44
books_df.sort(F.col('Cost').asc()).limit(10).show()

# Taskk 45
books_df.filter(F.length(F.col('title')) == 12).show()

# Taskk 46
books_df = books_df.withColumn('Title_Upper', F.upper(F.col('title')))

# Taskk 47
books_df = books_df.withColumn('Author_Lower', F.lower(F.col('author_name')))

# Taskk 48
books_df.select(F.substring(F.col('title'), -4, 4)).show()

# Taskk 49
books_df = books_df.withColumn('Book_Label', F.concat_ws(' - ', F.col('title'), F.col('author_name')))

# Taskk 50
books_df = books_df.withColumn('author_name', F.regexp_replace(F.col('author_name'), 'a', '@'))

# Taskk 51
books_df.filter(F.col('Cost') != 15.99).show()

# Taskk 52
books_df.filter(F.col('rating') == 4.8).select('title').show()

# Taskk 53
books_df = books_df.withColumn('Is_Cheap', F.when(F.col('Cost') < 20, True).otherwise(False))

# Taskk 54
print(books_df.filter(F.col('Is_Cheap') == True).count())

# Taskk 55
books_df.filter(F.col('author_name').isin(['Asimov', 'Art'])).show()

# Taskk 56
books_df.filter(F.col('author_name') != 'Kafka').show()

# Taskk 57
books_df.filter(F.col('Cost').cast('string').endswith('.00')).show()

# Taskk 58
books_df.select('author_name').distinct().show()

# Taskk 59
books_df.limit(25).show()

# Taskk 60
books_df.filter(F.col('Cost') > 50).sort('rating').explain()

# Taskk 61
inner_join = books_df.join(categories_df, 'category_id', 'inner')

# Taskk 62
left_join = books_df.join(categories_df, 'category_id', 'left')

# Taskk 63
right_join = books_df.join(categories_df, 'category_id', 'right')

# Taskk 64
full_join = books_df.join(categories_df, 'category_id', 'outer')

# Taskk 65
left_anti = books_df.join(categories_df, 'category_id', 'left_anti')

# Taskk 66
left_semi = books_df.join(categories_df, 'category_id', 'left_semi')

# Taskk 67
inner_join.select('title', 'category_name').show()

# Taskk 68
inner_join.filter(F.col('category_name') == 'History').show()

# Taskk 69
print(inner_join.filter(F.col('category_name') == 'Science').count())

# Taskk 70
inner_join.filter(F.col('category_name') == 'Poetry').select(F.avg('Cost')).show()

# Taskk 71
inner_join.filter(F.col('category_name') == 'Technology').select(F.max('rating')).show()

# Taskk 72
inner_join.filter(F.col('category_name') == 'Fiction').select(F.min('Cost')).show()

# Taskk 73
inner_join.sort('category_name').show()

# Taskk 74
inner_join.filter(F.col('Cost') > 70).show()

# Taskk 75
inner_join = inner_join.withColumnRenamed('category_name', 'Department')

# Taskk 76
print(f"Inner: {inner_join.count()}, Left: {left_join.count()}")

# Taskk 77
print(right_join.count())

# Taskk 78
books_df.select('category_id').subtract(categories_df.select('category_id')).show()

# Taskk 79
inner_join.filter((F.col('author_name') == 'Tolstoy') & (F.col('Department') == 'Philosophy')).show()

# Taskk 80
# Correct way: Join first, then drop the duplicate category_id column
books_df.join(categories_df, "category_id", "inner").drop(categories_df.category_id).show()
# Taskk 81
inner_join = inner_join.withColumn('Adjusted_Rating', F.col('rating') + 0.2)

# Taskk 82
art_sum = inner_join.filter(F.col('Department') == 'Art').select(F.sum('Cost')).collect()[0][0]
print(f"Task 82: Art Total Cost: {art_sum}")

# Taskk 83
inner_join.filter((F.col('author_name') == 'Ghalib') & (F.col('Department') == 'History')).show()

# Taskk 84
inner_join.filter(F.col('Department') == 'Biography').sort(F.col('Cost').desc()).limit(10).show()

# Taskk 85
left_join.filter(F.col('category_name').isNull()).show()

# Taskk 86
left_join = left_join.na.fill('Miscellaneous', ['category_name'])

# Taskk 87
inner_join.groupBy('Department').count().show()

# Taskk 88
left_join.write.mode("overwrite").csv('left_join_result.csv', header=True)

# Taskk 89
inner_join.write.mode("overwrite").parquet('inner_join_result.parquet')

# Taskk 90
print(f"Task 90 Parquet count: {spark.read.parquet('inner_join_result.parquet').count()}")

# Taskk 91
books_df.withColumnRenamed('author_name', 'Writer').groupBy('Writer').count().show()

# Taskk 92
books_df.groupBy('category_id').avg('Cost').show()

# Taskk 93
books_df.withColumnRenamed('author_name', 'Writer').groupBy('Writer').min('rating').show()

# Taskk 94
books_df.groupBy('category_id').sum('Cost').show()

# Taskk 95
print(f"Task 95 Total Cost: {books_df.select(F.sum('Cost')).collect()[0][0]}")

# Taskk 96
print(f"Task 96 Avg Rating: {books_df.select(F.avg('rating')).collect()[0][0]}")

# Taskk 97
print(f"Task 97 Min Rating: {books_df.select(F.min('rating')).collect()[0][0]}")

# Taskk 98
print(f"Task 98 Max Cost: {books_df.select(F.max('Cost')).collect()[0][0]}")

# Taskk 99
books_df.groupBy('author_name', 'category_id').count().show()

# Taskk 100
books_df.groupBy('author_name', 'category_id').count().show()

# Taskk 101
books_df.select(F.min('rating'), F.max('rating')).show()

# Taskk 102
books_df.select(F.sum('Cost'), F.avg('Cost'), F.count('Cost')).show()

# Taskk 103
books_df.groupBy('author_name').count().filter(F.col('count') == 3).show()

# Taskk 104
books_df.groupBy('category_id').max('Cost').filter(F.col('max(Cost)') < 50).show()

# Taskk 105
books_df.groupBy('category_id').pivot('author_name').count().show()

# Taskk 106
books_df.groupBy('author_name').avg('rating').sort('avg(rating)').limit(1).show()

# Taskk 107
books_df.groupBy('category_id').count().sort(F.col('count').desc()).limit(1).show()

# Taskk 108
books_df.groupBy('author_name').sum('Cost').show()

# Taskk 109
books_df.groupBy('author_name').sum('Cost').sort('sum(Cost)').show()

# Taskk 110
books_df.filter(F.col('rating') > 3.5).groupBy('author_name').avg('Cost').show()

# Taskk 111
books_df.groupBy('category_id').agg(F.variance('rating')).show()

# Taskk 112
k_fiction = inner_join.filter((F.col('author_name') == 'Kafka') & (F.col('Department') == 'Fiction')).count()
print(f"Task 112 Kafka Fiction count: {k_fiction}")

# Taskk 113
print(f"Task 113 25th Percentile: {books_df.approxQuantile('Cost', [0.25], 0.05)}")

# Taskk 114
books_df.groupBy('rating').count().show()

# Taskk 115
books_df.groupBy('author_name').agg(F.countDistinct('category_id').alias('cat_count')).filter(F.col('cat_count') == 2).show()

# Taskk 116
books_df.groupBy('author_name').agg(F.sum('Cost').alias('Author_Revenue')).show()

# Taskk 117
orwell_pct = (books_df.filter(F.col('author_name') == 'Orwell').count() / books_df.count()) * 100
print(f"Task 117 Orwell Pct: {orwell_pct}%")

# Taskk 118
books_df.sort(F.col('Cost').desc()).select('author_name').limit(1).show()

# Taskk 119
books_df.groupBy('category_id').agg((F.max('Cost') - F.min('Cost')).alias('range')).sort('range').limit(1).show()

# Taskk 120
books_df.groupBy('author_name').sum('Cost').filter(F.col('sum(Cost)') > 1000).show()

# Taskk 121
books_df.withColumn('Rounded_Cost', F.round(F.col('Cost')/5)*5).groupBy('Rounded_Cost').count().show()

# Taskk 122
med_price = books_df.approxQuantile('Cost', [0.5], 0.05)[0]
books_df.filter(F.col('Cost') < med_price).select(F.avg('rating')).show()

# Taskk 123
rumi_list = books_df.filter(F.col('author_name') == 'Rumi').select('title').collect()

# Taskk 124
dost_set = books_df.filter(F.col('author_name') == 'Dostoevsky').select('category_id').distinct().collect()

# Taskk 125
books_df.groupBy('category_id').agg(F.countDistinct('author_name')).show()
# Taskk 126
from pyspark.sql.window import Window

# Taskk 127
win_spec = Window.partitionBy('category_id').orderBy('Cost')
books_df.withColumn('rank', F.rank().over(win_spec)).show()

# Taskk 128
books_df.withColumn('rank', F.rank().over(win_spec)).filter(F.col('rank') == 1).show()

# Taskk 129
books_df.withColumn('Med_Diff', F.col('Cost') - F.percentile_approx('Cost', 0.5).over(Window.partitionBy('category_id'))).show()

# Taskk 130
auth_win = Window.partitionBy('author_name').orderBy('Product_ID')
books_df.withColumn('row_num', F.row_number().over(auth_win)).show()

# Taskk 131
rate_win = Window.partitionBy('author_name').orderBy(F.col('rating').desc())
books_df.withColumn('rank', F.rank().over(rate_win)).filter(F.col('rank') == 3).show()

# Taskk 132
run_win = Window.partitionBy('category_id').orderBy('Cost').rowsBetween(Window.unboundedPreceding, Window.currentRow)
books_df.withColumn('running_total', F.sum('Cost').over(run_win)).show()

# Taskk 133
books_df.withColumn('prev_price', F.lag('Cost', 1).over(win_spec)).show()

# Taskk 134
books_df = books_df.repartition(4)

# Taskk 135
books_df = books_df.coalesce(2)

# Taskk 136
print(f"Task 136 Partitions: {books_df.rdd.getNumPartitions()}")

# Taskk 137
books_df.filter("Cost BETWEEN 20 AND 50").show()

# Taskk 138
books_df = books_df.withColumn('Total_Sales', F.col('Cost') * 5)

# Taskk 139
books_df = books_df.withColumn('Cost', F.round(F.col('Cost'), 1))

# Taskk 140
books_df.groupBy('category_id').agg((F.max('Cost')-F.min('Cost')).alias('Spread')).show()

# Taskk 141
books_df = books_df.withColumn('Budget', F.when(F.col('Cost') < 20, 'Budget').when(F.col('Cost') < 50, 'Standard').otherwise('Premium'))

# Taskk 142
books_df = books_df.withColumn('Review', F.when(F.col('rating') < 3.5, 'Poor').when(F.col('rating') < 4.5, 'Good').otherwise('Excellent'))

# Taskk 143
exc_tolstoy = books_df.filter((F.col('Review') == 'Excellent') & (F.col('author_name') == 'Tolstoy')).count()
print(f"Task 143 Excellent Tolstoy books: {exc_tolstoy}")

# Taskk 144
books_df.groupBy('Budget').avg('rating').show()

# Taskk 145
books_df.persist()

# Taskk 146
books_df.unpersist()

# Taskk 147
books_df.write.mode("overwrite").json('final_analytics.json')

# Taskk 148
books_df.groupBy('category_id').count().write.mode("overwrite").option("sep", "|").csv('category_summary.csv')

# Taskk 149
spark.catalog.dropTempView('books_sql')

# Taskk 150
spark.stop()