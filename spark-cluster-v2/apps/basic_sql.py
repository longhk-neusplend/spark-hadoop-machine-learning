from __future__ import division
import pyspark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType
from pyspark.sql.functions import udf

sc = pyspark.SparkContext()
sqlContext = pyspark.SQLContext(sc)

#DT1 = sqlContext.createDataFrame(data=[(1,2), (3,4)], schema=("A", "B"))
lines = sc.textFile("/opt/spark-data/2023-07-23-r.csv", use_unicode=True).take(4)
dats = sc.textFile("/opt/spark-data/2023-07-23-r.csv", use_unicode=True) \
                    .map(lambda x:x.replace('"', "")) \
                    .map(lambda x:x.split(","))
#dats_rdd = sc.parallelize(dats)


DT2=sqlContext.createDataFrame(data=dats.filter(lambda x:x[0]!='date'),
                               schema=dats.filter(lambda x:x[0]=='date').
                               collect()[0])
DT2.persist()
#DT2.show(n=10)


DT3 = DT2.withColumn("date", DT2["date"].cast(DateType()))
DT3 = DT3.withColumn("time", DT3["time"].cast(TimestampType()))
#print(DT3.dtypes)

DT4 = DT3.withColumnRenamed("version","lastest version")
#DT4.show(5)
#DT4.sort(DT4.size.asc()).show(10)
#print(DT4.filter(DT4['size'] > 3000000).count() / DT4.count())
#DT4.show()
#print(DT4.groupBy("country").count().sort("count", ascending = False).show(40))

country_count = DT4.groupBy("country").count().sort("count", ascending = False)
#country_count.show(60)

derive_perc = udf(lambda x: str(round(x * 100,3)) + "%")

country_count = country_count.withColumn("perc", derive_perc(country_count['count'] / DT4.count()))
#country_count.show(10)
#country_count.filter(country_count.country == 'IN').show()

country_count.createOrReplaceTempView("country_count_sql_table")

# Basic Spark SQL Query - 1
query_result = sqlContext.sql("select perc \
                              from country_count_sql_table \
                              where country = 'IN'")

#print("result query: ",query_result.collect())

query_result = sqlContext.sql("select * \
                              from country_count_sql_table \
                              where count > 100 \
                              order by count desc")

#print("result query2: ",query_result.show(10))

# Use the Spark RDD way to process the results from Spark SQL query result
print(query_result.rdd.map(lambda x:x['country'] + ":" + x['perc']).take(10))


