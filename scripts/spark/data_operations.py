#все необходимые расчеты (4-7)
import os
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, count, max, min, percentile_approx as persentile, avg as average, floor, regexp_replace

DATA_DIR = os.getenv("DATA_DIR")

spark = SparkSession.builder.appName("dataOperationApp").getOrCreate()
#чтение, очистка от null, каст типов в нужных столбцах
schema = StructType([
    StructField("house_id", IntegerType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("maintenance_year", IntegerType(), True),
    StructField("square", StringType(), True),
    StructField("population", StringType(), True),
    StructField("region", StringType(), True),
    StructField("locality_name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("full_address", StringType(), True),
    StructField("communal_service_id", DoubleType(), True),
    StructField("description", StringType(), True)
])
df = spark.read.csv(f"{DATA_DIR}/russian_houses_utf_8.csv", header=True, schema=schema,  encoding='utf-8')

#очищенные и кеширование данные для общих расчетов
df =   df.withColumn(
                    "communal_service_id", 
                    col("communal_service_id").cast("integer")
        ).withColumn(
                    "square",
                    regexp_replace(col("square"), r'[^0-9\.]', '').cast("double")
        ).withColumn(
                    "population",
                    regexp_replace(col("population"), r'[^0-9\.]', '').cast("integer"))
df_base = df.dropna(how='all').where((col("maintenance_year") <= 2026) & (col("square") > 0) & (col("population") > 0))

df_base.persist(StorageLevel.MEMORY_AND_DISK).count()

#кешированные данные для регионов
df_without_null_region = df_base.dropna(subset = ["region"])
df_without_null_region.persist(StorageLevel.MEMORY_AND_DISK).count()

#медиана и среднее для года постройки
print("медиана и среднее для года постройки:")
df_base.agg(average("maintenance_year").cast("integer").alias("average_year"), persentile("maintenance_year", 0.5).cast("integer").alias("median_year")).show()

#топ-10 регионов по количеству объектов
print("топ-10 регионов по количеству объектов:")
df_without_null_region.groupBy("region").agg(count("house_id").alias("houses_count")).orderBy(col("houses_count").desc()).show(10)

#топ-10 городов по количеству объектов
print("\nтоп-10 городов по количеству объектов:")
df_base.dropna(subset=["locality_name"]).groupBy("locality_name").agg(count("house_id").alias("houses_count")).orderBy(col("houses_count").desc()).show(10)

#min и max площадь по городу
print("\nmin и max площадь по городу:")
result = (
  df_without_null_region
  .groupBy("region")
  .agg(
        max("square").alias("max_square"),
        min("square").alias("min_square")
      )
  )
result.orderBy(col("max_square").desc()).show(15) # чтобы можно было увидеть max/min алиасы в сортировке

#количество зданий по десятилетиям
print("\nколичество зданий по десятилетиям:")
(
  df_base.dropna(subset=["maintenance_year"])
    .withColumn("decade", floor(col("maintenance_year") / 10) * 10)
    .where(col("decade") != 0)
    .groupBy("decade")
    .agg(count("house_id").alias("num_of_houses"))
    .orderBy(count("house_id").desc())
    .show(20)
)

df_base.unpersist()
df_without_null_region.unpersist()
spark.stop()