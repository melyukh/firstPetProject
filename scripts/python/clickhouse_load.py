import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col as column, regexp_replace
import clickhouse_connect

DATA_DIR = os.getenv("DATA_DIR")

client = clickhouse_connect.get_client(
    host=os.getenv("CH_HOST", "clickhouse"),      # ← localhost → clickhouse
    port=int(os.getenv("CH_PORT", 8123)),
    username=os.getenv("CH_USER", "airflow"),
    password=os.getenv("CH_PASSWORD", "airflow")
)

spark = SparkSession.builder.appName("clickhouseLoadApp").getOrCreate()  # ← добавить

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

df = spark.read.csv(
    f"{DATA_DIR}/russian_houses_utf_8.csv",
    header=True, schema=schema, encoding='utf-8'
)

df = df.withColumn(
    "communal_service_id",
    column("communal_service_id").cast("integer")
).withColumn(
    "square",
    regexp_replace(column("square"), r'[^0-9\.]', '').cast("double")   # ← cast
).withColumn(
    "population",
    regexp_replace(column("population"), r'[^0-9\.]', '').cast("integer")  # ← cast
)

df_base = df.dropna(how='all').where(
    (column("maintenance_year") <= 2026) &
    (column("square") > 0) &
    (column("population") > 0)
)

# Spark DataFrame → pandas → clickhouse
pandas_df = df_base.dropna(subset=["house_id"]).toPandas()  # ← конвертация
client.insert_df(table='airflow.russian_houses', df=pandas_df)
print(f"Загружено строк: {len(pandas_df)}")

spark.stop()