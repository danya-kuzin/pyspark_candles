from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("task_2")
    .getOrCreate()
)

csv_path = "data/fin_sample.csv"

deals_data_frame = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(csv_path)
)

deals_data_frame = deals_data_frame.select(
    "SYSTEM",
    "#SYMBOL",
    "MOMENT",
    "ID_DEAL",
    "PRICE_DEAL",
)

candle_width = 300000

moment_text = func.col("MOMENT").cast("string")

trade_date = func.substring(moment_text, 1, 8).cast("int")
hour = func.substring(moment_text, 9, 2).cast("int")
minute = func.substring(moment_text, 11, 2).cast("int")
second = func.substring(moment_text, 13, 2).cast("int")
millisecond = func.substring(moment_text, 15, 3).cast("int")

time_ms = hour * 3600000 + minute * 60000 + second * 1000 + millisecond
candle_number = func.floor(func.col("TIME_MS") / candle_width)
candle_start_ms = candle_number * candle_width

deals_data_frame = deals_data_frame.withColumn("TRADE_DATE", trade_date)
deals_data_frame = deals_data_frame.withColumn("TIME_MS", time_ms)
deals_data_frame = deals_data_frame.withColumn("CANDLE_START_MS", candle_start_ms)



print("Кусочек таблицы:")
deals_data_frame.show(10)

print("Схема типов:")
deals_data_frame.printSchema()

print("Количество строк:", deals_data_frame.count())

print("Торговые системы:")
deals_data_frame.groupBy("SYSTEM").count().show()

print("Финансовые инструменты:")
deals_data_frame.groupBy("#SYMBOL").count().show()

candle_groups = deals_data_frame.groupBy(
    "#SYMBOL",
    "TRADE_DATE",
    "CANDLE_START_MS",
)


spark.stop()