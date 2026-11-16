from pyspark.sql import SparkSession
from pyspark.sql import functions as func
import argparse
import xml.etree.ElementTree as ET
from pathlib import Path
import shutil

DEFAULT_CONFIG = {
    "candle.width": 300000,
    "candle.date.from": 19000101,
    "candle.date.to": 20200101,
    "candle.time.from": 1000,
    "candle.time.to": 1800,
}


def read_config(config_path):
    config = DEFAULT_CONFIG.copy()

    if config_path is None:
        return config

    xml_root = ET.parse(config_path).getroot()

    for property_element in xml_root.findall("property"):
        name = property_element.findtext("name")
        value = property_element.findtext("value")

        if name in config and value is not None:
            config[name] = int(value)

    return config

parser = argparse.ArgumentParser()

parser.add_argument(
    "config",
    nargs="?",
    default=None,
)

parser.add_argument(
    "--input",
    default="data/input.csv",
)

parser.add_argument(
    "--output",
    default="output",
)

arguments = parser.parse_args()

config = read_config(arguments.config)

csv_path = arguments.input
candle_width = config["candle.width"]
date_from = config["candle.date.from"]
date_to = config["candle.date.to"]
time_from = config["candle.time.from"]
time_to = config["candle.time.to"]


print("Настройки программы:")
print(config)


def hhmm_to_ms(time_hhmm):
    if time_hhmm == 2400:
        return 86_400_000

    hours = time_hhmm // 100
    minutes = time_hhmm % 100

    if not (0 <= hours <= 23 and 0 <= minutes <= 59):
        raise ValueError(f"Некорректное время в config.xml: {time_hhmm}")

    return hours * 3_600_000 + minutes * 60_000


spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("task_2")
    .getOrCreate()
)

deals_data_frame = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(csv_path)
)

deals_data_frame = deals_data_frame.select(
    "#SYMBOL",
    "MOMENT",
    "ID_DEAL",
    "PRICE_DEAL",
)

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

time_from_ms = hhmm_to_ms(time_from)
time_to_ms = hhmm_to_ms(time_to)

deals_data_frame = deals_data_frame.filter(
    (func.col("TRADE_DATE") >= date_from)
    & (func.col("TRADE_DATE") < date_to)
    & (func.col("TIME_MS") >= time_from_ms)
    & (func.col("TIME_MS") < time_to_ms)
)

candle_start = func.col("CANDLE_START_MS")

candle_hour = func.floor(candle_start / 3_600_000).cast("int")
candle_minute = func.floor((candle_start % 3_600_000) / 60_000).cast("int")
candle_second = func.floor((candle_start % 60_000) / 1_000).cast("int")
candle_millisecond = (candle_start % 1_000).cast("int")

candle_moment = func.concat(
    func.col("TRADE_DATE").cast("string"),
    func.lpad(candle_hour.cast("string"), 2, "0"),
    func.lpad(candle_minute.cast("string"), 2, "0"),
    func.lpad(candle_second.cast("string"), 2, "0"),
    func.lpad(candle_millisecond.cast("string"), 3, "0"),
).cast("long")

deals_data_frame = deals_data_frame.withColumn(
    "CANDLE_MOMENT",
    candle_moment,
)

print("Кусочек таблицы:")
deals_data_frame.show(10)

print("Схема типов:")
deals_data_frame.printSchema()

print("Количество строк:", deals_data_frame.count())

print("Финансовые инструменты:")
deals_data_frame.groupBy("#SYMBOL").count().show()

candle_groups = deals_data_frame.groupBy(
    "#SYMBOL",
    "CANDLE_MOMENT",
)

deal_order = func.struct(
    func.col("MOMENT"),
    func.col("ID_DEAL"),
)

candles_data_frame = candle_groups.agg(
    func.min_by("PRICE_DEAL", deal_order).alias("OPEN"),
    func.max("PRICE_DEAL").alias("HIGH"),
    func.min("PRICE_DEAL").alias("LOW"),
    func.max_by("PRICE_DEAL", deal_order).alias("CLOSE"),
)

print("Рассчитанные свечи:")
candles_data_frame.show(20, truncate=False)

candles_data_frame = candles_data_frame.select(
    func.col("#SYMBOL").alias("SYMBOL"),
    func.col("CANDLE_MOMENT").alias("MOMENT"),
    func.round("OPEN", 1).alias("OPEN"),
    func.round("HIGH", 1).alias("HIGH"),
    func.round("LOW", 1).alias("LOW"),
    func.round("CLOSE", 1).alias("CLOSE"),
)

candles_data_frame = candles_data_frame.orderBy(
    "SYMBOL",
    "MOMENT",
)

print("Итоговые свечи:")
candles_data_frame.show(20, truncate=False)

output_directory = Path(arguments.output)

output_directory.mkdir(
    parents=True,
    exist_ok=True,
)

symbol_rows = (
    candles_data_frame
    .select("SYMBOL")
    .distinct()
    .collect()
)

symbols = [
    row["SYMBOL"]
    for row in symbol_rows
]

for symbol in symbols:
    temporary_directory = output_directory / f"_temporary_{symbol}"
    final_file = output_directory / f"{symbol}.csv"

    symbol_candles = (
        candles_data_frame
        .filter(func.col("SYMBOL") == symbol)
        .coalesce(1)
        .sortWithinPartitions("MOMENT")
    )

    (
        symbol_candles.write
        .mode("overwrite")
        .option("header", False)
        .csv(str(temporary_directory))
    )

    part_file = next(
        temporary_directory.glob("part-*.csv")
    )

    if final_file.exists():
        final_file.unlink()

    shutil.move(
        str(part_file),
        str(final_file),
    )

    shutil.rmtree(temporary_directory)

print(f"Результаты записаны в: {output_directory}")

spark.stop()