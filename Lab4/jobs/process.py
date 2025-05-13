from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, to_date, month, desc,
    row_number, max as spark_max, date_sub, lit
)
from pyspark.sql.window import Window


def avg_duration_by_day(df):
    return (
        df.withColumn("start_date", to_date(col("start_time"), "yyyy-MM-dd HH:mm:ss"))
          .groupBy("start_date")
          .agg(avg("tripduration").alias("avg_duration"))
    )


def trips_count_by_day(df):
    return (
        df.withColumn("start_date", to_date(col("start_time"), "yyyy-MM-dd HH:mm:ss"))
          .groupBy("start_date")
          .agg(count("trip_id").alias("trips_count"))
    )


def top_start_station_per_month(df):
    df2 = df.withColumn("start_date", to_date(col("start_time"), "yyyy-MM-dd HH:mm:ss"))
    df2 = df2.withColumn("month", month(col("start_date")))
    w = Window.partitionBy("month").orderBy(desc("count"))
    popular = (
        df2.groupBy("month", "from_station_name")
           .agg(count("trip_id").alias("count"))
           .withColumn("rank", row_number().over(w))
           .filter(col("rank") == 1)
           .drop("rank")
    )
    return popular


def top3_stations_last_2weeks_per_day(df):
    # Визначаємо максимальну дату
    max_date_row = df.select(
        spark_max(to_date(col("start_time"), "yyyy-MM-dd HH:mm:ss")).alias("max_date")
    ).collect()[0]
    max_date = max_date_row["max_date"]
    # Фільтруємо по останнім 14 дням
    df2 = df.withColumn("start_date", to_date(col("start_time"), "yyyy-MM-dd HH:mm:ss"))
    df_recent = df2.filter(
        (col("start_date") >= date_sub(lit(max_date), 14)) &
        (col("start_date") <= lit(max_date))
    )
    # Групуємо та ранжуємо
    w = Window.partitionBy("start_date").orderBy(desc("count"))
    ranked = (
        df_recent.groupBy("start_date", "from_station_name")
                 .agg(count("trip_id").alias("count"))
                 .withColumn("rank", row_number().over(w))
    )
    return ranked.filter(col("rank") <= 3).drop("rank")


def gender_avg_duration(df):
    return (
        df.groupBy("gender")
          .agg(avg("tripduration").alias("avg_duration"))
    )


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Divvy Q4 Analysis") \
        .getOrCreate()

    # Читання CSV
    input_path = "/opt/bitnami/spark/jobs/Divvy_Trips_2019_Q4.csv"
    df = spark.read.option("header", True) \
                   .option("inferSchema", True) \
                   .csv(input_path)

    tasks = [
        (avg_duration_by_day, "avg_duration_by_day"),
        (trips_count_by_day, "trips_count_by_day"),
        (top_start_station_per_month, "popular_stations_by_month"),
        (top3_stations_last_2weeks_per_day, "top_3_stations_last2weeks_per_day"),
        (gender_avg_duration, "avg_duration_by_gender")
    ]

    output_base = "/opt/bitnami/spark/jobs/out"
    for func, name in tasks:
        result = func(df)
        result.coalesce(1) \
              .write.mode("overwrite") \
              .option("header", True) \
              .csv(f"{output_base}/{name}")

    spark.stop()
