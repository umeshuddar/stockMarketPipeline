from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    avg,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    lag,
    round as spark_round,
)
from pyspark.sql.window import Window
import sys


def create_spark_session():
    return (
        SparkSession.builder
        .appName("StockMarketBatchProcessor")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .getOrCreate()
    )


def process_stock_data(spark, execution_date):
    input_path = "s3a://realtime-stock-data/realtime/*.parquet"

    print(f"Reading data from: {input_path}")

    df = spark.read.parquet(input_path)

    print(f"Total records read: {df.count()}")

    window_spec = (
        Window
        .partitionBy("ticker")
        .orderBy("timestamp")
    )

    processed_df = (
        df
        .withColumn(
            "prev_close",
            lag("close", 1).over(window_spec)
        )
        .withColumn(
            "price_change",
            spark_round(
                col("close") - col("prev_close"),
                4,
            ),
        )
        .withColumn(
            "price_change_pct",
            spark_round(
                (col("price_change") / col("prev_close")) * 100,
                4,
            ),
        )
    )

    summary_df = (
        df.groupBy("ticker")
        .agg(
            spark_round(avg("close"), 4).alias("avg_close"),
            spark_round(spark_max("high"), 4).alias("max_high"),
            spark_round(spark_min("low"), 4).alias("min_low"),
            spark_sum("volume").alias("total_volume"),
        )
    )

    output_path = (
        f"s3a://stock-market-data/processed/{execution_date}"
    )

    processed_df.write \
        .mode("overwrite") \
        .partitionBy("ticker") \
        .parquet(f"{output_path}/detailed")

    summary_df.write \
        .mode("overwrite") \
        .parquet(f"{output_path}/summary")

    print(f"Processed data written to: {output_path}")


if __name__ == "__main__":
    execution_date = (
        sys.argv[1]
        if len(sys.argv) > 1
        else "2024-01-01"
    )

    spark = create_spark_session()

    process_stock_data(spark, execution_date)

    spark.stop()