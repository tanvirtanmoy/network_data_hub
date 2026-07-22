"""Shared Spark session factory."""

from pyspark.sql import SparkSession


def get_spark(cfg: dict) -> SparkSession:
    spark_cfg = cfg["spark"]
    builder = (
        SparkSession.builder.appName(spark_cfg["app_name"])
        .master(spark_cfg["master"])
        .config("spark.sql.shuffle.partitions", spark_cfg["shuffle_partitions"])
        .config("spark.sql.session.timeZone", "UTC")
    )
    # In production this is also where the Iceberg catalog and S3 credentials
    # get configured, e.g.
    #   .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    #   .config("spark.sql.catalog.lakehouse.type", "glue")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
