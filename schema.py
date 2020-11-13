#!/usr/bin/env python3
from typing import Iterable
from loguru import logger
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat
spark = SparkSession.builder.appName("schema").enableHiveSupport().getOrCreate()


def source_code_table(full_name):
    """Get the source code of a table.
    """
    try:
        return spark.sql(f"show create table {full_name}").toPandas().iloc[0, 0]
    except:
        return ""


def _source_code_db(dbase):
    logger.info("Processing {}...", dbase)
    df = spark.sql(f"show tables in {dbase}") \
        .filter(col("isTemporary") == False) \
        .withColumn("full_name", concat(col("database"), lit("."), col("tableName"))) \
        .toPandas()
    df["source_code"] = df.full_name.apply(source_code_table)
    spark.createDataFrame(df) \
        .write \
        .mode("overwrite") \
        .parquet(f"schema/{dbase}")


def source_code_dbs(dbases: Iterable[str]):
    """Get the source code of tables in databases.
    """
    for dbase in dbases:
        _source_code_db(dbase) 


if __name__ == "__main__":
    dbs = [
        "db1",
        "db2",
    ]
    source_code_dbs(dbs)
