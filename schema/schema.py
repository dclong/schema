#!/usr/bin/env python3
from typing import Iterable
from argparse import ArgumentParser
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


def _source_code_db(dbase, output):
    logger.info("Processing {}...", dbase)
    df = spark.sql(f"show tables in {dbase}") \
        .filter(col("isTemporary") == False) \
        .withColumn("full_name", concat(col("database"), lit("."), col("tableName"))) \
        .toPandas()
    df["source_code"] = df.full_name.apply(source_code_table)
    spark.createDataFrame(df) \
        .write \
        .mode("overwrite") \
        .parquet(f"{output}/{dbase}")


def source_code_dbs(dbases: Iterable[str], output):
    """Get the source code of tables in databases.
    """
    for dbase in dbases:
        _source_code_db(dbase, output)


def parse_args(args=None, namespace=None):
    """Parse command-line arguments.
    """
    parser = ArgumentParser(description="Get source of tables in databases.")
    parser.add_argument(
        "-d",
        "--dbs",
        "--databases",
        dest="dbs",
        nargs="+",
        default=(),
        help="A list of databases whose tables to get source code."
    )
    parser.add_argument(
        "-i",
        "--input",
        dest="input",
        default="",
        help="An input text file containing one database name in each line."
    )
    parser.add_argument(
        "-o",
        "--output",
        dest="output",
        required=True,
        help="Specify target memory in megabytes."
    )
    return parser.parse_args(args=args, namespace=namespace)


def main():
    args = parse_args()
    if args.input:
        with open(args.input, "r") as fin:
            args.dbs = [line.strip() for line in fin]
    source_code_dbs(args.dbs, args.output)


if __name__ == "__main__":
    main()
