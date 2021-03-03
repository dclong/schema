"""Get schema of Hive tables using PySpark.
"""
#!/usr/bin/env python3
from __future__ import annotations
from typing import Union, Iterable
from argparse import ArgumentParser, Namespace
import numpy as np
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat
spark = SparkSession.builder.appName("schema").enableHiveSupport().getOrCreate()


def source_code_table(table_name: str) -> str:
    """Get the source code of a table.

    :param tabe_name: The full name of a table.
    """
    try:
        return spark.sql(f"show create table {table_name}").toPandas().iloc[0, 0]
    except:  # pylint: disable=W0702
        return ""


def _source_code_db(dbase: str, output_dir: str) -> None:
    """Get the source code of tables in a database.

    :param dbase: The name of a database.
    :param output_dir: The output director.
    """
    logger.info("Processing {}...", dbase)
    frame = spark.sql(f"show tables in {dbase}") \
        .filter(~col("isTemporary")) \
        .withColumn("full_name", concat(col("database"), lit("."), col("tableName"))) \
        .toPandas()
    frame["source_code"] = frame.full_name.apply(source_code_table)
    spark.createDataFrame(frame) \
        .write \
        .mode("overwrite") \
        .parquet(f"{output_dir}/{dbase}")


def schema(table_name: str) -> str:
    """Describe a table.

    :param table_name: The name of the table to describe.
    :param from_cache: Whether to read cached results (if available)
    instead of querying the database.
    :param to_cache: Whether to save SQL results to cache.
    :return: A DataFrame containing schema of the table.
    """
    frame = spark.sql(f"DESCRIBE TABLE {table_name}").toPandas()
    arr = np.where(frame.col_name.str.startswith("# "))[0]
    if arr.size > 0:
        frame = frame.iloc[:arr.min(), ].copy()
    frame.col_name = frame.col_name.str.lower()
    return table_name + "\n" + "\n".join(frame.col_name + "    " + frame.data_type)


def _dump_schema_db(dbase: str, output_dir: str) -> None:
    logger.info("Dumping schema of tables in the database {}", dbase)
    tables = spark.sql(f"SHOW TABLES in {dbase}").toPandas()
    tables["schema"] = [schema(table) for table in tables.database + "." + tables.table]
    spark.createDataFrame(tables).write.mode("overwrite"
                                            ).parquet(f"{output_dir}/{dbase}")


def dump_schema_db(dbases: Union[str, list[str]], output_dir: str) -> None:
    """Dump schema of tables in databases.

    :param dbs: A (list of) database(s).
    :param output: The output directory.
    """
    if isinstance(dbases, str):
        dbases = [dbases]
    for dbase in dbases:
        _dump_schema_db(dbase, output_dir)


def source_code_dbs(dbases: Iterable[str], output) -> None:
    """Get the source code of tables in databases.
    """
    for dbase in dbases:
        _source_code_db(dbase, output)


def parse_args(args=None, namespace=None) -> Namespace:
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


def main() -> None:
    """The main function of the script.
    """
    args = parse_args()
    if args.input:
        with open(args.input, "r") as fin:
            args.dbs = [line.strip() for line in fin]
    dump_schema_db(args.dbs, args.output)


if __name__ == "__main__":
    main()
