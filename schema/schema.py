#!/usr/bin/env python3
from typing import Iterable
from argparse import ArgumentParser
from loguru import logger
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


def describe(table_name: str):
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
    return frame


def _dump_schema_db(self, dbase, output_dir: Path) -> None:
    logger.info("Dumping schema of tables in the database {}", dbase)
    output_dir = output_dir / dbase
    output_dir.mkdir(parents=True, exist_ok=True)
    tables = spark.sql(f"SHOW TABLES in {dbase}").toPandas()
    for _, (db, table) in tqdm(tables[["database", "table"]].iterrows(), total=tables.shape[0]):
        table = f"{db}.{table}"
        schema = describe(table)
        with (output_dir / f"{table}.txt").open("w") as fout:
            fout.write(table + "\n")
            for _, (col_name, data_type) in schema[["col_name", "data_type"]].iterrows(): 
                fout.write(f"{col_name}    {data_type}\n")


def dump_schema_db(self, dbs: Union[str, List[str]], output_dir: Union[str, Path] = ""):
    if isinstance(dbs, str):
        dbs = [dbs]
    if isinstance(output_dir, str):
        output_dir = Path(output_dir)
    for db in dbs:
        self._dump_schema_db(db, output_dir)


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
