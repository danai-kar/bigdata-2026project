from __future__ import annotations

import argparse
import os
import sys
import csv

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from time import perf_counter

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def build_path(base_path: str, relative_path: str) -> str:
    return f"{base_path.rstrip('/')}/{relative_path.lstrip('/')}"


def write_local_csv_output(output_path: str, rows: list[tuple[str, str]]) -> None:
    os.makedirs(output_path, exist_ok=True)
    output_file = os.path.join(output_path, "part-00000")
    with open(output_file, "w", encoding="utf-8") as file_handle:
        for row in rows:
            file_handle.write(",".join(str(value) for value in row) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Order times of day by percentage of crimes committed on the street.",
    )
    parser.add_argument("--base-path", help="Base path.")
    parser.add_argument("--census", help="Explicit census blocks CSV path.")
    parser.add_argument("--income", help="Explicit household income CSV path.")
    parser.add_argument("--output", help="Explicit output path.")
    parser.add_argument("--master", help="Optional Spark master.")
    return parser.parse_args()

def parse_csv_line(line: str) -> list[str]:
    return next(csv.reader([line]))

def safe_int(val: str) -> int | None:
    try:
        clean_val = str(val).replace("$", "").replace(",", "").strip()
        return int(clean_val)
    except (ValueError, TypeError):
        return None

def main() -> None:
    args = parse_args()
    census_path = args.census or "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Census_Blocks_2020.geojson"
     
    income_path = args.income or "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_income_2021.csv"

    builder = SparkSession.builder.appName("Rdd query 3 execution")
    if args.master:
        builder = builder.master(args.master)
        if args.master.startswith("local"):
            builder = builder.config("spark.submit.deployMode", "client")
    elif "://" not in census_path and "://" not in income_path:
        builder = builder.master("local[*]").config("spark.submit.deployMode", "client")

    spark = builder.getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    output_path = args.output
    if output_path is None and args.base_path:
        output_path = build_path(args.base_path, f"RddQ3_{spark.sparkContext.applicationId}")
    
    blocks_df = (
    spark.read
    .option("multiLine", "true")
    .json(census_path)
    ).selectExpr("explode(features) as features") \
    .select("features.*")

    flattened_blocks_df = blocks_df.select(
      [
        F.col(f"properties.{col_name}").alias(col_name)
        for col_name in blocks_df.schema["properties"].dataType.fieldNames()
      ]
      + ["geometry"]
    ).drop("properties").drop("type")

    census_rdd = flattened_blocks_df.rdd
    mapped_census = census_rdd.filter(
        lambda row: row.ZCTA20 is not None and row.POP20 is not None and row.HOUSING20 is not None and row.POP20 > 0
    ).map(
        lambda row: (str(row.ZCTA20), (float(row.POP20), float(row.HOUSING20)))
    )
    census_agg_rdd = mapped_census.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    income = sc.textFile(income_path)

    header = income.first()
    income_rdd = income.filter(lambda line: line!=header).map(lambda line: line.split(';')).filter(lambda parts: len(parts) >= 3 and parts[2] != "").map(lambda parts: (parts[0].strip(), safe_int(parts[2])))

    joined_rdd = census_agg_rdd.join(income_rdd)

    result_df = joined_rdd.filter(
        lambda x: x[1][0][0] is not None and x[1][0][1] is not None and x[1][1] is not None and x[1][0][0] != 0
    ).map(
        lambda x: (
            x[0], 
            round((x[1][0][1] * x[1][1]) / x[1][0][0], 2)
        )
    )

    start = perf_counter()

    results = result_df.collect()
    for item in results:
        print(item)
    
    elapsed = perf_counter() - start

    print(f"QUERY_ELAPSED_SECONDS={elapsed:.3f}")

    if output_path:
        write_local_csv_output(output_path, results)
        print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()