from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col,when,count,year,month,rank,to_timestamp,round
from pyspark.sql.window import Window
from time import perf_counter

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def build_path(base_path: str, relative_path: str) -> str:
    return f"{base_path.rstrip('/')}/{relative_path.lstrip('/')}"


def write_local_csv_output(output_path: str, rows: list[tuple[int, int]]) -> None:
    os.makedirs(output_path, exist_ok=True)
    output_file = os.path.join(output_path, "part-00000")
    with open(output_file, "w", encoding="utf-8") as file_handle:
        for row in rows:
            file_handle.write(",".join(str(value) for value in row) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Average income.",
    )
    parser.add_argument("--base-path", help="Base path.")
    parser.add_argument("--census", help="Explicit census blocks path.")
    parser.add_argument("--income", help="Explicit household income CSV path.")
    parser.add_argument("--output", help="Explicit output path.")
    parser.add_argument("--master", help="Optional Spark master.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    census_path = args.census or "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Census_Blocks_2020.geojson"
     
    income_path = args.income or "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_income_2021.csv"

    builder = SparkSession.builder.appName("DF query 3 execution")
    if args.master:
        builder = builder.master(args.master)
        if args.master.startswith("local"):
            builder = builder.config("spark.submit.deployMode", "client")
    elif "://" not in census_path and "://" not in income_path:
        builder = builder.master("local[*]").config("spark.submit.deployMode", "client")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    output_path = args.output
    if output_path is None and args.base_path:
        output_path = build_path(args.base_path, f"DFQ33_{spark.sparkContext.applicationId}")

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

    df_census_agg = flattened_blocks_df.groupBy("ZCTA20").agg(
        F.sum("POP20").alias("total_population"),
        F.sum("HOUSING20").alias("total_housing")
    )

    income_df = spark.read.option("header", "true").option("delimiter", ";").csv(income_path)

    income_df_new = income_df.withColumn(
        "income", F.regexp_replace(F.col("Estimated Median Income"), "[\$,]", "").cast("double")
    )

    result_df = (
        df_census_agg.join(income_df_new, df_census_agg['ZCTA20'] == income_df_new['Zip Code'])
        .withColumn('average_income', round((col('total_housing')*col('income'))/col('total_population')))
        .select('ZCTA20','average_income')
    )

    result_df.explain("formatted")
    

    start = perf_counter()

    results = [(row.ZCTA20, row.average_income) for row in result_df.collect()]
    for item in results:
        print(item)
    
    elapsed = perf_counter() - start

    print(f"QUERY_ELAPSED_SECONDS={elapsed:.3f}")

    if output_path:
        if "://" in output_path:
            result_df.coalesce(1).write.mode("overwrite").csv(output_path)
        else:
            write_local_csv_output(output_path, results)
        print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()