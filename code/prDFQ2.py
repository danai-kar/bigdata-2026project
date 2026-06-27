from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when,count,year,month,rank,to_timestamp
from pyspark.sql.window import Window
from time import perf_counter

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def build_path(base_path: str, relative_path: str) -> str:
    return f"{base_path.rstrip('/')}/{relative_path.lstrip('/')}"


def write_local_csv_output(output_path: str, rows: list[tuple[int, int, int, int]]) -> None:
    os.makedirs(output_path, exist_ok=True)
    output_file = os.path.join(output_path, "part-00000")
    with open(output_file, "w", encoding="utf-8") as file_handle:
        for row in rows:
            file_handle.write(",".join(str(value) for value in row) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Order months of each year by number of crimes committed.",
    )
    parser.add_argument("--base-path", help="Base path.")
    parser.add_argument("--crimes1", help="Explicit crimes part1 CSV path.")
    parser.add_argument("--crimes2", help="Explicit crimes part2 CSV path.")
    parser.add_argument("--output", help="Explicit output path.")
    parser.add_argument("--master", help="Optional Spark master.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    crimes1_path = args.crimes1 or "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv"
     
    crimes2_path = args.crimes2 or "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv"

    builder = SparkSession.builder.appName("DF query 2 execution")
    if args.master:
        builder = builder.master(args.master)
        if args.master.startswith("local"):
            builder = builder.config("spark.submit.deployMode", "client")
    elif "://" not in crimes1_path and "://" not in crimes2_path:
        builder = builder.master("local[*]").config("spark.submit.deployMode", "client")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    output_path = args.output
    if output_path is None and args.base_path:
        output_path = build_path(args.base_path, f"DFQ2_{spark.sparkContext.applicationId}")

    crimes1_df = spark.read.csv(crimes1_path,  header=True).select('DATE OCC')
    crimes2_df = spark.read.csv(crimes2_path,  header=True).select('DATE OCC')

    all_crimes = crimes1_df.union(crimes2_df)

    count_df = (
        all_crimes
        .withColumn('parsed_date', to_timestamp(col('DATE OCC'), 'yyyy MMM dd hh:mm:ss a'))
        .withColumn('year',year(col('parsed_date')))
        .withColumn('month', month(col('parsed_date')))
        .groupBy('year', 'month')
        .agg(count('*').alias('crime_total'))
    )

    partition = Window.partitionBy('year').orderBy(col('crime_total').desc())

    ranked_df = count_df.withColumn('ranking', rank().over(partition))

    result_df = (
        ranked_df
        .filter(col('ranking')<=3)
        .orderBy(col('year').asc(), col('crime_total').desc(), col('ranking').asc())
        .select('year','month','crime_total','ranking')
    )

    
    result_df.show(50)

    start = perf_counter()

    results = [(row.year, row.month, row.crime_total, row.ranking) for row in result_df.collect()]
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