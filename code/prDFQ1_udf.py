from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when,count,round,udf
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from time import perf_counter

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def build_path(base_path: str, relative_path: str) -> str:
    return f"{base_path.rstrip('/')}/{relative_path.lstrip('/')}"


def write_local_csv_output(output_path: str, rows: list[tuple[str, float]]) -> None:
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
    parser.add_argument("--crimes1", help="Explicit crimes part1 CSV path.")
    parser.add_argument("--crimes2", help="Explicit crimes part2 CSV path.")
    parser.add_argument("--output", help="Explicit output path.")
    parser.add_argument("--master", help="Optional Spark master.")
    return parser.parse_args()

def partition_day(time_occ):
    if 500<=time_occ<=1159:
        part = 'morning'
    elif 1200<=time_occ<=1659:
        part = 'afternoon'
    elif 1700<=time_occ<=2059:
        part = 'evening'
    else:
        part='night'
    return part 


def main() -> None:
    args = parse_args()
    crimes1_path = args.crimes1 or "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv"
     
    crimes2_path = args.crimes2 or "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv"

    builder = SparkSession.builder.appName("DF query 1 execution")
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
        output_path = build_path(args.base_path, f"DFQ1udf_{spark.sparkContext.applicationId}")

    crimes1_df = spark.read.csv(crimes1_path,  header=True)
    crimes2_df = spark.read.csv(crimes2_path,  header=True)

    all_crimes = crimes1_df.union(crimes2_df)

    street_crimes = all_crimes.filter(col("Premis Desc") == "STREET").withColumn("TIME OCC", col("TIME OCC").cast("int"))

    num_str_crimes = street_crimes.count()

    day_part_udf = udf(partition_day, StringType())
    

    result_df = (
        street_crimes.withColumn('period',day_part_udf(col('TIME OCC')))
        .groupBy('period')
        .agg(count('*').alias('num-crimes'))
        .withColumn('percentage', round(col('num-crimes')*100/num_str_crimes ,2))
        .orderBy(col('percentage').desc())
        .select('period','percentage')
    )
    
    start = perf_counter()

    results = [(row.period, row.percentage) for row in result_df.collect()]
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