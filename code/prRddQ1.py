from __future__ import annotations

import argparse
import os
import sys
import csv

from pyspark.sql import SparkSession
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

def parse_csv_line(line: str) -> list[str]:
    return next(csv.reader([line]))

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

    builder = SparkSession.builder.appName("Rdd query 1 execution")
    if args.master:
        builder = builder.master(args.master)
        if args.master.startswith("local"):
            builder = builder.config("spark.submit.deployMode", "client")
    elif "://" not in crimes1_path and "://" not in crimes2_path:
        builder = builder.master("local[*]").config("spark.submit.deployMode", "client")

    spark = builder.getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    output_path = args.output
    if output_path is None and args.base_path:
        output_path = build_path(args.base_path, f"RddQ1_{spark.sparkContext.applicationId}")
    
    crimes1 = sc.textFile(crimes1_path)
    crimes2 = sc.textFile(crimes2_path)

    header = crimes1.first()
    header_list = parse_csv_line(header)

    premis_idx = header_list.index("Premis Desc")
    time_idx = header_list.index("TIME OCC")

    all_crimes = (
        crimes1.union(crimes2)
        .filter(lambda line: line != header)
        .map(parse_csv_line)
    )

    street_crimes = all_crimes.filter(lambda row: row[premis_idx] == 'STREET')

    num_str_crimes = street_crimes.count()

    start = perf_counter()

    result = (
        street_crimes
        .map(lambda row: (partition_day(int(row[time_idx])), 1))
        .reduceByKey(lambda x,y: x+y)
    )
    
    percentage = result.map(lambda x: (x[0], round((x[1]*100)/num_str_crimes,2)))

    sort = percentage.sortBy(lambda x: x[1], ascending=False)

    results = sort.collect()
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