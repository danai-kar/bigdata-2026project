from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def build_path(base_path: str, relative_path: str) -> str:
    return f"{base_path.rstrip('/')}/{relative_path.lstrip('/')}"

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Turn csv file to parquet.",
    )
    parser.add_argument("--base-path", help="Base path.")
    parser.add_argument("--crimes1", help="Explicit crimes part1 CSV path.")
    parser.add_argument("--crimes2", help="Explicit crimes part2 CSV path.")
    parser.add_argument("--master", help="Optional Spark master.")
    parser.add_argument("--output-path", help="Output path for parquet.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    crimes1_path = args.crimes1 or "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv"
     
    crimes2_path = args.crimes2 or "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv"
    
    builder = SparkSession.builder.appName("csv to parquet")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    output_path = args.base_path or f"/user/dsml00293/data_parquet"
    if output_path is None and args.base_path:
        output_path = build_path(args.base_path, f"CSVtoPAR_{spark.sparkContext.applicationId}")

    crimes1_df = spark.read.csv(crimes1_path,  header=True)
    crimes2_df = spark.read.csv(crimes2_path,  header=True)

    crimes1_par_path = build_path(output_path, 'Crimes1.parquet')
    crimes2_par_path = build_path(output_path, 'Crimes2.parquet')

    crimes1_df.write.mode('overwrite').parquet(crimes1_par_path)
    crimes2_df.write.mode('overwrite').parquet(crimes2_par_path)

    print('ΕΠΙΤΥΧΙΑ')

    spark.stop()

if __name__ == "__main__":
    main()
