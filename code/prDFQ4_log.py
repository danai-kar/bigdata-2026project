from __future__ import annotations

import argparse
import os
import sys
import math 

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when,count,avg,round,udf
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DoubleType
from time import perf_counter

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def build_path(base_path: str, relative_path: str) -> str:
    return f"{base_path.rstrip('/')}/{relative_path.lstrip('/')}"


def write_local_csv_output(output_path: str, rows: list[tuple[str, int, int]]) -> None:
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
    parser.add_argument("--stations", help="Explicit police stations CSV path.")
    parser.add_argument("--output", help="Explicit output path.")
    parser.add_argument("--master", help="Optional Spark master.")
    return parser.parse_args()

def main() -> None:
    args = parse_args()
    crimes1_path = args.crimes1 or "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv"
     
    crimes2_path = args.crimes2 or "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv"

    stations_path = args.stations or "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Police_Stations.csv"

    builder = SparkSession.builder.appName("DF query 4 execution")
    if args.master:
        builder = builder.master(args.master)
        if args.master.startswith("local"):
            builder = builder.config("spark.submit.deployMode", "client")
    elif "://" not in crimes1_path and "://" not in crimes2_path and "://" not in stations_path:
        builder = builder.master("local[*]").config("spark.submit.deployMode", "client")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    output_path = args.output
    if output_path is None and args.base_path:
        output_path = build_path(args.base_path, f"DFQ4_{spark.sparkContext.applicationId}")

    crimes1_df = spark.read.csv(crimes1_path,  header=True)
    crimes2_df = spark.read.csv(crimes2_path,  header=True)
    stations_df = spark.read.csv(stations_path, header=True)

    all_crimes = crimes1_df.union(crimes2_df)

    stations = stations_df.collect()

    new_stations = []
    for station in stations:
        new_stations.append({'division':str(station['DIVISION']),'lat':float(str(station['Y'])),'lon':float(str(station['X']))})

    stations_b = spark.sparkContext.broadcast(new_stations)

    def closest_station(long,lat):
        min_dist = float('inf')
        closest_station = None
        R = 6371.0

        long1 = math.radians(float(long))
        lat1 = math.radians(float(lat))

        for station in stations_b.value:
            long2 = math.radians(station['lon'])
            lat2 = math.radians(station['lat'])

            dlong = long2 - long1
            dlat = lat2 - lat1

            a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlong / 2)**2
            c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
            distance = R * c
            
            if distance < min_dist:
                min_dist = distance
                closest_station = station['division']

        return (closest_station,min_dist)


    station_dist_schema = StructType([
        StructField("division", StringType(), True),
        StructField("distance", DoubleType(), True)
    ])

    find_station = udf(closest_station,station_dist_schema)

    station_df = all_crimes.withColumn('closest_station',find_station(col('LON'),col('LAT')))

    result_df = (
        station_df
        .groupBy(col('closest_station.division').alias('division'))
        .agg(count('*').alias('crime_total'),round(avg(col('closest_station.distance')),2).alias('avg_distance'))
        .select('division','avg_distance','crime_total')
    )

    result_df.explain("formatted")

    start = perf_counter()

    results = [(row.division, row.avg_distance, row.crime_total) for row in result_df.collect()]
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