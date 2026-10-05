import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, udf
from pyspark.sql.types import DoubleType


# ============================================================
# PATHS
# ============================================================

current_dir = os.path.abspath(os.getcwd())

trip_data_path = f"file://{current_dir}/Data/yellow_tripdata_*.parquet"
zone_lookup_path = f"file://{current_dir}/Data/taxi_zone_lookup.csv"
output_path = f"file://{current_dir}/Output/spark_cleaned_data.parquet"


# ============================================================
# SPARK CONFIGURATION
# ============================================================

spark = (
    SparkSession.builder
    .master("spark://10.143.141.144:7077")
    .appName("TaxiDataCleaning_Spark")

    # --------------------------------------------------------
    # Cluster resources
    # 2 workers × 2 cores = 4 total cores
    # --------------------------------------------------------
    .config("spark.executor.instances", "2")
    .config("spark.executor.cores", "2")
    .config("spark.cores.max", "4")

    # Worker limit = 3 GB
    # Executor heap = 2 GB
    .config("spark.executor.memory", "2g")

    # Driver runs on the local machine
    .config("spark.driver.memory", "2g")

    # Moderate shuffle parallelism for the available RAM
    .config("spark.sql.shuffle.partitions", "16")

    # Better diagnostics if a Python worker crashes
    .config(
        "spark.sql.execution.pyspark.udf.faulthandler.enabled",
        "true"
    )
    .config(
        "spark.python.worker.faulthandler.enabled",
        "true"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# BENCHMARK START
# ============================================================

start_time = time.time()


# ============================================================
# 1. INGESTION
# ============================================================

trip_data = spark.read.parquet(trip_data_path)

zone_lookup = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(zone_lookup_path)
)


# ============================================================
# 2. CLEANSING
# ============================================================

# Remove rows containing null values
# Remove duplicate records
clean_trips = (
    trip_data
    .dropna()
    .dropDuplicates()
)

# Format timestamps
clean_trips = (
    clean_trips
    .withColumn(
        "pickup_time",
        col("tpep_pickup_datetime").cast("timestamp")
    )
    .withColumn(
        "dropoff_time",
        col("tpep_dropoff_datetime").cast("timestamp")
    )
)


# ============================================================
# 3. JOIN
# ============================================================

joined_data = clean_trips.join(
    broadcast(zone_lookup),
    clean_trips.PULocationID == zone_lookup.LocationID,
    "inner"
)


# ============================================================
# 4. CUSTOM PYTHON UDF
# ============================================================

def calculate_avg_speed(distance, pickup_time, dropoff_time):
    """
    Calculate average speed in miles per hour.
    """

    if (
        distance is None
        or pickup_time is None
        or dropoff_time is None
    ):
        return 0.0

    duration_seconds = (
        dropoff_time.timestamp()
        - pickup_time.timestamp()
    )

    if duration_seconds <= 0:
        return 0.0

    duration_hours = duration_seconds / 3600.0

    return float(distance / duration_hours)


avg_speed_udf = udf(
    calculate_avg_speed,
    DoubleType()
)


# ============================================================
# UDF EXECUTION TIMING
# ============================================================

udf_start = time.time()

final_data = joined_data.withColumn(
    "avg_speed_mph",
    avg_speed_udf(
        col("trip_distance"),
        col("pickup_time"),
        col("dropoff_time")
    )
)

# Force execution of the UDF
final_data.select("avg_speed_mph").agg(
    {"avg_speed_mph": "max"}
).show()

udf_time = time.time() - udf_start


# ============================================================
# 5. EXPORT
# ============================================================

final_data.write.mode("overwrite").parquet(output_path)


# ============================================================
# BENCHMARK RESULTS
# ============================================================

total_time = time.time() - start_time

print()
print("=" * 60)
print("SPARK BENCHMARK RESULTS")
print("=" * 60)
print(f"Total Execution Time : {total_time:.2f} seconds")
print(f"Python UDF Stage Time: {udf_time:.2f} seconds")
print("=" * 60)


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()
