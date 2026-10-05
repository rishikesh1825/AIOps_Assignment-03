import os
import glob
import time
import shutil

import pandas as pd
import pyarrow.parquet as pq
import ray


# ============================================================
# Paths
# ============================================================

CURRENT_DIR = os.path.abspath(os.getcwd())

DATA_DIR = os.path.join(CURRENT_DIR, "Data")
OUTPUT_DIR = os.path.join(CURRENT_DIR, "Output")
DEDUP_DIR = os.path.join(CURRENT_DIR, "ray_deduped_data")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(DEDUP_DIR, exist_ok=True)

TRIP_DATA_PATTERN = os.path.join(DATA_DIR, "yellow_tripdata_*.parquet")
ZONE_LOOKUP_PATH = os.path.join(DATA_DIR, "taxi_zone_lookup.csv")

RAY_OUTPUT_PATH = os.path.join(
    OUTPUT_DIR,
    "ray_cleaned_data.parquet"
)


# ============================================================
# Find input files
# ============================================================

trip_data_paths = sorted(
    glob.glob(TRIP_DATA_PATTERN)
)

if not trip_data_paths:
    raise FileNotFoundError(
        f"No taxi Parquet files found matching:\n{TRIP_DATA_PATTERN}"
    )

print(f"Found {len(trip_data_paths)} taxi Parquet files.")


# ============================================================
# Step 1: Per-file preprocessing and deduplication
#
# This is deliberately done BEFORE Ray's distributed pipeline.
# The previous global groupby/map_groups operation caused a
# massive hash shuffle and exhausted the available disk space.
# ============================================================

print("\nPreparing monthly Parquet files...")

deduped_paths = []

for input_path in trip_data_paths:

    filename = os.path.basename(input_path)
    output_path = os.path.join(
        DEDUP_DIR,
        filename
    )

    print(f"Processing {filename} ...")

    # Read one monthly file
    df = pd.read_parquet(input_path)

    original_rows = len(df)

    # Remove exact duplicate rows within this monthly file
    df = df.drop_duplicates()

    deduped_rows = len(df)

    # Write the deduplicated monthly file
    df.to_parquet(
        output_path,
        index=False
    )

    deduped_paths.append(output_path)

    print(
        f"  rows: {original_rows:,} -> "
        f"{deduped_rows:,} "
        f"(removed {original_rows - deduped_rows:,})"
    )

    del df


print("\nPer-file deduplication complete.")


# ============================================================
# Step 2: Start / connect to Ray cluster
# ============================================================

print("\nConnecting to Ray cluster...")

ray.init(address="auto")

print("Connected to Ray cluster.")


# ============================================================
# Ray Data configuration
# ============================================================

data_context = ray.data.DataContext.get_current()

# Allow Ray to use disk-based shuffle if another Ray operation
# internally requires it. The pipeline below avoids the large
# global duplicate-removal shuffle.
data_context.use_disk_based_hash_shuffle = True


# ============================================================
# Start total runtime measurement
# ============================================================

start_time = time.time()


# ============================================================
# Step 3: Read taxi data
# ============================================================

print("\nReading taxi Parquet data...")

trip_ds = ray.data.read_parquet(
    deduped_paths
)

print("Taxi data loaded.")


# ============================================================
# Step 4: Read taxi zone lookup
# ============================================================

print("\nReading taxi zone lookup...")

zone_lookup_ds = ray.data.read_csv(
    ZONE_LOOKUP_PATH
)

# The zone lookup is tiny compared with the taxi data.
# Materialize it as pandas so that it can be broadcast to
# each Ray batch instead of performing a distributed shuffle.
zone_lookup_pd = zone_lookup_ds.to_pandas()

print(
    f"Taxi zone lookup loaded: "
    f"{len(zone_lookup_pd):,} rows"
)


# ============================================================
# Step 5: Normalize schema
#
# Different monthly files can have slightly different columns.
# Use the first file as the canonical schema and add missing
# columns where necessary.
# ============================================================

canonical_schema = pq.read_schema(
    deduped_paths[0]
)

canonical_columns = canonical_schema.names

print(
    f"\nCanonical schema contains "
    f"{len(canonical_columns)} columns."
)


def normalize_schema(batch):
    """
    Ensure every batch has exactly the canonical set
    of columns in the same order.
    """

    batch = batch.copy()

    for column in canonical_columns:

        if column not in batch.columns:
            batch[column] = pd.NA

    return batch[canonical_columns]


trip_ds = trip_ds.map_batches(
    normalize_schema,
    batch_format="pandas"
)


# ============================================================
# Step 6: Remove rows containing null values
# ============================================================

def remove_null_rows(batch):
    """
    Equivalent to Spark's dropna() for the complete row.
    """

    return batch.dropna()


clean_trips = trip_ds.map_batches(
    remove_null_rows,
    batch_format="pandas"
)


# ============================================================
# Step 7: Format timestamps
# ============================================================

def format_timestamps(batch):
    """
    Convert pickup and dropoff timestamps to datetime.
    """

    batch = batch.copy()

    batch["tpep_pickup_datetime"] = pd.to_datetime(
        batch["tpep_pickup_datetime"],
        errors="coerce"
    )

    batch["tpep_dropoff_datetime"] = pd.to_datetime(
        batch["tpep_dropoff_datetime"],
        errors="coerce"
    )

    return batch


clean_trips = clean_trips.map_batches(
    format_timestamps,
    batch_format="pandas"
)


# ============================================================
# Step 8: Broadcast-style join with taxi zone lookup
#
# Spark used a broadcast join because the zone lookup is tiny.
# We reproduce the same logical strategy here rather than
# performing a distributed Ray hash join over 100M+ rows.
# ============================================================

def broadcast_join_batch(batch, zone_lookup):

    batch = batch.merge(
        zone_lookup,
        how="inner",
        left_on="PULocationID",
        right_on="LocationID"
    )

    return batch


joined_data = clean_trips.map_batches(
    broadcast_join_batch,
    batch_format="pandas",
    fn_kwargs={
        "zone_lookup": zone_lookup_pd
    }
)


# ============================================================
# Step 9: Calculate average speed
#
# This corresponds to the Python UDF stage.
# ============================================================

def calculate_avg_speed(batch):

    batch = batch.copy()

    duration_hours = (
        (
            batch["tpep_dropoff_datetime"]
            - batch["tpep_pickup_datetime"]
        ).dt.total_seconds()
        / 3600.0
    )

    # Default value
    batch["avg_speed_mph"] = 0.0

    # Only calculate speed for positive-duration trips
    valid_duration = duration_hours > 0

    batch.loc[
        valid_duration,
        "avg_speed_mph"
    ] = (
        batch.loc[
            valid_duration,
            "trip_distance"
        ]
        / duration_hours.loc[valid_duration]
    )

    return batch


# ============================================================
# Step 10: Force complete UDF execution and measure UDF time
# ============================================================

print("\nExecuting average-speed UDF...")

udf_start_time = time.time()

final_data = joined_data.map_batches(
    calculate_avg_speed,
    batch_format="pandas"
)

# Ray Data execution is lazy.
# max() forces the complete upstream pipeline to execute.
max_speed = final_data.max(
    "avg_speed_mph"
)

udf_time = time.time() - udf_start_time

print(
    f"UDF stage completed in "
    f"{udf_time:.2f} seconds"
)

print(
    f"Maximum average speed: "
    f"{max_speed}"
)


# ============================================================
# Step 11: Write final Parquet output
# ============================================================

print("\nWriting final Ray output...")

final_data.write_parquet(
    RAY_OUTPUT_PATH,
    mode="overwrite"
)

print(
    f"Output written to:\n"
    f"{RAY_OUTPUT_PATH}"
)


# ============================================================
# Step 12: Total runtime
# ============================================================

total_time = time.time() - start_time

print("\n" + "=" * 60)
print("RAY PIPELINE COMPLETED")
print("=" * 60)

print(
    f"Number of input files : {len(trip_data_paths)}"
)

print(
    f"UDF execution time    : {udf_time:.2f} seconds"
)

print(
    f"Total runtime         : {total_time:.2f} seconds"
)

print(
    f"Maximum average speed : {max_speed}"
)

print(
    f"Output path           : {RAY_OUTPUT_PATH}"
)

print("=" * 60)


# ============================================================
# Shutdown Ray
# ============================================================

ray.shutdown()
