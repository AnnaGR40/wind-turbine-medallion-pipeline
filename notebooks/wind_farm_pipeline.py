# Databricks notebook source
# 0. Config and path check
from pyspark.sql import functions as F
from pyspark.sql import types as T

CATALOG = "wind_farm"
SCHEMA  = "turbines"
RAW_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

Z_THRESHOLD = 2          # flag turbines more than 2 standard deviations from the day's mean
# Standard deviation: stddev_samp (sample). Conservative default; the brief doesn't specify.
# To confirm with the business analyst. stddev_pop would flag 20 turbine-days instead of 17.

NULL_WARN_PCT = 0.01     # warn if more than 1% of rows have nulls in the data columns used in cell 8

# Configurable value bounds for quality checks used in cells 10 and 12 
WS_MIN    = 0      # Minimum valid wind speed (m/s)
DIR_MIN   = 0      # Minimum valid direction
DIR_MAX   = 360    # Maximum valid direction (exclusive)
POWER_MIN = 0      # Minimum valid power output (MW)

display(dbutils.fs.ls(RAW_PATH))

# COMMAND ----------

# plainly read
spark.read.text(f"{RAW_PATH}/data_group_1.csv").show(5, truncate=False)

# COMMAND ----------

# 1. Bronze: ingest data as is
bronze_df = (
    spark.read
         .option("header", True)
         .csv(f"{RAW_PATH}/data_group_*.csv")          # wildcard for the 3 files
         .select("*", "_metadata.file_path")  # keep every column plus the file path
         .withColumnRenamed("file_path", "source_file")
         .withColumn("ingested_at", F.current_timestamp())
)

(bronze_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("wind_farm.turbines.bronze_readings"))

# COMMAND ----------

display(bronze_df)

# COMMAND ----------

bronze = spark.table("wind_farm.turbines.bronze_readings")

print("Total rows:", bronze.count())

display(
    bronze.groupBy("source_file")
          .count()
          .orderBy("source_file")
)

# COMMAND ----------

# DBTITLE 1,Silver data casting cleaning and validation steps
# 2. Silver: type casting, cleaning, validation

# Silver data casting cleaning and validation steps
bronze = spark.table("wind_farm.turbines.bronze_readings")
silver_typed = (
    bronze
    .withColumn("timestamp", F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("turbine_id", F.col("turbine_id").cast("int"))
    .withColumn("wind_speed", F.col("wind_speed").cast("double"))
    .withColumn("wind_direction", F.col("wind_direction").cast("int"))
    .withColumn("power_output", F.col("power_output").cast("double"))
    .select(
        "timestamp",
        "turbine_id",
        "wind_speed",
        "wind_direction",
        "power_output",
        "source_file",
        "ingested_at"
    )
)

counts = {}
counts["loaded"] = bronze.count()
counts["after_cast"] = silver_typed.count()



# COMMAND ----------

display(silver_typed)


# COMMAND ----------

# DBTITLE 0,Null analysis with threshold warning on silver_typed
# Check 1: Null analysis with threshold warning on silver_typed

# Count nulls in each column
null_counts = silver_typed.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in silver_typed.columns
]).toPandas().iloc[0].to_dict()
print("Nulls per column:", null_counts)

# Five data columns
DATA_COLS = ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output"]

# Count rows with any null in the five columns
rows_with_any_null = silver_typed.where(
    " OR ".join([f"{c} IS NULL" for c in DATA_COLS])
).count()
percent_null = rows_with_any_null / counts["after_cast"]

print(f"Rows with any null in data columns: {rows_with_any_null} ({percent_null:.2%})")
if percent_null > NULL_WARN_PCT:
    print("WARNING: Too many nulls in critical columns")
else:
    print("Null check OK: No significant nulls detected")


# COMMAND ----------

# DBTITLE 0,Turbine duplicate key and value conflict detection summ ...
# Check 2: duplicates

# Turbine duplicate key and value conflict detection
key_groups = (
    silver_typed
    .groupBy("turbine_id", "timestamp")
    .agg(
        F.count("*").alias("group_count"),
        F.countDistinct("power_output", "wind_speed", "wind_direction").alias("value_distinct_count")
    )
)

duplicate_keys = key_groups.filter(F.col("group_count") > 1)
duplicate_count = duplicate_keys.count()
print(f"Duplicate key groups (turbine_id + timestamp): {duplicate_count}")

# Check for conflicting values
conflict_count = duplicate_keys.filter(F.col("value_distinct_count") > 1).count()
print(f"Duplicate key groups with conflicting values: {conflict_count}")

if duplicate_count > 0:
    print("WARNING: Duplicate keys detected.")
else:
    print("Duplicate check OK: No duplicate keys.")

if conflict_count > 0:
    print("Conflicting duplicates present.")
else:
    print("No conflicts among duplicates.")


# COMMAND ----------

# DBTITLE 0,Out-of-range value check
# Check 3: out-of-range values

# Bounds (add to config cell if needed)
# Wind speed rule: unexpected negative or zero
ws_out_of_range = silver_typed.filter(F.col("wind_speed") < WS_MIN).count()
print(f"Wind speed < {WS_MIN}: {ws_out_of_range}")

# Wind direction rule: invalid values
# Use DIR_MIN <= wind_direction < DIR_MAX (exclude DIR_MAX itself)
dir_out_of_range = silver_typed.filter((F.col("wind_direction") < DIR_MIN) | (F.col("wind_direction") >= DIR_MAX)).count()
print(f"Wind direction not in [{DIR_MIN},{DIR_MAX}): {dir_out_of_range}")

# Power output rule: negative
power_out_of_range = silver_typed.filter(F.col("power_output") < POWER_MIN).count()
print(f"Power output < {POWER_MIN}: {power_out_of_range}")

if ws_out_of_range + dir_out_of_range + power_out_of_range == 0:
    print("All value ranges OK.")
else:
    print("WARNING: Out-of-range values detected.")


# COMMAND ----------

# DBTITLE 1,Completeness check: expected rows per turbine
# Check 4: Completeness check: expected rows per turbine

from pyspark.sql.window import Window

# Count rows per turbine
turbine_hour_counts = silver_typed.groupBy("turbine_id").count() # actual rows per turbine

# Get min and max timestamp in data
min_max = silver_typed.agg(F.min("timestamp").alias("min_ts"), F.max("timestamp").alias("max_ts")).collect()[0]
min_ts = min_max["min_ts"]
max_ts = min_max["max_ts"]

# Calculate total expected hours: difference in hours (inclusive)
min_ts_sec = min_ts.timestamp()
max_ts_sec = max_ts.timestamp()
expected_hours = int((max_ts_sec - min_ts_sec) / 3600) + 1
print(f"Expected hours: {expected_hours}")

# Add check:
df_full = turbine_hour_counts.withColumn("expected_hours", F.lit(expected_hours))
df_full = df_full.withColumn("completeness", F.col("count") == F.col("expected_hours"))
display(df_full)

# Check output
if df_full.filter(~F.col("completeness")).count() == 0:
    print("Completeness OK: All turbines have expected 744 hours.")
else:
    print("WARNING: Some turbines are missing hours.")


# COMMAND ----------

# DBTITLE 1,Deduplication, out-of-range flag, split, count, reconciliation
# S3: quarantine null keys, dedup, flag out-of-range, split, count, reconcile
from pyspark.sql.window import Window

# Step 1: quarantine null keys
key_cols = ["turbine_id", "timestamp"]
silver_typed = silver_typed.withColumn("is_null_key",
    (F.col("turbine_id").isNull() | F.col("timestamp").isNull())
)
quarantine_null_key = silver_typed.filter(F.col("is_null_key"))
non_null_key_df = silver_typed.filter(~F.col("is_null_key"))
n_null_key = quarantine_null_key.count()

# Step 2: row_id
with_id = non_null_key_df.withColumn("row_id", F.monotonically_increasing_id())

# Step 3: dedup with custom rule
w_pick = Window.partitionBy(*key_cols).orderBy(
    F.col("ingested_at").desc(), F.col("source_file").desc(), F.col("row_id").desc()
)
w_all = Window.partitionBy(*key_cols)
ranked = (
    with_id
    .withColumn("rn", F.row_number().over(w_pick))
    .withColumn("had_conflict",
        F.size(F.collect_set(F.struct("wind_speed", "wind_direction", "power_output")).over(w_all)) > 1
    )
)
deduped = ranked.filter(F.col("rn") == 1).drop("rn", "row_id")
counts["after_dedup"] = deduped.count()

# Step 4: flag out-of-range rows
is_out_of_range = F.coalesce(
    (F.col("wind_speed") < WS_MIN) |
    (F.col("wind_direction") < DIR_MIN) |
    (F.col("wind_direction") >= DIR_MAX) |
    (F.col("power_output") < POWER_MIN), F.lit(False)
)
deduped = deduped.withColumn("is_out_of_range", is_out_of_range)
deduped = deduped.withColumn("is_valid", ~F.col("is_out_of_range"))

# Step 5: split and count
# Only select columns present in both quarantine_out_of_range and quarantine_null_key
dedup_columns = ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output", "source_file", "ingested_at"]
silver_clean = deduped.filter(F.col("is_valid")).select(*dedup_columns, "had_conflict")
quarantine_out_of_range = deduped.filter(~F.col("is_valid")).select(*dedup_columns).withColumn("reason", F.lit("out_of_range"))
quarantine_null_key = quarantine_null_key.select(*dedup_columns).withColumn("reason", F.lit("null_key"))
quarantine = quarantine_out_of_range.unionByName(quarantine_null_key)
counts["silver"] = silver_clean.count()
counts["quarantined"] = quarantine.count()


# Step 6: reconcile
display(silver_clean)
display(quarantine)
removed_dups = counts["after_cast"] - n_null_key - counts["after_dedup"]
print("Counts:", counts)
assert counts["after_cast"] == counts["silver"] + counts["quarantined"] + removed_dups


# COMMAND ----------

# Write cleaned silver readings to Delta table
silver_clean.write.format("delta").mode("overwrite").saveAsTable("wind_farm.turbines.silver_readings")

print("Silver readings written:", silver_clean.count())

# COMMAND ----------

# Write quarantine table to Delta (overwrite)
quarantine.write.format("delta").mode("overwrite").saveAsTable("wind_farm.turbines.silver_quarantine")

print("Quarantine table written:", quarantine.count())
print("Silver readings in table:", spark.table("wind_farm.turbines.silver_readings").count())

# COMMAND ----------

print("Counts:", counts)

# COMMAND ----------

# 3. Gold: daily summary and anomaly flags
silver = spark.table("wind_farm.turbines.silver_readings")

daily = (
    silver
    .withColumn("date", F.to_date("timestamp"))
    .groupBy("date", "turbine_id")
    .agg(
        F.min("power_output").alias("min_power"),
        F.max("power_output").alias("max_power"),
        F.avg("power_output").alias("avg_power"),
        F.count("*").alias("n_readings"),
    )
)
print("Daily rows:", daily.count())

# COMMAND ----------

from pyspark.sql.window import Window
w_day = Window.partitionBy("date")

gold = (
    daily
    .withColumn("day_mean", F.avg("avg_power").over(w_day))
    .withColumn("day_std",  F.stddev_samp("avg_power").over(w_day))   # sample std dev (see config)
    .withColumn("z_score",  (F.col("avg_power") - F.col("day_mean")) / F.col("day_std"))
    .withColumn("is_anomaly", F.abs(F.col("z_score")) > Z_THRESHOLD)
)

# COMMAND ----------

flagged = gold.filter("is_anomaly").orderBy("date", "turbine_id")
print("Flagged turbine-days:", flagged.count())
display(flagged)

# COMMAND ----------

# 3. Gold output: write daily summary

gold.write.format("delta").mode("overwrite").saveAsTable("wind_farm.turbines.gold_daily_summary")

gold_summary = spark.table("wind_farm.turbines.gold_daily_summary")
anomaly_count = gold_summary.filter("is_anomaly").count()
print("Gold daily summary row count:", gold_summary.count())
print("Anomaly flag count:", anomaly_count)

# COMMAND ----------

# 4. Export outputs
import os
OUT_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/raw/outputs"   # a subfolder in your volume
os.makedirs(OUT_DIR, exist_ok=True)

cleaned = spark.table("wind_farm.turbines.silver_readings").orderBy("turbine_id", "timestamp")
summary = spark.table("wind_farm.turbines.gold_daily_summary").orderBy("date", "turbine_id")
flagged = summary.filter("is_anomaly")

cleaned.toPandas().to_csv(f"{OUT_DIR}/cleaned_readings.csv", index=False)
summary.toPandas().round(4).to_csv(f"{OUT_DIR}/daily_summary.csv", index=False)
flagged.toPandas().round(4).to_csv(f"{OUT_DIR}/flagged_anomalies.csv", index=False)

display(dbutils.fs.ls(OUT_DIR))