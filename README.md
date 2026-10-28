# Wind Turbine Medallion Pipeline

PySpark / Delta pipeline on Databricks that cleans hourly readings from a 15-turbine wind farm and flags unusual daily turbine performance. Bronze → Silver → Gold.

## Repository contents

| Path | What it is |
|---|---|
| `notebooks/wind_farm_pipeline.py` | The whole pipeline in one Databricks notebook |
| `outputs/cleaned_readings.csv` | Cleaned data (silver), 11,160 rows |
| `outputs/daily_summary.csv` | Daily min/max/avg power per turbine with anomaly flags (gold), 465 rows |
| `outputs/flagged_anomalies.csv` | Only the flagged turbine-days (17 rows) |

The raw input CSVs (`data_group_1/2/3.csv`) are not in this repo.

## How to run

1. Use a Databricks workspace (developed on Databricks Free Edition, serverless).
2. Create a catalog `wind_farm` and a schema `turbines`, then a managed volume `raw`.
3. Upload the three raw CSVs to `/Volumes/wind_farm/turbines/raw/`.
4. Import `notebooks/wind_farm_pipeline.py` into the workspace (Import → file).
5. If your names differ, edit `CATALOG`, `SCHEMA` and `RAW_PATH` in the first cell. All settings live there (thresholds, value bounds, null warning level).
6. Click **Run all**. It creates Delta tables `bronze_readings`, `silver_readings`, `silver_quarantine`, `gold_daily_summary`, and writes the CSV exports to `/Volumes/wind_farm/turbines/raw/outputs/`.

## Pipeline design

- **Bronze:** the three files loaded as they are (all columns strings) plus `source_file` and `ingested_at`. Written with `overwrite` so re-running never duplicates rows (Delta keeps the version history).
- **Silver:** explicit type casts (`to_timestamp` and numeric casts), data-quality checks, deduplication, and a split into valid rows (`silver_readings`) and rejected rows (`silver_quarantine`, with a `reason`).
- **Gold:** daily min, max and average power per turbine, then for each day the mean and standard deviation of the 15 turbine averages, a z-score and an anomaly flag.

## Row counts at each stage

| Stage | Rows |
|---|---|
| Loaded (bronze, 3 files x 3,720) | 11,160 |
| After type cast | 11,160 |
| After deduplication | 11,160 |
| Valid (silver) | 11,160 |
| Quarantined | 0 |
| Gold (31 days x 15 turbines) | 465 |

## Data problems found and what I did

I checked for each problem the brief describes. **None were present in this dataset**, but the code handles all of them.

| Check | Result | Handling rule |
|---|---|---|
| Null in `timestamp` or `turbine_id` | 0 | Quarantine (reason `null_key`) |
| Null in a measurement | 0 | Keep and flag, never fill (filling would bias averages) |
| Null percentage warning | 0.00% | Warning if more than 1% of rows have nulls |
| Exact duplicates | 0 | Keep one |
| Conflicting duplicates (same turbine and hour, different values) | 0 | Keep the last by a fixed order: latest `ingested_at`, then later `source_file`, then later row; flagged `had_conflict` |
| Out-of-range: wind speed < 0, direction outside 0-359, power < 0 | 0 | Quarantine (reason `out_of_range`) |
| Missing hours | 0 (744 rows for each of the 15 turbines) | Report only, never fill |

Row counts reconcile: loaded = silver + quarantined + duplicates removed.

## Anomaly flags

For each day I took the 15 turbine averages, calculated their mean and **sample standard deviation**, and flagged any turbine whose average is more than 2 standard deviations from that day's mean (`|z| > 2`).

**17 turbine-days were flagged:**

| Turbine | Flagged dates (March 2022) |
|---|---|
| 4 | 11, 20, 26 |
| 3 | 6, 28 |
| 9 | 15, 29 |
| 11 | 8, 22 |
| 12 | 3, 12 |
| 15 | 1, 18 |
| 1 | 7 |
| 2 | 2 |
| 7 | 13 |
| 8 | 4 |

Full details (average, day mean, z-score) are in `outputs/flagged_anomalies.csv`.

**Interpretation:** These are statistical outliers, not confirmed faults. With 465 turbine-days, a 2-sigma rule would flag roughly 4-5% by chance alone, so 17 is about what random variation would give. Turbine 4 is flagged most often (3 times) and is the first one I would investigate.

## Assumptions

- Timestamps are treated as naive/UTC. No hour is missing around the 27 March UK clock change.
- Negative power is treated as invalid, as the brief says. No upper limits were applied; observed values are 9-15 m/s wind and 1.5-4.5 MW.
- The daily mean is the plain mean of the 15 turbine averages (each has 24 readings, so weighting makes no difference).
- **Sample vs population standard deviation:** the brief doesn't specify which. I used the sample version (`stddev_samp`) as the conservative default. It should be confirmed with the business analyst, because the 15 turbines are arguably the whole farm, which would favour the population version. The population version flags 20 turbine-days instead of 17. Changing it is a one-line edit.
- Exported CSVs are rounded to 4 decimals. The Delta tables keep full precision.

## Observation about the data

Power output is spread evenly between 1.5 and 4.5 MW and has almost no correlation with wind speed (r about 0), which is not what real turbines do. The data may be synthetic or the sensors mislabelled. Worth checking before trusting the flags operationally.

## Why PySpark

It matches the Databricks stack and scales if the data grows. It isn't strictly necessary for 15 turbines and one month of data.

## What I would do next

- Split the notebook into one task per layer and schedule it as a Databricks Job.
- Add automated tests (pytest) for the cleaning rules and the anomaly logic.
- Add data-quality expectations, such as upper-bound checks and alerts.
- Load files incrementally (append plus Auto Loader) instead of overwriting.
- Confirm the standard deviation choice with the analyst, then compare flags using a rolling per-turbine baseline.
- Add a small dashboard of daily averages with flags marked.

## AI tools

I used Claude as a mentor for guidance and review, and Databricks Genie to debug PySpark and find the right functions. I wrote and ran the notebook myself, and I can explain every line.
