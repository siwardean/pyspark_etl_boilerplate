# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 00 — Setup: Load DreamAirlines Data
# MAGIC
# MAGIC This notebook loads the **DreamAirlines** CSV source files into Delta tables
# MAGIC in the `raw` schema. Run it once before starting any exercise.
# MAGIC
# MAGIC ## Source Data Model
# MAGIC
# MAGIC DreamAirlines is a Portuguese travel agency operating from Lisbon, Porto and Faro.
# MAGIC The data warehouse covers bookings from **October 2015 to September 2018**.
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────────────┐
# MAGIC │                        DreamAirlines — Star Schema                          │
# MAGIC ├─────────────────────────┬───────────────────────────────────────────────────┤
# MAGIC │  DIMENSIONS             │  FACTS                                            │
# MAGIC ├─────────────────────────┼───────────────────────────────────────────────────┤
# MAGIC │  dim_customer    (99)   │  fact_travel_booking  (99)  AMT_Travel            │
# MAGIC │  dim_employee    (15)   │  fact_hotel_booking   (83)  AMT_Accomodation      │
# MAGIC │  dim_travel_agency (3)  │  fact_car_renting     (88)  AMT_Rental            │
# MAGIC │  dim_travel_booking(99) │                             AMT_DailyRate         │
# MAGIC │  dim_hotel_booking (83) │                             AMT_CarInsurance      │
# MAGIC │  dim_car_renting   (88) │  fact_insurance      (155)  AMT_Travel_Insurance  │
# MAGIC │  dim_hotel         (21) │                             AMT_Car_Insurance     │
# MAGIC │  dim_car           (25) │                                                   │
# MAGIC │  dim_insurance      (6) │                                                   │
# MAGIC │  dim_date       (10958) │                                                   │
# MAGIC └─────────────────────────┴───────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC Each fact table links to `dim_travel_booking` via `FK_TravelBooking`,
# MAGIC making the travel booking the central entity of the model.
# MAGIC
# MAGIC ## Column naming conventions
# MAGIC
# MAGIC | Prefix | Meaning | Example |
# MAGIC |---|---|---|
# MAGIC | `PK_` | Primary key (surrogate) | `PK_Customer` |
# MAGIC | `FK_` | Foreign key to another dimension | `FK_TravelBooking` |
# MAGIC | `DSC_` | Descriptive / label field | `DSC_Customer` |
# MAGIC | `COD_` | Code field | `COD_PostalCode` |
# MAGIC | `AMT_` | Monetary amount | `AMT_Travel` |
# MAGIC | `Load_ID` | ETL batch identifier | — |
# MAGIC | `HashColumn` | SHA-1 hash for change detection (SCD) | — |

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS raw;
# MAGIC CREATE SCHEMA IF NOT EXISTS analytics;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load CSV files into Delta tables

# COMMAND ----------

DATA_PATH = "/path/to/pyspark_etl_boilerplate/data/dreamAirlines_DW"  # adjust to your repo path

TABLE_MAP = {
    "dim_customer":       "dimCustomer.csv",
    "dim_employee":       "dimEmployee.csv",
    "dim_travel_agency":  "dimTravelAgency.csv",
    "dim_travel_booking": "dimTravelBooking.csv",
    "dim_hotel_booking":  "dimHotelBooking.csv",
    "dim_car_renting":    "dimCarRenting.csv",
    "dim_hotel":          "dimHotel.csv",
    "dim_car":            "dimCar.csv",
    "dim_insurance":      "dimInsurance.csv",
    "dim_date":           "dimDate.csv",
    "fact_travel_booking": "factTravelBooking.csv",
    "fact_hotel_booking":  "factHotelBooking.csv",
    "fact_car_renting":    "factCarRenting.csv",
    "fact_insurance":      "factInsurance.csv",
}

for table_name, csv_file in TABLE_MAP.items():
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .option("encoding", "UTF-8")
          .csv(f"{DATA_PATH}/{csv_file}"))

    (df.write
       .format("delta")
       .mode("overwrite")
       .saveAsTable(f"raw.{table_name}"))

    print(f"raw.{table_name:<25} → {df.count():>4} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_name, num_rows FROM (
# MAGIC     SELECT 'dim_customer'       AS table_name, COUNT(*) AS num_rows FROM raw.dim_customer       UNION ALL
# MAGIC     SELECT 'dim_employee',                     COUNT(*)             FROM raw.dim_employee       UNION ALL
# MAGIC     SELECT 'dim_travel_agency',                COUNT(*)             FROM raw.dim_travel_agency  UNION ALL
# MAGIC     SELECT 'dim_travel_booking',               COUNT(*)             FROM raw.dim_travel_booking UNION ALL
# MAGIC     SELECT 'dim_hotel_booking',                COUNT(*)             FROM raw.dim_hotel_booking  UNION ALL
# MAGIC     SELECT 'dim_car_renting',                  COUNT(*)             FROM raw.dim_car_renting    UNION ALL
# MAGIC     SELECT 'dim_hotel',                        COUNT(*)             FROM raw.dim_hotel          UNION ALL
# MAGIC     SELECT 'dim_car',                          COUNT(*)             FROM raw.dim_car            UNION ALL
# MAGIC     SELECT 'dim_insurance',                    COUNT(*)             FROM raw.dim_insurance      UNION ALL
# MAGIC     SELECT 'dim_date',                         COUNT(*)             FROM raw.dim_date           UNION ALL
# MAGIC     SELECT 'fact_travel_booking',              COUNT(*)             FROM raw.fact_travel_booking UNION ALL
# MAGIC     SELECT 'fact_hotel_booking',               COUNT(*)             FROM raw.fact_hotel_booking  UNION ALL
# MAGIC     SELECT 'fact_car_renting',                 COUNT(*)             FROM raw.fact_car_renting    UNION ALL
# MAGIC     SELECT 'fact_insurance',                   COUNT(*)             FROM raw.fact_insurance
# MAGIC ) ORDER BY table_name;
