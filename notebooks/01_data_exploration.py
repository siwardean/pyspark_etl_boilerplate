# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 01 — Data Exploration & Profiling
# MAGIC ## DreamAirlines Source Data
# MAGIC
# MAGIC **Objective:** Understand the raw data before building the analytics pipeline.
# MAGIC A data engineer must always answer four questions first:
# MAGIC
# MAGIC 1. **What is the shape?** — row counts, schemas, key relationships
# MAGIC 2. **What is the quality?** — nulls, duplicates, encoding issues
# MAGIC 3. **What are the relationships?** — how do tables join, referential integrity
# MAGIC 4. **What does the business need?** — which fields feed the analytics layer
# MAGIC
# MAGIC > **Pre-requisite:** Run `00_setup_data` first.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Schema & Shape

# COMMAND ----------

# MAGIC %md
# MAGIC ### Core booking tables

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE raw.dim_travel_booking;

# COMMAND ----------

display(spark.table("raw.dim_travel_booking").limit(5))

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE raw.fact_travel_booking;

# COMMAND ----------

display(spark.table("raw.fact_travel_booking").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fact table summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     'fact_travel_booking' AS fact_table,
# MAGIC     COUNT(*)              AS rows,
# MAGIC     ROUND(MIN(AMT_Travel), 2)  AS min_amt,
# MAGIC     ROUND(MAX(AMT_Travel), 2)  AS max_amt,
# MAGIC     ROUND(AVG(AMT_Travel), 2)  AS avg_amt,
# MAGIC     ROUND(SUM(AMT_Travel), 2)  AS total_amt
# MAGIC FROM raw.fact_travel_booking
# MAGIC UNION ALL
# MAGIC SELECT 'fact_hotel_booking', COUNT(*),
# MAGIC        ROUND(MIN(AMT_Accomodation), 2), ROUND(MAX(AMT_Accomodation), 2),
# MAGIC        ROUND(AVG(AMT_Accomodation), 2), ROUND(SUM(AMT_Accomodation), 2)
# MAGIC FROM raw.fact_hotel_booking
# MAGIC UNION ALL
# MAGIC SELECT 'fact_car_renting', COUNT(*),
# MAGIC        ROUND(MIN(AMT_Rental), 2), ROUND(MAX(AMT_Rental), 2),
# MAGIC        ROUND(AVG(AMT_Rental), 2), ROUND(SUM(AMT_Rental), 2)
# MAGIC FROM raw.fact_car_renting
# MAGIC UNION ALL
# MAGIC SELECT 'fact_insurance', COUNT(*),
# MAGIC        ROUND(MIN(AMT_Travel_Insurance), 2), ROUND(MAX(AMT_Travel_Insurance), 2),
# MAGIC        ROUND(AVG(AMT_Travel_Insurance), 2), ROUND(SUM(AMT_Travel_Insurance + AMT_Car_Insurance), 2)
# MAGIC FROM raw.fact_insurance;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data Quality Assessment

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2a. Null analysis

# COMMAND ----------

from pyspark.sql import functions as F

def null_report(table_name):
    df    = spark.table(table_name)
    total = df.count()
    rows  = [
        {"column": c,
         "null_count": df.filter(F.col(c).isNull()).count(),
         "null_pct": round(df.filter(F.col(c).isNull()).count() / total * 100, 1)}
        for c in df.columns
    ]
    return (spark.createDataFrame(rows)
                 .filter(F.col("null_count") > 0)
                 .orderBy(F.col("null_count").desc()))

# COMMAND ----------

# MAGIC %md **dim_travel_booking — nulls**

# COMMAND ----------

display(null_report("raw.dim_travel_booking"))

# COMMAND ----------

# MAGIC %md **fact_car_renting — nulls**

# COMMAND ----------

display(null_report("raw.fact_car_renting"))

# COMMAND ----------

# MAGIC %md
# MAGIC > **Finding:** `FK_TravelBooking` in `fact_car_renting` can be null —
# MAGIC > some car rentals are standalone (not linked to a travel booking).
# MAGIC > These rows must be handled with a `LEFT JOIN` and `COALESCE` in the ETL.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. Referential integrity — do all FK values exist in their dimension?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- fact_travel_booking → dim_travel_booking
# MAGIC SELECT 'fact_travel_booking → dim_travel_booking' AS check_name,
# MAGIC        COUNT(*) AS orphan_rows
# MAGIC FROM raw.fact_travel_booking f
# MAGIC LEFT JOIN raw.dim_travel_booking d ON f.PK_TravelBooking = d.PK_TravelBooking
# MAGIC WHERE d.PK_TravelBooking IS NULL
# MAGIC UNION ALL
# MAGIC -- fact_hotel_booking → dim_travel_booking
# MAGIC SELECT 'fact_hotel_booking → dim_travel_booking',
# MAGIC        COUNT(*)
# MAGIC FROM raw.fact_hotel_booking f
# MAGIC LEFT JOIN raw.dim_travel_booking d ON f.FK_TravelBooking = d.PK_TravelBooking
# MAGIC WHERE d.PK_TravelBooking IS NULL
# MAGIC UNION ALL
# MAGIC -- fact_insurance → dim_travel_booking
# MAGIC SELECT 'fact_insurance → dim_travel_booking',
# MAGIC        COUNT(*)
# MAGIC FROM raw.fact_insurance f
# MAGIC LEFT JOIN raw.dim_travel_booking d ON f.FK_TravelBooking = d.PK_TravelBooking
# MAGIC WHERE d.PK_TravelBooking IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2c. Duplicate check — dim tables should have unique PKs

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'dim_travel_booking' AS dim_table,
# MAGIC        COUNT(*)             AS total_rows,
# MAGIC        COUNT(DISTINCT PK_TravelBooking) AS unique_pk,
# MAGIC        COUNT(*) - COUNT(DISTINCT PK_TravelBooking) AS duplicates
# MAGIC FROM raw.dim_travel_booking
# MAGIC UNION ALL
# MAGIC SELECT 'dim_customer', COUNT(*), COUNT(DISTINCT PK_Customer),
# MAGIC        COUNT(*) - COUNT(DISTINCT PK_Customer)
# MAGIC FROM raw.dim_customer
# MAGIC UNION ALL
# MAGIC SELECT 'dim_car', COUNT(*), COUNT(DISTINCT PK_Car),
# MAGIC        COUNT(*) - COUNT(DISTINCT PK_Car)
# MAGIC FROM raw.dim_car;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Business Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### Revenue by travel agency

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     d.DSC_TravelAgency                       AS agency,
# MAGIC     COUNT(DISTINCT f.PK_TravelBooking)       AS bookings,
# MAGIC     ROUND(SUM(f.AMT_Travel), 2)              AS travel_revenue
# MAGIC FROM raw.fact_travel_booking f
# MAGIC JOIN raw.dim_travel_booking d ON f.PK_TravelBooking = d.PK_TravelBooking
# MAGIC GROUP BY d.DSC_TravelAgency
# MAGIC ORDER BY travel_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top destination countries by number of bookings

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     DSC_DestinyCountry   AS destination,
# MAGIC     COUNT(*)             AS bookings,
# MAGIC     ROUND(AVG(Travel_EndDate - Travel_StartDate), 1) AS avg_stay_days
# MAGIC FROM raw.dim_travel_booking
# MAGIC GROUP BY DSC_DestinyCountry
# MAGIC ORDER BY bookings DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Booking volume by year and quarter

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     d.Year,
# MAGIC     d.QuarterName,
# MAGIC     COUNT(DISTINCT f.PK_TravelBooking) AS bookings,
# MAGIC     ROUND(SUM(f.AMT_Travel), 2)        AS travel_revenue
# MAGIC FROM raw.fact_travel_booking f
# MAGIC JOIN raw.dim_date d ON f.FK_date = d.PK_Date
# MAGIC GROUP BY d.Year, d.Quarter, d.QuarterName
# MAGIC ORDER BY d.Year, d.Quarter;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Travel class breakdown

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     DSC_TravelClass,
# MAGIC     COUNT(*)                               AS bookings,
# MAGIC     ROUND(AVG(f.AMT_Travel), 2)            AS avg_travel_amt
# MAGIC FROM raw.dim_travel_booking dtb
# MAGIC JOIN raw.fact_travel_booking f ON f.PK_TravelBooking = dtb.PK_TravelBooking
# MAGIC GROUP BY DSC_TravelClass
# MAGIC ORDER BY bookings DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer type — Individual vs Company

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     c.DSC_CustomerType                     AS customer_type,
# MAGIC     COUNT(DISTINCT f.PK_TravelBooking)     AS bookings,
# MAGIC     ROUND(SUM(f.AMT_Travel), 2)            AS travel_revenue,
# MAGIC     ROUND(AVG(f.AMT_Travel), 2)            AS avg_booking_value
# MAGIC FROM raw.fact_travel_booking f
# MAGIC JOIN raw.dim_travel_booking  dtb ON f.PK_TravelBooking   = dtb.PK_TravelBooking
# MAGIC JOIN raw.dim_customer        c   ON dtb.DSC_Customer      = c.DSC_Customer
# MAGIC GROUP BY c.DSC_CustomerType
# MAGIC ORDER BY travel_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data Contract Summary
# MAGIC
# MAGIC Before building the analytics layer, the data engineer documents every
# MAGIC decision made about data quality:
# MAGIC
# MAGIC | # | Finding | Table | Decision |
# MAGIC |---|---|---|---|
# MAGIC | Q1 | `FK_TravelBooking` nullable in `fact_car_renting` | fact_car_renting | Use `LEFT JOIN` + `COALESCE(AMT, 0)` |
# MAGIC | Q2 | Monetary columns may be null when no product was purchased | all facts | `COALESCE` all `AMT_` columns to `0` |
# MAGIC | Q3 | Dates stored as integers (`YYYYMMDD`) in fact tables | all facts | Join via `dim_date.PK_Date` to get real dates |
# MAGIC | Q4 | `DSC_Customer` is the join key (not `PK_Customer`) between dim_travel_booking and dim_customer | dim tables | Join on name string — verify no duplicates |
# MAGIC | Q5 | `HashColumn` and `Load_ID` are ETL audit fields, not analytics fields | all dims | Drop from analytics output |
