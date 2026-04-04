# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 — ETL Solution: DreamAirlines Analytics Layer
# MAGIC
# MAGIC > **Instructor notebook — share only after trainees complete the exercise.**

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load source tables

# COMMAND ----------

dim_travel_booking = spark.table("raw.dim_travel_booking")
dim_customer       = spark.table("raw.dim_customer")
dim_date           = spark.table("raw.dim_date")
fact_travel        = spark.table("raw.fact_travel_booking")
fact_hotel         = spark.table("raw.fact_hotel_booking")
fact_car           = spark.table("raw.fact_car_renting")
fact_insurance     = spark.table("raw.fact_insurance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Join and enrich (solution)
# MAGIC
# MAGIC Key decisions:
# MAGIC - `dim_date` joined on `Booking_Date = PK_Date` (integer keys) → gives us a real `Date` column
# MAGIC - `dim_customer` joined on name string `DSC_Customer` — the dim tables don't share a PK
# MAGIC - All four facts are **left** joins — a booking may have no hotel, no car, no insurance
# MAGIC - `COALESCE` applied immediately so downstream aggregations never produce nulls

# COMMAND ----------

enriched = (
    dim_travel_booking
    .join(dim_date,
          dim_travel_booking["Booking_Date"] == dim_date["PK_Date"],
          "inner")
    .join(dim_customer,
          dim_travel_booking["DSC_Customer"] == dim_customer["DSC_Customer"],
          "inner")
    .join(fact_travel,
          fact_travel["PK_TravelBooking"] == dim_travel_booking["PK_TravelBooking"],
          "left")
    .join(fact_hotel,
          fact_hotel["FK_TravelBooking"] == dim_travel_booking["PK_TravelBooking"],
          "left")
    .join(fact_car,
          fact_car["FK_TravelBooking"] == dim_travel_booking["PK_TravelBooking"],
          "left")
    .join(fact_insurance,
          fact_insurance["FK_TravelBooking"] == dim_travel_booking["PK_TravelBooking"],
          "left")
    .withColumn("travel_amt",    F.coalesce(fact_travel["AMT_Travel"],             F.lit(0)))
    .withColumn("hotel_amt",     F.coalesce(fact_hotel["AMT_Accomodation"],        F.lit(0)))
    .withColumn("car_amt",       F.coalesce(fact_car["AMT_Rental"],                F.lit(0)))
    .withColumn("insurance_amt", F.coalesce(
        fact_insurance["AMT_Travel_Insurance"] + fact_insurance["AMT_Car_Insurance"],
        F.lit(0)))
)

display(enriched.select(
    dim_travel_booking["PK_TravelBooking"],
    dim_date["Date"],
    dim_travel_booking["DSC_TravelAgency"],
    dim_travel_booking["DSC_DestinyCountry"],
    "travel_amt", "hotel_amt", "car_amt", "insurance_amt"
).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Aggregate (solution)

# COMMAND ----------

aggregated = (
    enriched
    .groupBy(
        dim_date["Date"],
        dim_travel_booking["DSC_TravelAgency"],
        dim_travel_booking["DSC_DestinyCountry"],
        dim_travel_booking["DSC_TravelClass"],
        dim_customer["DSC_CustomerType"],
    )
    .agg(
        F.countDistinct(dim_travel_booking["PK_TravelBooking"]).alias("num_bookings"),
        F.round(F.sum("travel_amt"),    2).alias("travel_revenue"),
        F.round(F.sum("hotel_amt"),     2).alias("hotel_revenue"),
        F.round(F.sum("car_amt"),       2).alias("car_revenue"),
        F.round(F.sum("insurance_amt"), 2).alias("insurance_revenue"),
    )
)

display(aggregated.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Total revenue and tier (solution)

# COMMAND ----------

with_tier = (
    aggregated
    .withColumn("total_revenue",
                F.round(F.col("travel_revenue") + F.col("hotel_revenue")
                        + F.col("car_revenue")  + F.col("insurance_revenue"), 2))
    .withColumn("revenue_tier",
                F.when(F.col("total_revenue") >= 2000, "High")
                 .when(F.col("total_revenue") >= 500,  "Medium")
                 .otherwise("Low"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Final select and rename

# COMMAND ----------

final_df = (
    with_tier
    .withColumnRenamed("Date",               "booking_date")
    .withColumnRenamed("DSC_TravelAgency",   "travel_agency")
    .withColumnRenamed("DSC_DestinyCountry", "destination")
    .withColumnRenamed("DSC_TravelClass",    "travel_class")
    .withColumnRenamed("DSC_CustomerType",   "customer_type")
    .select("booking_date", "travel_agency", "destination", "travel_class",
            "customer_type", "num_bookings", "travel_revenue", "hotel_revenue",
            "car_revenue", "insurance_revenue", "total_revenue", "revenue_tier")
    .orderBy(F.col("booking_date"), F.col("total_revenue").desc())
)

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Write

# COMMAND ----------

(final_df.write
         .format("delta")
         .mode("overwrite")
         .saveAsTable("analytics.booking_revenue_summary"))

print(f"analytics.booking_revenue_summary → {final_df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Validate and reporting queries

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rows FROM analytics.booking_revenue_summary;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Revenue by agency
# MAGIC SELECT travel_agency,
# MAGIC        SUM(num_bookings)            AS total_bookings,
# MAGIC        ROUND(SUM(travel_revenue),    2) AS flight_rev,
# MAGIC        ROUND(SUM(hotel_revenue),     2) AS hotel_rev,
# MAGIC        ROUND(SUM(car_revenue),       2) AS car_rev,
# MAGIC        ROUND(SUM(insurance_revenue), 2) AS insurance_rev,
# MAGIC        ROUND(SUM(total_revenue),     2) AS total_rev
# MAGIC FROM analytics.booking_revenue_summary
# MAGIC GROUP BY travel_agency
# MAGIC ORDER BY total_rev DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Revenue by destination country
# MAGIC SELECT destination,
# MAGIC        SUM(num_bookings)           AS bookings,
# MAGIC        ROUND(SUM(total_revenue),2) AS total_revenue
# MAGIC FROM analytics.booking_revenue_summary
# MAGIC GROUP BY destination
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Revenue tier distribution
# MAGIC SELECT revenue_tier,
# MAGIC        COUNT(*)                    AS rows,
# MAGIC        ROUND(SUM(total_revenue),2) AS revenue
# MAGIC FROM analytics.booking_revenue_summary
# MAGIC GROUP BY revenue_tier
# MAGIC ORDER BY revenue DESC;
