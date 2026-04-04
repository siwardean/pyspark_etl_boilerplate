# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 02 — ETL Exercise: Build the DreamAirlines Analytics Layer
# MAGIC
# MAGIC ## Context
# MAGIC
# MAGIC You are a data engineer at **DreamAirlines**. The analytics team needs a
# MAGIC consolidated revenue view that combines all four revenue streams — flights,
# MAGIC hotels, car rentals, and insurance — into a single queryable table.
# MAGIC
# MAGIC ## Source → Target Data Model
# MAGIC
# MAGIC ```
# MAGIC SOURCE (raw schema)                       TARGET (analytics schema)
# MAGIC ──────────────────────────────────────    ──────────────────────────────────────────
# MAGIC                                           analytics.booking_revenue_summary
# MAGIC raw.dim_travel_booking ──┐                ──────────────────────────────────────────
# MAGIC raw.dim_customer        ├──► transform    booking_date       DATE
# MAGIC raw.dim_travel_agency   │                 travel_agency      STRING
# MAGIC raw.dim_date            │                 destination        STRING
# MAGIC raw.fact_travel_booking ┤                 travel_class       STRING
# MAGIC raw.fact_hotel_booking  ┤                 customer_type      STRING
# MAGIC raw.fact_car_renting    ┤                 num_bookings       LONG
# MAGIC raw.fact_insurance      ┘                 travel_revenue     DOUBLE
# MAGIC                                           hotel_revenue      DOUBLE
# MAGIC                                           car_revenue        DOUBLE
# MAGIC                                           insurance_revenue  DOUBLE
# MAGIC                                           total_revenue      DOUBLE
# MAGIC                                           revenue_tier       STRING
# MAGIC ```
# MAGIC
# MAGIC ## Data contract (from notebook 01)
# MAGIC
# MAGIC | # | Rule |
# MAGIC |---|---|
# MAGIC | R1 | Join `dim_travel_booking` → `dim_date` on `Booking_Date = PK_Date` to get real dates |
# MAGIC | R2 | Join `dim_travel_booking` → `dim_customer` on `DSC_Customer` (name string, not PK) |
# MAGIC | R3 | Left-join all fact tables — a booking may not have hotel, car, or insurance |
# MAGIC | R4 | `COALESCE` all `AMT_` columns to `0` before aggregating |
# MAGIC | R5 | Drop `HashColumn`, `Load_ID`, `Insert_Date`, `Update_Date` from output |
# MAGIC | R6 | `revenue_tier`: `High ≥ 2000`, `Medium ≥ 500`, else `Low` |
# MAGIC
# MAGIC > **Pre-requisite:** Run `00_setup_data` first.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Load source tables
# MAGIC Already done — do not modify.

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
# MAGIC ## Step 2 — Join and enrich
# MAGIC
# MAGIC **TODO 1:** Build `enriched` by joining `dim_travel_booking` with:
# MAGIC - `dim_date` on `dim_travel_booking.Booking_Date == dim_date.PK_Date` (inner)
# MAGIC - `dim_customer` on `dim_travel_booking.DSC_Customer == dim_customer.DSC_Customer` (inner)
# MAGIC - `fact_travel` on `fact_travel.PK_TravelBooking == dim_travel_booking.PK_TravelBooking` (left)
# MAGIC - `fact_hotel` on `fact_hotel.FK_TravelBooking == dim_travel_booking.PK_TravelBooking` (left)
# MAGIC - `fact_car` on `fact_car.FK_TravelBooking == dim_travel_booking.PK_TravelBooking` (left)
# MAGIC - `fact_insurance` on `fact_insurance.FK_TravelBooking == dim_travel_booking.PK_TravelBooking` (left)
# MAGIC
# MAGIC Then apply R4: add four `COALESCE`-d amount columns named
# MAGIC `travel_amt`, `hotel_amt`, `car_amt`, `insurance_amt`.

# COMMAND ----------

# TODO 1: build enriched — join all tables and coalesce amounts
enriched = dim_travel_booking   # ← replace this line

# display(enriched.select("Booking_Date", "DSC_TravelAgency", "DSC_DestinyCountry",
#                         "travel_amt", "hotel_amt", "car_amt", "insurance_amt").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Aggregate
# MAGIC
# MAGIC **TODO 2:** Group `enriched` by:
# MAGIC `dim_date.Date`, `DSC_TravelAgency`, `DSC_DestinyCountry`, `DSC_TravelClass`, `DSC_CustomerType`
# MAGIC
# MAGIC Aggregate:
# MAGIC - `num_bookings`      = count of distinct `PK_TravelBooking`
# MAGIC - `travel_revenue`    = `ROUND( SUM(travel_amt),    2 )`
# MAGIC - `hotel_revenue`     = `ROUND( SUM(hotel_amt),     2 )`
# MAGIC - `car_revenue`       = `ROUND( SUM(car_amt),       2 )`
# MAGIC - `insurance_revenue` = `ROUND( SUM(insurance_amt), 2 )`

# COMMAND ----------

# TODO 2: aggregate by booking date, agency, destination, class, customer type
aggregated = enriched   # ← replace this line

# display(aggregated.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Compute total revenue and classify tier
# MAGIC
# MAGIC **TODO 3:** Add two columns:
# MAGIC - `total_revenue = ROUND(travel_revenue + hotel_revenue + car_revenue + insurance_revenue, 2)`
# MAGIC - `revenue_tier`:
# MAGIC   - `"High"`   if `total_revenue >= 2000`
# MAGIC   - `"Medium"` if `total_revenue >= 500`
# MAGIC   - `"Low"`    otherwise

# COMMAND ----------

# TODO 3: add total_revenue and revenue_tier
with_tier = aggregated   # ← replace this line

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Final select and rename
# MAGIC Already done — do not modify.

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
# MAGIC ## Step 6 — Write to analytics schema
# MAGIC
# MAGIC **TODO 4:** Write `final_df` to a Delta table named `analytics.booking_revenue_summary`
# MAGIC using mode `"overwrite"`.

# COMMAND ----------

# TODO 4: write final_df to analytics.booking_revenue_summary
# final_df.write ...

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Validate

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Uncomment after completing Step 6
# MAGIC -- SELECT COUNT(*) AS rows FROM analytics.booking_revenue_summary;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Total revenue by agency
# MAGIC -- SELECT travel_agency,
# MAGIC --        SUM(num_bookings)      AS total_bookings,
# MAGIC --        ROUND(SUM(total_revenue), 2) AS total_revenue
# MAGIC -- FROM analytics.booking_revenue_summary
# MAGIC -- GROUP BY travel_agency
# MAGIC -- ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Revenue breakdown by stream
# MAGIC -- SELECT
# MAGIC --     ROUND(SUM(travel_revenue),    2) AS flight_total,
# MAGIC --     ROUND(SUM(hotel_revenue),     2) AS hotel_total,
# MAGIC --     ROUND(SUM(car_revenue),       2) AS car_total,
# MAGIC --     ROUND(SUM(insurance_revenue), 2) AS insurance_total,
# MAGIC --     ROUND(SUM(total_revenue),     2) AS grand_total
# MAGIC -- FROM analytics.booking_revenue_summary;
