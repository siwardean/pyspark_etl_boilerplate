# RetailCo Data Engineering Exercise

A hands-on Databricks exercise demonstrating core data engineering concepts:
raw data ingestion, data profiling, data quality remediation, and building
a star schema data mart for a reporting pipeline.

---

## Scenario

You are a data engineer at **DreamAirlines**, a Portuguese travel agency
operating from Lisbon, Porto and Faro. The analytics team needs a consolidated
revenue view combining flights, hotels, car rentals and insurance into a single
queryable table to power BI dashboards.

Your job is to load the source CSV files, explore and profile the data,
and build an analytics layer on top of the existing star schema.

---

## Data Sources (Raw Layer)

CSV files in `data/dreamAirlines_DW/` loaded by `00_setup_data` into Delta tables.

| Table | Rows | Description |
|---|---|---|
| `raw.dim_travel_booking` | 99 | Core booking entity — origin, destination, class, dates |
| `raw.dim_customer` | 99 | Customer master — name, type, nationality, city |
| `raw.dim_employee` | 15 | Staff across the three agencies |
| `raw.dim_travel_agency` | 3 | Lisbon, Porto, Faro offices |
| `raw.dim_hotel` | 21 | Hotel properties by country |
| `raw.dim_car` | 25 | Vehicle inventory by rental company |
| `raw.dim_insurance` | 6 | Insurance products (Standard / Plus / Full / Basic) |
| `raw.dim_hotel_booking` | 83 | Hotel booking details linked to travel bookings |
| `raw.dim_car_renting` | 88 | Car rental details linked to travel bookings |
| `raw.dim_date` | 10 958 | Full calendar dimension 2000–2029 |
| `raw.fact_travel_booking` | 99 | Flight revenue — `AMT_Travel` |
| `raw.fact_hotel_booking` | 83 | Hotel revenue — `AMT_Accomodation` |
| `raw.fact_car_renting` | 88 | Car rental revenue — `AMT_Rental`, `AMT_DailyRate` |
| `raw.fact_insurance` | 155 | Insurance revenue — `AMT_Travel_Insurance`, `AMT_Car_Insurance` |

---

## Target Analytics Model

```
SOURCE (raw schema)                        TARGET (analytics schema)
───────────────────────────────────────    ──────────────────────────────────────────
raw.dim_travel_booking ──┐                 analytics.booking_revenue_summary
raw.dim_customer        ├──► ETL job  ──►  ──────────────────────────────────────────
raw.dim_date            │                  booking_date       DATE
raw.fact_travel_booking ┤                  travel_agency      STRING
raw.fact_hotel_booking  ┤                  destination        STRING
raw.fact_car_renting    ┤                  travel_class       STRING
raw.fact_insurance      ┘                  customer_type      STRING
                                           num_bookings       LONG
                                           travel_revenue     DOUBLE
                                           hotel_revenue      DOUBLE
                                           car_revenue        DOUBLE
                                           insurance_revenue  DOUBLE
                                           total_revenue      DOUBLE
                                           revenue_tier       STRING  (High/Medium/Low)
```

---

## Notebooks

Run them in order:

| # | Notebook | Role | Share with students? |
|---|---|---|---|
| 00 | `00_setup_data` | Generates synthetic raw Delta tables | Yes |
| 01 | `01_data_exploration` | Guided profiling — nulls, duplicates, outliers, RI | Yes |
| 02 | `02_etl_exercise` | Exercise — TODOs for students to complete | Yes |
| 03 | `03_etl_solution` | Complete solution with inline explanations | Instructor only |
| 04 | `04_dataframe_api_exercise` | 30-min exercise: rewrite a complex SQL query using the DataFrame API | Yes |

---

## Data Quality Issues to Discover

| # | Issue | Table | Decision |
|---|---|---|---|
| Q1 | `FK_TravelBooking` nullable in `fact_car_renting` | fact_car_renting | Use LEFT JOIN + COALESCE(AMT, 0) |
| Q2 | All AMT_ columns may be null (no product purchased) | all facts | COALESCE all amounts to 0 |
| Q3 | Dates stored as integers (YYYYMMDD) in fact tables | all facts | Join via dim_date.PK_Date |
| Q4 | dim_travel_booking joins dim_customer on name string | dim tables | Join on DSC_Customer, not PK |
| Q5 | HashColumn, Load_ID, Insert_Date are ETL audit fields | all dims | Drop from analytics output |

---

## Learning Objectives

After completing this exercise you will be able to:

1. **Profile raw data** — identify nulls, duplicates, outliers, and referential integrity breaks
2. **Apply data quality rules** — make and document remediation decisions
3. **Build a date dimension** — generate a calendar table programmatically with PySpark
4. **Apply surrogate keys** — use `row_number()` windows for deterministic integer keys
5. **Design a star schema** — separate measures (fact) from context (dimensions)
6. **Compute financial metrics** — chain column expressions for revenue, discount, margin
7. **Validate a data mart** — referential integrity checks and financial consistency checks
8. **Write reporting queries** — join across a star schema for BI-ready aggregations

---

## Reporting Use Cases Enabled by the Mart

Once built, the mart directly answers:

- Monthly and quarterly revenue trends
- Margin analysis by product category and subcategory
- Customer segment contribution (Premium / Standard / Basic)
- Country-level revenue breakdown
- Weekend vs weekday purchasing patterns
- Top customers by net revenue
- Brand performance within categories
