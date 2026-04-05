# DreamAirlines — Data Engineering Training

> This folder contains the hands-on training material for this repository.
> It is intentionally kept separate from the production ETL boilerplate so
> the two can evolve independently. Think of it as the flight simulator
> before you take the controls of the real aircraft.

---

```
Good morning, Agent.

Your target: DreamAirlines — a Portuguese travel agency running three offices
in Lisbon, Porto and Faro. Their data is scattered across 14 source tables
covering flights, hotels, car rentals and insurance. Nobody has ever joined
them together. The analytics team is flying blind.

This is your first data engineering assignment.
Your mission, should you choose to accept it, is to ingest the raw data,
expose its flaws, and deliver a clean, consolidated revenue pipeline that
the business can actually trust.

The source files are in data/dreamAirlines_DW/.
The Spark cluster is standing by.
This README will not self-destruct — but it will hold you accountable.

Good luck.
```

---

## What is this training about?

This is an applied illustration of the ETL boilerplate defined in this
repository. Every pattern you use here — `ETLInterface`, `ETLConfig`,
`[SPARK_CONTEXT]`, MLflow tracking — maps directly to a real file in `etl/`
or `utils/`. The exercises are designed to be your reference point the first
time you build a production job on top of this framework.

By the end you will have built a working daily ETL pipeline, profiled real
data quality issues, written the same transformation twice (SQL then DataFrame
API), and used MLflow to detect and recover from simulated pipeline failures.

---

## The data

DreamAirlines operates a classic star schema data warehouse covering
**October 2015 to September 2018**. The raw CSV files live in
`data/dreamAirlines_DW/` and are loaded into Delta tables by notebook `00`.

### Dimensions

| Table | Rows | What it describes |
|---|---|---|
| `raw.dim_travel_booking` | 99 | Core booking — origin, destination, class, dates |
| `raw.dim_customer` | 99 | Customer master — name, type, nationality |
| `raw.dim_employee` | 15 | Staff across the three agencies |
| `raw.dim_travel_agency` | 3 | Lisbon, Porto, Faro offices |
| `raw.dim_hotel` | 21 | Hotel properties by country |
| `raw.dim_car` | 25 | Vehicle inventory by rental company |
| `raw.dim_insurance` | 6 | Insurance products (Standard / Plus / Full / Basic) |
| `raw.dim_hotel_booking` | 83 | Hotel booking details |
| `raw.dim_car_renting` | 88 | Car rental details |
| `raw.dim_date` | 10 958 | Full calendar dimension 2000–2029 |

### Facts

| Table | Rows | Revenue column |
|---|---|---|
| `raw.fact_travel_booking` | 99 | `AMT_Travel` |
| `raw.fact_hotel_booking` | 83 | `AMT_Accomodation` |
| `raw.fact_car_renting` | 88 | `AMT_Rental`, `AMT_DailyRate` |
| `raw.fact_insurance` | 155 | `AMT_Travel_Insurance`, `AMT_Car_Insurance` |

---

## Target: what you will build

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

## The notebooks — run them in order

| # | Notebook | What you do | Share? |
|---|---|---|---|
| `00` | `00_setup_data` | Load all 14 CSV files into `raw.*` Delta tables | Yes |
| `01` | `01_data_exploration` | Profile the data — nulls, integrity, distributions | Yes |
| `02` | `02_etl_exercise` | Build the analytics table — 4 TODOs to complete | Yes |
| `03` | `03_etl_solution` | Full solution with design notes | Instructor only |
| `04` | `04_dataframe_api_exercise` | Rewrite a SQL job using the DataFrame API + MLflow gap tracker | Yes |

Notebooks `02` and `04` follow the exact class structure from the boilerplate:

```python
class YourETLJob(ETLInterface):
    def extract(self): ...      # read sources
    def transform(self): ...    # your work lives here
    def load(self): ...         # write output
    def run(self): ...          # MLflow wraps it all
```

You fill in `transform()`. Everything else is already wired up.

---

## Data quality issues waiting for you in notebook 01

Part of the job is finding these before you write a single transformation:

| # | Issue | Table | What to do |
|---|---|---|---|
| Q1 | `FK_TravelBooking` is nullable | `fact_car_renting` | LEFT JOIN + `COALESCE(AMT, 0)` |
| Q2 | All `AMT_` columns can be null | all facts | `COALESCE` before aggregating |
| Q3 | Dates are integers (`YYYYMMDD`) | all facts | Join via `dim_date.PK_Date` |
| Q4 | `dim_customer` joins on name string, not PK | dim tables | Join on `DSC_Customer` |
| Q5 | `HashColumn`, `Load_ID`, audit dates are ETL noise | all dims | Drop from analytics output |

---

## Notebook 04 — the MLflow tracker

Once your daily job is running, notebook `04` simulates two weeks of execution
with three injected failures, then shows how to query MLflow to find the gaps
and rerun only the missing dates. This pattern — log every run, query for gaps,
backfill — is the operational backbone of any reliable data pipeline.

---

## Configuration

The training notebooks use:

```
config/dev/training_notebook.properties
```

A copy of `example.properties` with `spark.app.name=training_notebook`.
Adjust `DATA_PATH` in `00_setup_data` to point to your local clone of the repo.

---

## Scala archive — `scala/`

The `scala/` folder holds the original Databricks exercises from the 2019
DreamAirlines training session. They are kept here as a reference and a
reminder of how far the tooling has come since then.

| File | Description |
|---|---|
| `exo_cert_siwar.scala` | Original certification exercise |
| `Exo_certif.scala` | Shared exercise from the training |

---

## Learning objectives

After completing all four notebooks you will be able to:

1. Load CSV sources into a Delta Lake raw layer
2. Profile data quality — nulls, duplicates, referential integrity
3. Build an analytics table by joining across a star schema
4. Apply `COALESCE`, LEFT JOINs, and aggregation with the DataFrame API
5. Explain why the DataFrame API is preferable to SQL for production ETL
6. Use MLflow to track daily runs, detect gaps, and trigger backfills
7. Package any transformation inside the `ETLInterface` structure for production deployment
