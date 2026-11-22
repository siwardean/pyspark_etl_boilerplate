"""One streaming job, two engines: Spark Structured Streaming and Flink.

``BookingStreamJob`` holds what does not depend on the engine: the settings
and the transformation, written in SQL that both engines understand. Each
engine then adds a thin subclass with its own ``source()`` and ``sink()``,
because connectors are engine-specific.

Run either one with ``--job``::

    python main.py --job jobs.examples.booking_stream_job.BookingSparkStreamJob --conf config/dev/example_stream.yaml
    python main.py --job jobs.examples.booking_stream_job.BookingFlinkStreamJob --conf config/dev/example_stream.yaml
"""
import os

from jobs import FlinkSessionMixin, JobSettings, JobStream, SparkSessionMixin

BOOKING_COLUMNS = """
    PK_TravelBooking INT, FK_date INT, FK_TravelAgency INT, FK_Employee INT,
    FK_HotelBooking INT, FK_Customer INT, AMT_Travel DOUBLE
"""

CLEAN_BOOKINGS = """
    SELECT PK_TravelBooking                              AS booking_id,
           TO_DATE(CAST(FK_date AS STRING), 'yyyyMMdd')  AS booking_date,
           FK_TravelAgency                               AS travel_agency_id,
           AMT_Travel                                    AS travel_revenue
    FROM bookings
    WHERE AMT_Travel IS NOT NULL
"""


class BookingStreamJob(JobStream):
    """Engine-neutral part: reads the ``bookings`` stream, keeps the valid rows."""

    class Settings(JobSettings):
        source_path: str   # landing directory of booking CSV files
        target_path: str   # output directory

    def transform(self):
        # self.sql() exists on both session mixins, so this line runs on either engine.
        self.stream_df = self.sql(CLEAN_BOOKINGS)


class BookingSparkStreamJob(SparkSessionMixin, BookingStreamJob):
    """Spark version. The checkpoint makes a rerun process new files only."""

    def source(self):
        bookings = (
            self.spark.readStream.schema(BOOKING_COLUMNS).option("header", "true")
            .csv(self.settings.source_path)
        )
        bookings.createOrReplaceTempView("bookings")

    def sink(self):
        target = self.settings.target_path
        return (
            self.stream_df.writeStream
            .format("parquet")
            .outputMode("append")
            .option("checkpointLocation", f"{target}/_checkpoint")
            .trigger(availableNow=True)   # process what is there, then stop
            .start(target)
        )


class BookingFlinkStreamJob(FlinkSessionMixin, BookingStreamJob):
    """Flink version. A file source is bounded, so the job ends by itself."""

    def source(self):
        self.table_env.execute_sql(f"""
            CREATE TEMPORARY TABLE bookings ({BOOKING_COLUMNS}) WITH (
                'connector' = 'filesystem',
                'path' = 'file://{os.path.abspath(self.settings.source_path)}',
                'format' = 'csv',
                'csv.ignore-parse-errors' = 'true'   -- skips the header line
            )
        """)

    def sink(self):
        self.table_env.execute_sql(f"""
            CREATE TEMPORARY TABLE clean_bookings (
                booking_id INT, booking_date DATE, travel_agency_id INT, travel_revenue DOUBLE
            ) WITH (
                'connector' = 'filesystem',
                'path' = 'file://{os.path.abspath(self.settings.target_path)}',
                'format' = 'csv'
            )
        """)
        return self.stream_df.execute_insert("clean_bookings")
