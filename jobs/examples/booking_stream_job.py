"""Spark Structured Streaming job: booking events enriched with their agency."""
from pyspark.sql import functions as F

from jobs import JobStream, SparkSessionMixin

BOOKING_SCHEMA = """
    PK_TravelBooking INT, FK_date INT, FK_TravelAgency INT, FK_Employee INT,
    FK_HotelBooking INT, FK_Customer INT, AMT_Travel DOUBLE
"""


class BookingStreamJob(SparkSessionMixin, JobStream):
    """Reads booking files as they land and writes one enriched row per booking.

    The sink uses ``availableNow``: it processes every file not yet seen, then
    stops. The checkpoint remembers what was processed, so running the job
    again only picks up new files.
    """

    def source(self):
        self.bookings = (
            self.spark.readStream
            .schema(BOOKING_SCHEMA)
            .option("header", "true")
            .option("pathGlobFilter", self.config.get("job.source_files", "*.csv"))
            .csv(self.config.require("job.source_path"))
        )
        # Static lookup table: joined to every micro-batch of the stream.
        self.agencies = (
            self.spark.read.option("header", "true").option("inferSchema", "true")
            .csv(self.config.require("job.agency_path"))
            .select("PK_TravelAgency", "DSC_TravelAgency")
        )

    def transform(self):
        self.stream_df = (
            self.bookings
            .filter(F.col("AMT_Travel").isNotNull())
            .join(self.agencies, F.col("FK_TravelAgency") == F.col("PK_TravelAgency"), "left")
            .select(
                F.col("PK_TravelBooking").alias("booking_id"),
                F.to_date(F.col("FK_date").cast("string"), "yyyyMMdd").alias("booking_date"),
                F.col("DSC_TravelAgency").alias("travel_agency"),
                F.col("AMT_Travel").alias("travel_revenue"),
            )
        )

    def sink(self):
        target = self.config.require("job.target_path")
        return (
            self.stream_df.writeStream
            .format("parquet")
            .outputMode("append")
            .option("checkpointLocation", self.config.get("job.checkpoint_path", f"{target}/_checkpoint"))
            .trigger(availableNow=True)
            .start(target)
        )
