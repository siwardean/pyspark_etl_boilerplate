"""PyFlink streaming job: running travel revenue per agency."""
import os

from jobs import FlinkSessionMixin, JobStream


class BookingFlinkJob(FlinkSessionMixin, JobStream):
    """Reads the booking file as a stream and keeps a running total per agency.

    The ``print`` sink shows the changelog Flink produces: ``+I`` for a new
    agency, then ``-U`` / ``+U`` pairs each time its total is updated.
    """

    def source(self):
        path = os.path.abspath(self.config.require("job.source_path"))
        self.table_env.execute_sql(f"""
            CREATE TEMPORARY TABLE bookings (
                PK_TravelBooking INT, FK_date INT, FK_TravelAgency INT, FK_Employee INT,
                FK_HotelBooking INT, FK_Customer INT, AMT_Travel DOUBLE
            ) WITH (
                'connector' = 'filesystem',
                'path' = 'file://{path}',
                'format' = 'csv',
                'csv.ignore-parse-errors' = 'true'   -- skips the header line
            )
        """)

    def transform(self):
        self.stream_df = self.table_env.sql_query("""
            SELECT FK_TravelAgency          AS travel_agency_id,
                   COUNT(*)                 AS num_bookings,
                   ROUND(SUM(AMT_Travel), 2) AS travel_revenue
            FROM bookings
            WHERE AMT_Travel IS NOT NULL
            GROUP BY FK_TravelAgency
        """)

    def sink(self):
        self.table_env.execute_sql("""
            CREATE TEMPORARY TABLE revenue_by_agency (
                travel_agency_id INT, num_bookings BIGINT, travel_revenue DOUBLE
            ) WITH ('connector' = 'print')
        """)
        return self.stream_df.execute_insert("revenue_by_agency")
