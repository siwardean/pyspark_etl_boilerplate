"""Spark session for jobs that need one."""
from time import monotonic as _now


def _to_conf_value(value):
    return str(value).lower() if isinstance(value, bool) else str(value)


class SparkSessionMixin:
    """Gives a job a lazily created ``self.spark``.

    List it before the job class::

        class DailyRevenueJob(SparkSessionMixin, JobBatch): ...

    Nothing is imported or started until ``self.spark`` is first used, so the
    job can be built and its config inspected without Spark.
    """

    engine = "spark"
    stream_poll_seconds = 5
    _spark = None

    def _get_spark_conf(self):
        """Spark settings from the ``spark`` config section, as strings."""
        return {str(key): _to_conf_value(value) for key, value in self.config.section("spark").items()}

    def _get_spark_session(self):
        """Reuse the active session (notebooks, Databricks) or build one from the config."""
        from pyspark.sql import SparkSession

        conf = self._get_spark_conf()
        session = SparkSession.getActiveSession()
        if session is not None:
            # Cluster-level settings (master, jars, memory) are fixed once a
            # session exists; only the runtime ones can still be applied.
            for key, value in conf.items():
                try:
                    if session.conf.isModifiable(key):
                        session.conf.set(key, value)
                except Exception:  # not every runtime exposes every key
                    self.logger.debug("Spark setting %s not applied to the active session", key)
            return session

        builder = SparkSession.builder
        for key, value in conf.items():
            builder = builder.config(key, value)
        return builder.getOrCreate()

    @property
    def spark(self):
        if self._spark is None:
            self._spark = self._get_spark_session()
        return self._spark

    @spark.setter
    def spark(self, session):
        self._spark = session

    def sql(self, query):
        """Run a SQL query and return a DataFrame. Same call as on the Flink mixin."""
        return self.spark.sql(query)

    def count_records(self):
        """Rows in ``self.final_df``. This runs the query once more; set
        ``track_record_count = False`` on jobs where that is too expensive."""
        return None if self.final_df is None else self.final_df.count()

    def _await_stream(self, query, timeout=None):
        """Wait for a StreamingQuery, logging each micro-batch to MLflow."""
        deadline = None if timeout is None else _now() + float(timeout)
        last_batch, total_rows = -1, 0
        while True:
            wait = self.stream_poll_seconds
            if deadline is not None:
                wait = max(0.0, min(wait, deadline - _now()))
            finished = query.awaitTermination(wait)
            for progress in query.recentProgress:
                batch_id, rows = progress["batchId"], progress["numInputRows"]
                if batch_id > last_batch:
                    last_batch, total_rows = batch_id, total_rows + rows
                    self.logger.info("stream: batch %d processed %d rows", batch_id, rows)
                    self.log_metric("batch_input_rows", rows, step=batch_id)
            if finished:
                break
            if deadline is not None and _now() >= deadline:
                self.logger.info("stream: timeout reached, stopping the query")
                query.stop()
                break
        self.log_metric("record_count", total_rows)
        self.log_metric("batch_count", last_batch + 1)
