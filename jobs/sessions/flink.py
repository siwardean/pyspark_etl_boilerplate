"""Flink table environment for jobs that need one."""


class FlinkSessionMixin:
    """Gives a job a lazily created ``self.table_env`` (PyFlink Table API).

    List it before the job class::

        class BookingFlinkJob(FlinkSessionMixin, JobStream): ...

    The environment runs in streaming or batch mode to match the job's mode.
    PyFlink is only imported when ``self.table_env`` is first used
    (``pip install -r requirements-flink.txt``).
    """

    engine = "flink"
    _table_env = None

    def _get_flink_conf(self):
        """Flink settings from the ``flink`` config section, as strings."""
        return {str(key): str(value) for key, value in self.config.section("flink").items()}

    def _get_table_env(self):
        """Build a TableEnvironment from the config."""
        from pyflink.common import Configuration
        from pyflink.table import EnvironmentSettings, TableEnvironment

        configuration = Configuration()
        for key, value in self._get_flink_conf().items():
            configuration.set_string(key, value)
        builder = EnvironmentSettings.new_instance().with_configuration(configuration)
        builder = builder.in_batch_mode() if self.mode == "batch" else builder.in_streaming_mode()
        return TableEnvironment.create(builder.build())

    @property
    def table_env(self):
        if self._table_env is None:
            self._table_env = self._get_table_env()
        return self._table_env

    @table_env.setter
    def table_env(self, table_env):
        self._table_env = table_env

    def sql(self, query):
        """Run a SQL query and return a Table. Same call as on the Spark mixin."""
        return self.table_env.sql_query(query)

    def _await_stream(self, table_result, timeout=None):
        """Wait for a TableResult returned by ``execute_insert`` / ``execute_sql``."""
        client = table_result.get_job_client()
        if client is not None:
            self.set_tag("flink_job_id", str(client.get_job_id()))
        if timeout is None:
            table_result.wait()
            return
        try:
            table_result.wait(int(float(timeout) * 1000))
        except Exception:
            self.logger.info("stream: timeout reached, cancelling the Flink job")
            if client is not None:
                client.cancel().result()
