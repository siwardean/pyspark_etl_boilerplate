"""Unbounded jobs: source, transform, sink, then run until stopped."""
from abc import abstractmethod

from jobs.job import Job


class JobStream(Job):
    """A job over a stream of events.

    ``source()``, ``transform()`` and ``sink()`` only *describe* the pipeline.
    ``sink()`` starts it and returns the engine's handle (a Spark
    ``StreamingQuery``, a Flink ``TableResult``); the job then waits on that
    handle, reporting progress to MLflow, until the stream ends or
    ``job.stream_timeout_seconds`` elapses.
    """

    mode = "stream"

    def __init__(self, config):
        super().__init__(config)
        self.stream_df = None
        self.handle = None

    @abstractmethod
    def source(self):
        """Open the input stream."""

    @abstractmethod
    def transform(self):
        """Build ``self.stream_df`` from the input stream."""

    @abstractmethod
    def sink(self):
        """Start writing ``self.stream_df`` and return the running handle."""

    def _await_stream(self, handle, timeout=None):
        """Block until the stream ends. Provided by the session mixins."""
        raise NotImplementedError(
            f"{type(self).__name__} must use a session mixin or override _await_stream()"
        )

    def _execute(self):
        self._run_step(self.source)
        self._run_step(self.transform)
        self.handle = self._run_step(self.sink)
        self.logger.info("stream: running")
        self._await_stream(self.handle, self.config.get("job.stream_timeout_seconds"))
        self.logger.info("stream: stopped")
