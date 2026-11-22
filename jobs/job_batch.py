"""Bounded jobs: extract, transform, load, then stop."""

from abc import abstractmethod

from jobs.job import Job


class JobBatch(Job):
    """A job over a finite dataset.

    Implement ``extract()``, ``transform()`` and ``load()``. Leave the result
    of ``transform()`` in ``self.final_df`` so the row count can be tracked.
    """

    mode = "batch"
    track_record_count = True

    def __init__(self, config):
        super().__init__(config)
        self.final_df = None

    @abstractmethod
    def extract(self):
        """Read the sources."""

    @abstractmethod
    def transform(self):
        """Build ``self.final_df`` from the extracted data."""

    @abstractmethod
    def load(self):
        """Write ``self.final_df`` to its destination."""

    def count_records(self):
        """Rows produced by this run, or None when it cannot be told.

        Works for anything with a length (pandas DataFrame, list). Engines
        override it: ``SparkSessionMixin`` counts the DataFrame.
        """
        try:
            return len(self.final_df)
        except TypeError:
            return None

    def _execute(self):
        self._run_step(self.extract)
        self._run_step(self.transform)
        self._run_step(self.load)
        if self.track_record_count:
            records = self.count_records()
            if isinstance(records, int):
                self.logger.info("record_count=%d", records)
                self.log_metric("record_count", records)
