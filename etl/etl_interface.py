"""Deprecated name kept so older notebooks still import. Use ``jobs`` instead."""

import warnings

from jobs import JobBatch, SparkSessionMixin


class ETLInterface(SparkSessionMixin, JobBatch):
    def __init__(self, config):
        warnings.warn(
            "ETLInterface is deprecated: use `class MyJob(SparkSessionMixin, JobBatch)` from `jobs`",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(config)
