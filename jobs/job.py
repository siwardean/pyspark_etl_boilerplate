"""Base class shared by every job, whatever its mode or engine."""
import time
from abc import ABC, abstractmethod

from jobs.config import JobConfig
from jobs.logs import capture_run_log, get_logger
from jobs.tracking import Tracker


class Job(ABC):
    """A configured, logged and MLflow-tracked unit of work.

    Subclasses describe *what* runs by implementing ``_execute()`` (normally
    through ``JobBatch`` or ``JobStream``); ``run()`` owns everything around
    it: the MLflow run, parameters, timings, the log file and failure status.
    A job never needs to import ``mlflow`` or configure ``logging``.

    The base class has no engine. Add ``SparkSessionMixin`` or
    ``FlinkSessionMixin`` to a job that needs a session.
    """

    mode = "job"
    engine = "python"

    def __init__(self, config):
        self.config = config if isinstance(config, JobConfig) else JobConfig(config)
        self.config_path = self.config.path
        self.name = self.config.get("job.name") or type(self).__name__
        self.logger = get_logger(self.name, self.config.get("logging.level", "INFO"))
        self.params = {}
        self._tracker = Tracker(self.config)

    @abstractmethod
    def _execute(self):
        """Run the job's steps. Implemented by JobBatch and JobStream."""

    def run(self, **params):
        """Execute the job once. Keyword arguments become ``self.params``.

        ``job.run(run_date="2016-03-15")`` logs ``run_date`` as an MLflow
        parameter and makes it available to the steps as
        ``self.params["run_date"]``.
        """
        self.params = params
        tags = {"job": self.name, "mode": self.mode, "engine": self.engine}
        run_params = {"env": self.config.get("job.env"), **params}

        with capture_run_log() as log_path:
            with self._tracker.start_run(self._run_name(), tags, run_params):
                self._log_config()
                self.logger.info("Run started (run_id=%s, params=%s)", self._tracker.run_id, params)
                started = time.monotonic()
                try:
                    self._execute()
                    self.logger.info("Run finished in %.1fs", time.monotonic() - started)
                except Exception as error:
                    self.logger.exception("Run failed after %.1fs", time.monotonic() - started)
                    self.set_tag("error", f"{type(error).__name__}: {error}")
                    raise
                finally:
                    self.log_metric("duration_seconds", time.monotonic() - started)
                    self._tracker.log_file(log_path, "logs")
        return self

    def log_metric(self, key, value, step=None):
        """Record a metric on the current run. The tracking call jobs use."""
        self._tracker.log_metric(key, value, step)

    def set_tag(self, key, value):
        """Attach a tag to the current run."""
        self._tracker.set_tag(key, value)

    def _run_step(self, step):
        """Call one step method, logging its duration."""
        self.logger.info("%s: started", step.__name__)
        started = time.monotonic()
        result = step()
        elapsed = time.monotonic() - started
        self.logger.info("%s: done in %.1fs", step.__name__, elapsed)
        self.log_metric(f"{step.__name__}_seconds", elapsed)
        return result

    def _run_name(self):
        suffix = "-".join(str(value) for value in self.params.values())
        return f"{self.name}-{suffix}" if suffix else self.name

    def _log_config(self):
        if self.config_path:
            self._tracker.log_file(self.config_path, "config")
        else:
            self._tracker.log_dict(self.config.data, "config/config.yaml")
