"""Base class shared by every job, whatever its mode or engine."""
import time
from abc import ABC, abstractmethod

from jobs.config import JobConfig
from jobs.logs import capture_run_log, get_logger
from jobs.settings import JobParams, JobSettings, LoggingSettings, MlflowSettings
from jobs.tracking import Tracker


class Job(ABC):
    """A configured, logged and MLflow-tracked unit of work.

    Subclasses describe *what* runs by implementing ``_execute()`` (normally
    through ``JobBatch`` or ``JobStream``); ``run()`` owns everything around
    it: the MLflow run, parameters, timings, the log file and failure status.
    A job never needs to import ``mlflow`` or configure ``logging``.

    The base class has no engine. Add ``SparkSessionMixin`` or
    ``FlinkSessionMixin`` to a job that needs a session.

    Attributes available inside the steps:

    * ``self.settings``  the validated ``job`` config section (``Settings`` model)
    * ``self.params``    the validated run parameters (``Params`` model)
    * ``self.config``    the whole config, for other sections
    * ``self.logger``    a standard logger; its output is stored with the run
    """

    #: Override with a ``JobSettings`` subclass to declare the job's config keys.
    Settings = JobSettings
    #: Override with a ``JobParams`` subclass to declare the job's run parameters.
    Params = JobParams

    mode = "job"
    engine = "python"

    def __init__(self, config):
        """Create the job from a YAML path, a dict or a ``JobConfig``.

        The config is validated here, so a missing or mistyped key fails
        immediately, before any session is started.
        """
        self.config = config if isinstance(config, JobConfig) else JobConfig(config)
        self.config_path = self.config.path
        self.settings = self.config.load("job", self.Settings)
        self.params = self.Params.model_construct()
        self.name = self.settings.name or type(self).__name__
        self.logger = get_logger(self.name, self.config.load("logging", LoggingSettings).level)
        self._tracker = Tracker(self.config.load("mlflow", MlflowSettings))

    @abstractmethod
    def _execute(self):
        """Run the job's steps. Implemented by JobBatch and JobStream."""

    def run(self, **params):
        """Execute the job once and record the run. Returns the job.

        Keyword arguments are validated into ``self.params`` and logged as
        MLflow parameters: after ``job.run(run_date="2016-03-15")`` the steps
        read ``self.params.run_date``. If a step raises, the run is marked
        ``FAILED``, the traceback is kept in its log, and the error is re-raised.
        """
        self.params = self.Params(**params)
        tags = {"job": self.name, "mode": self.mode, "engine": self.engine}
        run_params = {"env": self.settings.env, **self.params.model_dump()}

        with capture_run_log() as log_path:
            with self._tracker.start_run(self._run_name(), tags, run_params):
                self._log_config()
                self.logger.info("Run started (run_id=%s, params=%s)", self._tracker.run_id, run_params)
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

    @property
    def run_id(self):
        """MLflow id of this job's latest run (None when tracking is disabled)."""
        return self._tracker.run_id

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
        suffix = "-".join(str(value) for value in self.params.model_dump().values())
        return f"{self.name}-{suffix}" if suffix else self.name

    def _log_config(self):
        if self.config_path:
            self._tracker.log_file(self.config_path, "config")
        else:
            self._tracker.log_dict(self.config.data, "config/config.yaml")
