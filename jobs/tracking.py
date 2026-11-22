"""MLflow tracking for job runs. This is the only module that talks to MLflow."""
import contextlib


class Tracker:
    """Records one job run in MLflow, or does nothing when tracking is disabled.

    Settings come from the ``mlflow`` config section:

    * ``enabled``      turn tracking off without touching the job (default true)
    * ``experiment``   experiment the runs are filed under
    * ``tracking_uri`` optional; normally left to ``MLFLOW_TRACKING_URI`` or to
      the platform (Databricks sets it for you)
    * ``tags``         extra tags added to every run
    """

    def __init__(self, config):
        self.enabled = bool(config.get("mlflow.enabled", True))
        self.experiment = config.get("mlflow.experiment")
        self.tracking_uri = config.get("mlflow.tracking_uri")
        self.extra_tags = config.get("mlflow.tags") or {}
        self.run_id = None
        self._client = None

    @contextlib.contextmanager
    def start_run(self, run_name, tags, params):
        """Open a run for the duration of the block; an exception marks it FAILED."""
        if not self.enabled:
            yield None
            return
        import mlflow

        if self.tracking_uri:
            mlflow.set_tracking_uri(self.tracking_uri)
        if self.experiment:
            mlflow.set_experiment(self.experiment)
        nested = mlflow.active_run() is not None
        with mlflow.start_run(run_name=run_name, nested=nested) as run:
            self.run_id = run.info.run_id
            # The client is addressed by run id, so metrics can also be logged
            # from other threads (streaming callbacks) without an active run.
            self._client = mlflow.MlflowClient()
            mlflow.set_tags({**self.extra_tags, **tags})
            mlflow.log_params(params)
            try:
                yield run
            finally:
                self._client = None

    @property
    def active(self):
        return self._client is not None

    def log_metric(self, key, value, step=None):
        if self.active:
            self._client.log_metric(self.run_id, key, float(value), step=step)

    def set_tag(self, key, value):
        if self.active:
            self._client.set_tag(self.run_id, key, str(value)[:5000])

    def log_file(self, path, artifact_dir=None):
        if self.active:
            self._client.log_artifact(self.run_id, path, artifact_dir)

    def log_dict(self, data, artifact_file):
        if self.active:
            self._client.log_dict(self.run_id, data, artifact_file)
