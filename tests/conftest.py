import mlflow
import pytest


@pytest.fixture(autouse=True)
def mlflow_store(tmp_path, monkeypatch):
    """Send every test's MLflow runs to its own throwaway store."""
    uri = f"sqlite:///{tmp_path}/mlflow.db"
    monkeypatch.setenv("MLFLOW_TRACKING_URI", uri)
    monkeypatch.chdir(tmp_path)  # default artifact root is ./mlruns
    mlflow.set_tracking_uri(uri)
    yield
    while mlflow.active_run():
        mlflow.end_run()


@pytest.fixture
def last_run():
    """Return the most recent MLflow run of an experiment."""

    def _last_run(experiment):
        runs = mlflow.search_runs(experiment_names=[experiment], output_format="list")
        assert runs, f"no MLflow run recorded in experiment {experiment!r}"
        return runs[0]

    return _last_run
