"""Logging for jobs: one console format, plus a per-run log file for MLflow."""
import contextlib
import logging
import os
import tempfile

ROOT_LOGGER = "jobs"
LOG_FORMAT = "%(asctime)s %(levelname)-7s [%(name)s] %(message)s"


def get_logger(job_name, level="INFO"):
    """Return the logger of a job, configuring console output on first use."""
    root = logging.getLogger(ROOT_LOGGER)
    if not root.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(LOG_FORMAT))
        root.addHandler(handler)
        root.propagate = False  # notebooks attach their own root handlers; avoid duplicates
    root.setLevel(str(level).upper())
    return root.getChild(job_name)


@contextlib.contextmanager
def capture_run_log():
    """Copy everything jobs log during the block into a file and yield its path."""
    directory = tempfile.mkdtemp(prefix="job-log-")
    path = os.path.join(directory, "run.log")
    handler = logging.FileHandler(path, encoding="utf-8")
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    root = logging.getLogger(ROOT_LOGGER)
    root.addHandler(handler)
    try:
        yield path
    finally:
        root.removeHandler(handler)
        handler.close()
