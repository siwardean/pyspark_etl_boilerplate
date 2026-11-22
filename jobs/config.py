"""YAML job configuration."""
import re
from pathlib import Path

import yaml

_MISSING = object()
_PLACEHOLDER = re.compile(r"\{(\w+)\}")


class ConfigError(Exception):
    """The configuration file is missing, unreadable or lacks a required key."""


class JobConfig:
    """Read-only view over a YAML config file (or a plain dict).

    Values are addressed as ``"section.key"`` and keep their YAML type.
    Scalars declared in ``PLACEHOLDER_SECTIONS`` can be referenced from any
    string value as ``{key}``::

        storage:
          bucket: s3://my-data-bucket
        job:
          target_path: "{bucket}/daily_revenue"   # -> s3://my-data-bucket/daily_revenue
    """

    PLACEHOLDER_SECTIONS = ("storage", "database")

    def __init__(self, source):
        if isinstance(source, dict):
            self.path = None
            self.data = source
        else:
            self.path = str(source)
            self.data = self._read(Path(source))
        self._placeholders = {
            key: value
            for section in self.PLACEHOLDER_SECTIONS
            for key, value in self.section(section).items()
            if not isinstance(value, (dict, list))
        }

    @staticmethod
    def _read(path):
        if not path.is_file():
            raise ConfigError(f"Config file not found: {path}")
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8"))
        except yaml.YAMLError as error:
            raise ConfigError(f"Invalid YAML in {path}: {error}") from error
        if not isinstance(data, dict):
            raise ConfigError(f"{path} must contain a mapping of sections")
        return data

    def get(self, key, default=None):
        """Return the value at ``section.key``, or ``default`` when it is absent."""
        value = self._lookup(self.data, key.split("."))
        return default if value is _MISSING else self._resolve(value)

    def require(self, key):
        """Return the value at ``section.key`` or fail with a clear error."""
        value = self.get(key, _MISSING)
        if value is _MISSING or value is None:
            raise ConfigError(f"Missing required config key '{key}' in {self.path or 'config'}")
        return value

    def section(self, name):
        """Return a whole section as a dict (empty when the section is absent)."""
        value = self.data.get(name)
        return dict(value) if isinstance(value, dict) else {}

    def _lookup(self, node, parts):
        # Engine settings such as ``spark.app.name`` are keys that contain dots,
        # so at each level the longest matching key wins.
        for size in range(len(parts), 0, -1):
            key = ".".join(parts[:size])
            if isinstance(node, dict) and key in node:
                if size == len(parts):
                    return node[key]
                found = self._lookup(node[key], parts[size:])
                if found is not _MISSING:
                    return found
        return _MISSING

    def _resolve(self, value):
        if isinstance(value, str):
            return _PLACEHOLDER.sub(
                lambda match: str(self._placeholders.get(match.group(1), match.group(0))), value
            )
        if isinstance(value, list):
            return [self._resolve(item) for item in value]
        if isinstance(value, dict):
            return {key: self._resolve(item) for key, item in value.items()}
        return value
