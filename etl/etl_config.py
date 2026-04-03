import configparser


class ETLConfig:
    def __init__(self, path):
        self.config = configparser.ConfigParser()
        self.config.read(path)

    def _resolve(self, value):
        if "COS" not in self.config:
            return value
        for key, cos_value in self.config["COS"].items():
            value = value.replace(f"{{{key}}}", cos_value)
        return value

    def get(self, key, fallback=None):
        for section in self.config.sections():
            if key in self.config[section]:
                return self._resolve(self.config[section][key])
        return fallback

    def get_spark_context(self):
        if "SPARK_CONTEXT" not in self.config:
            return {}
        return dict(self.config["SPARK_CONTEXT"])
