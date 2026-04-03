import configparser


class ETLConfig:
    PLACEHOLDER_SECTIONS = ("COS", "DATABASE")

    def __init__(self, path):
        self.config = configparser.ConfigParser()
        self.config.read(path)

    def _resolve(self, value):
        for section in self.PLACEHOLDER_SECTIONS:
            if section in self.config:
                for key, section_value in self.config[section].items():
                    value = value.replace(f"{{{key}}}", section_value)
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
