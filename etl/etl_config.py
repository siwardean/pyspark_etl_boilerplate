import configparser

class ETLConfig:
    def __init__(self, path):
        self.config = configparser.ConfigParser()
        self.config.read(path)

    def get(self, key, fallback=None):
        for section in self.config.sections():
            if key in self.config[section]:
                return self.config[section][key]
        return fallback
