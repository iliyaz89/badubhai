import os


class TPConfiguration(object):
    def __init__(self):
        self.config = {}
        self.env_config = self._load_config_from_environment()
        self.config.update(self.env_config)

    def set(self, key, value):
        self.config[key] = value

    def get(self, key):
        if key in self.config:
            return self.config[key]
        else:
            return None;

    def _load_config_from_environment(self):
        env_config = {}
        print os.environ
        for key in os.environ:
            if key.startswith('TP20'):
                env_config[key] = os.environ[key]

        return env_config

    def _read_from_files(self):
        pass

    def _read_from_database(self):
        pass

    def __str__(self):
        return str(self.config)
