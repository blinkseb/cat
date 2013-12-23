class Config(object):
    instance = None
    def __new__(clazz): 
        if clazz.instance is None:
            clazz.instance = object.__new__(clazz)
        return clazz.instance

    _config = None
    def set(self, config):
        self._config = config

    def get(self):
        return self._config

def get():
    return Config()
