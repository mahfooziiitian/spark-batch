from jproperties import Properties


class PropertiesHandler:
    def __init__(self, file_path):
        self.file_path = file_path

    def read_properties(self):
        with open(self.file_path, 'rb') as f:
            p = Properties()
            p.load(f, 'utf-8')
        return p

    def write_properties(self, properties):
        with open(self.file_path, 'wb') as f:
            properties.store(f, encoding='utf-8')

