from configparser import ConfigParser


class ConfigReader:

    def __init__(self, config_file):
        self.config_file = config_file
        self.config = ConfigParser()
        self.config.read(self.config_file)

    def get_user(self):
        return self.config['database']['user']

    def get_password(self):
        return self.config['database']['password']

    def get_host(self):
        return self.config['database']['host']

    def get_dns(self):
        return self.config['database']['dns']

    def get_url(self):
        return self.config['database']['url']

    def get_driver(self):
        return self.config['database']['driver']
