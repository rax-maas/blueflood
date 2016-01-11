from ConfigParser import SafeConfigParser
import os


class Config:

    parser = SafeConfigParser()
    parser.read(os.path.join(os.path.dirname(__file__), '..', 'config.ini'))

    def __init__(self, env):
        self.env = env

        print self.parser.sections()

    def __get_value(self, config_name):
        return Config.parser.get(self.env, config_name)

    def get_cassandra_nodes(self):
        return self.__get_value('cassandra_nodes').split(",")

    def get_es_nodes(self):
        return self.__get_value('es_nodes').split(",")

    @staticmethod
    def get_environments():
        return Config.parser.sections()
