from ConfigParser import SafeConfigParser
import os


class Config:
    def __init__(self, env):
        self.env = env

        self.parser = SafeConfigParser()
        self.parser.read(os.path.join(os.path.dirname(__file__), '..', 'config.ini'))

        print self.parser.sections()

    def __get_value(self, config_name):
        return self.parser.get(self.env, config_name)

    def get_cassandra_nodes(self):
        return self.__get_value('cassandra_nodes').split(",")

    def get_es_nodes(self):
        return self.__get_value('es_nodes').split(",")
