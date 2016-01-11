from unittest import TestCase
import enum_metrics.config as cf


class TestConfig(TestCase):
    def test_get_config_value(self):
        config = cf.Config('localhost')

        self.assertEquals(config.get_cassandra_nodes()[0], 'localhost')
        self.assertEquals(config.get_es_nodes()[0], 'localhost')
