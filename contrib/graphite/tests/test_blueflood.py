from unittest import TestCase
import requests
import requests_mock
from graphite_api import storage
from graphite_api.node import BranchNode
import logging.config
import os
import blueflood as bf

logging_file = os.path.join(os.path.dirname(__file__), 'logging.ini')
logging.config.fileConfig(logging_file)


class TestBlueFlood(TestCase):
    def setUp(self):
        self.dummy_tenant = 99999
        self.dummy_bf_url = 'http://dummybf.com'

        config = {'blueflood': {'tenant': self.dummy_tenant, 'urls': [self.dummy_bf_url]}}
        self.bff = bf.TenantBluefloodFinder(config)

    def test_resolution_full(self):
        self.assertEquals('FULL', bf.calc_res(0, 60))

    def test_resolution_5min(self):
        self.assertEquals('MIN5', bf.calc_res(0, 401 * 60))

    def test_resolution_20min(self):
        self.assertEquals('MIN20', bf.calc_res(0, 801 * 5 * 60))

    def test_resolution_60min(self):
        self.assertEquals('MIN60', bf.calc_res(0, 801 * 20 * 60))

    def test_resolution_240min(self):
        self.assertEquals('MIN240', bf.calc_res(0, 801 * 60 * 60))

    def test_resolution_14400min(self):
        self.assertEquals('MIN1440', bf.calc_res(0, 801 * 240 * 60))

    def test_find_metrics_endpoint(self):
        config = {'blueflood': {'urls': ['xxx']}}

        bff = bf.TenantBluefloodFinder(config)
        self.assertEquals('localhost/v2.0/836986/metrics/search?include_enum_values=true',
                          bff.find_metrics_endpoint('localhost', 836986))

    def test_complete_metric_name(self):
        self.assertTrue(self.bff.complete('a.b.c.d', 4))

    def test_non_complete_metric_name(self):
        self.assertFalse(self.bff.complete('a.b.c.d', 3))

    def test_find_nodes_all(self):
        prefix = '*'
        bf_mock_response = [{"metric": "one.two.three00.fourA.five100", "enum_values": ["ev1-1", "ev2-1"]},
                            {"metric": "one.two.three00.fourD"},
                            {"metric": "one.two.three00.fourC.five0"},
                            {"metric": "one.two.three00.fourE", "enum_values": ["ev1", "ev2"]},
                            {"metric": "one.two.three00.fourC.five2"},
                            {"metric": "one.two.three00", "enum_values": ["fourA", "fourB"]},
                            {"metric": "one.two.three00.fourC.five1"},
                            {"metric": "foo1.bar2", "enum_values": ["ev1", "ev2"]},
                            {"metric": "one.two.three00.fourA"},
                            {"metric": "one.two.three00.fourB.five2"},
                            {"metric": "one.two.three00.fourD.five100", "enum_values": ["ev1-2", "ev2-2"]},
                            {"metric": "one.two.three00.fourB.five0"},
                            {"metric": "one.two.three00.fourB.five1"},
                            {"metric": "one.two.three00.fourA.five2"},
                            {"metric": "one.two.three00.fourA.five1"},
                            {"metric": "one.two.three00.fourA.five0"}]
        bf_enum_mock_response = []

        # True indicates leaf node
        expected_branch_nodes = {'one',
                                 'foo1'}
        expected_leaf_nodes = set()

        self.verify_find_nodes_call(prefix, expected_branch_nodes, expected_leaf_nodes,
                                    self.bff, bf_mock_response, bf_enum_mock_response)

    def test_find_nodes_non_existent_metric_name(self):
        prefix = 'xxx.yyy.zzz.*'
        bf_mock_response = []
        bf_enum_mock_response = []

        # True indicates leaf node
        expected_branch_nodes = set()
        expected_leaf_nodes = set()

        self.verify_find_nodes_call(prefix, expected_branch_nodes, expected_leaf_nodes,
                                    self.bff, bf_mock_response, bf_enum_mock_response)

    def test_find_nodes_simple_prefix(self):
        prefix = 'one.two.*'
        bf_mock_response = [{"metric": "one.two.three00.fourA.five100", "enum_values": ["ev1-1", "ev2-1"]},
                            {"metric": "one.two.three00.fourD"},
                            {"metric": "one.two.three00.fourC.five0"},
                            {"metric": "one.two.three00.fourE", "enum_values": ["ev1", "ev2"]},
                            {"metric": "one.two.three00.fourC.five2"},
                            {"metric": "one.two.three00", "enum_values": ["fourA", "fourB"]},
                            {"metric": "one.two.three00.fourC.five1"},
                            {"metric": "one.two.three00.fourA"},
                            {"metric": "one.two.three00.fourB.five2"},
                            {"metric": "one.two.three00.fourD.five100", "enum_values": ["ev1-2", "ev2-2"]},
                            {"metric": "one.two.three00.fourB.five0"},
                            {"metric": "one.two.three00.fourB.five1"},
                            {"metric": "one.two.three00.fourA.five2"},
                            {"metric": "one.two.three00.fourA.five1"},
                            {"metric": "one.two.three00.fourA.five0"}]

        bf_enum_mock_response = [{"metric": "one.two.three00.fourA.five100", "enum_values": ["ev1-1", "ev2-1"]},
                                 {"metric": "one.two.three00.fourD"}, {"metric": "one.two.three00.fourC.five0"},
                                 {"metric": "one.two.three00.fourE", "enum_values": ["ev1", "ev2"]},
                                 {"metric": "one.two.three00.fourC.five2"},
                                 {"metric": "one.two.three00", "enum_values": ["fourA", "fourB"]},
                                 {"metric": "one.two.three00.fourC.five1"}, {"metric": "one.two.three00.fourA"},
                                 {"metric": "one.two.three00.fourB.five2"},
                                 {"metric": "one.two.three00.fourD.five100", "enum_values": ["ev1-2", "ev2-2"]},
                                 {"metric": "one.two.three00.fourB.five0"}, {"metric": "one.two.three00.fourB.five1"},
                                 {"metric": "one.two.three00.fourA.five2"}, {"metric": "one.two.three00.fourA.five1"},
                                 {"metric": "one.two.three00.fourA.five0"}]

        # True indicates leaf node
        expected_branch_nodes = {'one.two.three00'}
        expected_leaf_nodes = set()

        self.verify_find_nodes_call(prefix, expected_branch_nodes, expected_leaf_nodes,
                                    self.bff, bf_mock_response, bf_enum_mock_response)

    def test_find_nodes_last_node(self):
        prefix = 'one.two.three00.fourB.*'
        bf_mock_response = [{"metric": "one.two.three00.fourB.five2"},
                            {"metric": "one.two.three00.fourB.five0"},
                            {"metric": "one.two.three00.fourB.five1"}]

        bf_enum_mock_response = [{"metric": "one.two.three00.fourB.five2"},
                                 {"metric": "one.two.three00.fourB.five0"},
                                 {"metric": "one.two.three00.fourB.five1"}]
        # True indicates leaf node
        expected_branch_nodes = set()
        expected_leaf_nodes = {'one.two.three00.fourB.five0',
                               'one.two.three00.fourB.five1',
                               'one.two.three00.fourB.five2'}

        self.verify_find_nodes_call(prefix, expected_branch_nodes, expected_leaf_nodes,
                                    self.bff, bf_mock_response, bf_enum_mock_response)

    def test_find_nodes_partial_complete_metric_name(self):
        prefix = 'one.two.three00.four*'
        bf_mock_response = [{"metric": "one.two.three00.fourA.five100", "enum_values": ["ev1-1", "ev2-1"]},
                            {"metric": "one.two.three00.fourD"},
                            {"metric": "one.two.three00.fourC.five0"},
                            {"metric": "one.two.three00.fourE", "enum_values": ["ev1", "ev2"]},
                            {"metric": "one.two.three00.fourC.five2"},
                            {"metric": "one.two.three00.fourC.five1"},
                            {"metric": "one.two.three00.fourA"},
                            {"metric": "one.two.three00.fourB.five2"},
                            {"metric": "one.two.three00.fourD.five100", "enum_values": ["ev1-2", "ev2-2"]},
                            {"metric": "one.two.three00.fourB.five0"},
                            {"metric": "one.two.three00.fourB.five1"},
                            {"metric": "one.two.three00.fourA.five2"},
                            {"metric": "one.two.three00.fourA.five1"},
                            {"metric": "one.two.three00.fourA.five0"}]

        bf_enum_mock_response = [{"metric": "one.two.three00.fourA.five100", "enum_values": ["ev1-1", "ev2-1"]},
                                 {"metric": "one.two.three00.fourD"}, {"metric": "one.two.three00.fourC.five0"},
                                 {"metric": "one.two.three00.fourE", "enum_values": ["ev1", "ev2"]},
                                 {"metric": "one.two.three00.fourC.five2"},
                                 {"metric": "one.two.three00", "enum_values": ["fourA", "fourB"]},
                                 {"metric": "one.two.three00.fourC.five1"}, {"metric": "one.two.three00.fourA"},
                                 {"metric": "one.two.three00.fourB.five2"},
                                 {"metric": "one.two.three00.fourD.five100", "enum_values": ["ev1-2", "ev2-2"]},
                                 {"metric": "one.two.three00.fourB.five0"}, {"metric": "one.two.three00.fourB.five1"},
                                 {"metric": "one.two.three00.fourA.five2"}, {"metric": "one.two.three00.fourA.five1"},
                                 {"metric": "one.two.three00.fourA.five0"}]

        # True indicates leaf node
        expected_branch_nodes = {'one.two.three00.fourA',
                                 'one.two.three00.fourB',
                                 'one.two.three00.fourC',
                                 'one.two.three00.fourD',
                                 'one.two.three00.fourE'}  # fourE has enum values which makes it a branch node
        expected_leaf_nodes = {'one.two.three00.fourA',
                               'one.two.three00.fourD',
                               'one.two.three00.fourB'}  # metric one.two.three00 which has fourA, fourB as enum values

        self.verify_find_nodes_call(prefix, expected_branch_nodes, expected_leaf_nodes,
                                    self.bff, bf_mock_response, bf_enum_mock_response)

    def test_find_nodes_enum_as_next_level(self):
        prefix = 'one.two.three00.fourA.*'
        bf_mock_response = [{"metric": "one.two.three00.fourA.five100", "enum_values": ["ev1-1", "ev2-1"]},
                            {"metric": "one.two.three00.fourA.five2"},
                            {"metric": "one.two.three00.fourA.five1"},
                            {"metric": "one.two.three00.fourA.five0"}]

        bf_enum_mock_response = [{"metric": "one.two.three00.fourA.five100", "enum_values": ["ev1-1", "ev2-1"]},
                                 {"metric": "one.two.three00.fourA.five2"},
                                 {"metric": "one.two.three00.fourA.five1"},
                                 {"metric": "one.two.three00.fourA.five0"},
                                 {"metric": "one.two.three00.fourA"}]

        # True indicates leaf node
        expected_branch_nodes = {'one.two.three00.fourA.five100'}  # It is branch node because it has enum values
        expected_leaf_nodes = {'one.two.three00.fourA.five0',
                               'one.two.three00.fourA.five1',
                               'one.two.three00.fourA.five2'}

        self.verify_find_nodes_call(prefix, expected_branch_nodes, expected_leaf_nodes,
                                    self.bff, bf_mock_response, bf_enum_mock_response)

    def test_find_nodes_with_only_enum_as_next_level(self):
        prefix = 'one.two.three00.fourA.five100.*'
        bf_mock_response = []
        bf_enum_mock_response = [{"metric": "one.two.three00.fourA.five100", "enum_values": ["ev1-1", "ev2-1"]}]

        # True indicates leaf node
        expected_branch_nodes = set()
        expected_leaf_nodes = {'one.two.three00.fourA.five100.ev1-1',
                               'one.two.three00.fourA.five100.ev2-1'}

        self.verify_find_nodes_call(prefix, expected_branch_nodes, expected_leaf_nodes,
                                    self.bff, bf_mock_response, bf_enum_mock_response)

    def verify_find_nodes_call(self, prefix, expected_branch_nodes, expected_leaf_nodes, bff,
                               bf_mock_response, bf_enum_mock_response):
        actual_branch_nodes = set()
        actual_leaf_nodes = set()

        session = requests.Session()
        adapter = requests_mock.Adapter()
        session.mount('mock', adapter)

        with requests_mock.mock() as m:
            m.get('/v2.0/99999/metrics/search',
                  json=bf_mock_response, status_code=200)

            query_parts = prefix.split('.')
            if len(query_parts) > 1:
                enum_name = ".".join(query_parts[:-1])
                m.get('/v2.0/99999/metrics/search?include_enum_values=true&query=' + enum_name,
                      json=bf_enum_mock_response, status_code=200)

            for n in bff.find_nodes(storage.FindQuery(prefix, 1, 2)):
                if isinstance(n, BranchNode):
                    actual_branch_nodes.add(n.path)
                else:
                    actual_leaf_nodes.add(n.path)  # Leaf node

        print "Branch Nodes: %s" % actual_branch_nodes
        print "Leaf nodes: %s" % actual_leaf_nodes
        self.assertEquals(expected_branch_nodes, actual_branch_nodes)
        self.assertEquals(expected_leaf_nodes, actual_leaf_nodes)
