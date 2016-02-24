from unittest import TestCase
import blueflood as bf


class TestBlueFlood(TestCase):
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
        config = {'blueflood': {'urls': ['xxx']}}

        bff = bf.TenantBluefloodFinder(config)
        self.assertTrue(bff.complete('a.b.c.d', 4))

    def test_non_complete_metric_name(self):
        config = {'blueflood': {'urls': ['xxx']}}

        bff = bf.TenantBluefloodFinder(config)
        self.assertFalse(bff.complete('a.b.c.d', 3))