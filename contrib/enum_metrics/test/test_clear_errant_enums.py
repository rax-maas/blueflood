from unittest import TestCase

import sys
import enum_metrics.clear_errant_enums as em

class TestClearEnums(TestCase):

    def test_valid_args_1(self):
        metric_name = 'metric.test'
        args = em.parseArguments(['--dryrun', '-m', metric_name, '-t', '836986'])

        assert args.dryrun is True
        assert args.metricName is metric_name

    def test_valid_args_2(self):
        metric_name = 'metric.test'
        args = em.parseArguments(['--metricName',  metric_name, '--tenantId', '836986'])

        assert args.dryrun is False
        assert args.metricName is metric_name

    def test_invalid_args_1(self):
        self.assertRaises(SystemExit, em.parseArguments, ['--dryrun', '--tenantId', '836986'])

    def test_invalid_args_2(self):
        metric_name = 'metric.test'
        self.assertRaises(SystemExit, em.parseArguments, ['--dryrun', '--metricName', metric_name])