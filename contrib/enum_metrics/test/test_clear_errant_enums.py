from unittest import TestCase

import sys
import enum_metrics.clear_errant_enums as em

class TestClearEnums(TestCase):

    def test_valid_args_1(self):
        args = em.parseArguments(['--all',  '--dryrun'])

        assert args.all is True
        assert args.dryrun is True
        assert args.metricName is None

    def test_valid_args_2(self):
        args = em.parseArguments(['--metricName',  'metric.one', '--dryrun'])

        assert args.all is False
        assert args.dryrun is True
        assert args.metricName is 'metric.one'

    def test_valid_args_3(self):
        args = em.parseArguments(['--metricName',  'metric.one'])

        assert args.all is False
        assert args.dryrun is False
        assert args.metricName is 'metric.one'

    def test_invalid_args_1(self):
        self.assertRaises(SystemExit, em.parseArguments, ['--metricName',  'metric.one', '--dryrun', '--all'])
