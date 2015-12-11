from unittest import TestCase

import enum_metrics.errant_enums as em
import mock


class TestClearEnums(TestCase):
    def test_valid_args_1(self):
        metric_name = 'metric.test'
        args = em.parse_arguments(['delete', '--dryrun', '-m', metric_name, '-t', '836986'])

        self.assertEquals(args.dryrun, True)
        self.assertEquals(args.metricName, metric_name)
        self.assertEquals(args.env, em.LOCALHOST)

    def test_valid_args_2(self):
        metric_name = 'metric.test'
        args = em.parse_arguments(['delete', '--metricName', metric_name, '--tenantId', '836986', '-e', em.LOCALHOST])

        self.assertEquals(args.dryrun, False)
        self.assertEquals(args.metricName, metric_name)
        self.assertEquals(args.env, em.LOCALHOST)

    def test_valid_args_3(self):
        metric_name = 'metric.test'
        args = em.parse_arguments(['list'])

        self.assertTrue('dryrun' not in args)
        self.assertEquals(args.env, em.LOCALHOST)

    def test_invalid_args_1(self):
        self.assertRaises(SystemExit, em.parse_arguments, ['delete', '--dryrun', '--tenantId', '836986'])

    def test_invalid_args_2(self):
        metric_name = 'metric.test'
        self.assertRaises(SystemExit, em.parse_arguments, ['delete', '--dryrun', '--metricName', metric_name])

    def test_invalid_args_3(self):
        self.assertRaises(SystemExit, em.parse_arguments, ['list', '--dryrun', '--tenantId', '836986'])

    @mock.patch('enum_metrics.dbclient')
    @mock.patch('enum_metrics.esclient')
    def test_dryrun_should_not_perform_actual_delete(self, mock_db_client, mock_es_client):
        metric_name = 'metric.test'
        args = em.parse_arguments(['delete', '-m', metric_name, '-t', '836986', '--dryrun'])

        # set up the mock for db client
        mock_db_client.connect.return_value = ''
        mock_db_client.get_excess_enums_relevant_data.return_value = {
            'metrics_excess_enums': ('836986.enumtest.enum.one', 0),
            'metrics_preaggregated_full': ('836986.enumtest.enum.one', 0),
            'metrics_preaggregated_5m': ('836986.enumtest.enum.one', 0),
            'metrics_preaggregated_20m': ('836986.enumtest.enum.one', 0),
            'metrics_preaggregated_60m': ('836986.enumtest.enum.one', 0),
            'metrics_preaggregated_240m': ('836986.enumtest.enum.one', 0),
            'metrics_preaggregated_1440m': ('836986.enumtest.enum.one', 0),
        }

        # set up the mock for es client
        mock_es_client.get_metric_metadata.return_value = {'found': 'true'}
        mock_es_client.get_enums_data.return_value = {'found': 'true'}

        em.clear_excess_enums(args, mock_db_client, mock_es_client)

        self.assertFalse(mock_db_client.delete_metrics_excess_enums.called,
                         'delete_metrics_excess_enums method should not have been called')
        self.assertFalse(mock_db_client.delete_metrics_preaggregated_full.called,
                         'delete_metrics_preaggregated_full method should not have been called')
        self.assertFalse(mock_db_client.delete_metrics_preaggregated_5m.called,
                         'delete_metrics_preaggregated_5m method should not have been called')
        self.assertFalse(mock_db_client.delete_metrics_preaggregated_20m.called,
                         'delete_metrics_preaggregated_20m method should not have been called')
        self.assertFalse(mock_db_client.delete_metrics_preaggregated_60m.called,
                         'delete_metrics_preaggregated_60m method should not have been called')
        self.assertFalse(mock_db_client.delete_metrics_preaggregated_240m.called,
                         'delete_metrics_preaggregated_240m method should not have been called')
        self.assertFalse(mock_db_client.delete_metrics_preaggregated_1440m.called,
                         'delete_metrics_preaggregated_1440m method should not have been called')

        self.assertFalse(mock_es_client.delete_metric_metadata.called,
                         'delete_metric_metadata method should not have been called')
        self.assertFalse(mock_es_client.delete_enums_data.called,
                         'delete_enums_data method should not have been called')

    @mock.patch('enum_metrics.dbclient')
    @mock.patch('enum_metrics.esclient')
    def test_delete_from_es_only_if_it_is_an_excess_enum(self, mock_db_client, mock_es_client):
        metric_name = 'metric.test'
        args = em.parse_arguments(['delete', '-m', metric_name, '-t', '836986'])

        # set up the mock for db client
        mock_db_client.connect.return_value = ''
        mock_db_client.get_excess_enums_relevant_data.return_value = {}  # metric is not an excess enum

        # set up the mock for es client
        mock_es_client.get_metric_metadata.return_value = {'found': 'true'}
        mock_es_client.get_enums_data.return_value = {'found': 'true'}

        em.clear_excess_enums(args, mock_db_client, mock_es_client)

        self.assertFalse(mock_es_client.delete_metric_metadata.called,
                         'delete_metric_metadata method should not have been called')
        self.assertFalse(mock_es_client.delete_enums_data.called,
                         'delete_enums_data method should not have been called')
