from cassandra.cluster import Cluster
from cassandra.policies import (RetryPolicy)
from cassandra.query import (PreparedStatement, BoundStatement)


class DBClient:
    session = None

    def connect(self, nodes):
        cluster = Cluster(
            contact_points=nodes,
            default_retry_policy=RetryPolicy()
        )
        metadata = cluster.metadata
        self.session = cluster.connect('DATA')
        print '\nConnected to cluster: %s' % metadata.cluster_name
        for host in metadata.all_hosts():
            print 'Datacenter: %s; Host: %s; Rack: %s' % (host.datacenter, host.address, host.rack)

    def close(self):
        self.session.cluster.shutdown()
        self.session.shutdown()
        print 'Connection closed.'

    def format_results(self, results):
        """
        Formats results to a tuple and returns it with just the (key, column1, value) from
        database results.

        :param results:
        :return:
        """

        return [(x.key, x.column1, x.value) for x in results]


    def get_metrics_excess_enums(self, metric_name):
        """
        Retrive data from metrics_excess_enums
        :param metric_name:
        :return:
        """
        prepared_stmt = self.session.prepare("SELECT * FROM metrics_excess_enums WHERE (key = ?)")
        bound_stmt = prepared_stmt.bind([metric_name])
        results = self.session.execute(bound_stmt)

        return self.format_results(results)

    def get_metrics_preaggregated_full(self, metric_list=[]):
        """
        Retrieve data from metrics_preaggregated_full

        :param metric_list:
        :return:
        """

        results = []

        for metric_name in metric_list:
            prepared_stmt = self.session.prepare("SELECT * FROM metrics_preaggregated_full WHERE (key = ?)")
            bound_stmt = prepared_stmt.bind([metric_name])
            results = self.session.execute(bound_stmt)

        return self.format_results(results)

    def get_metrics_preaggregated_5m(self, metric_list=[]):
        """
        Retrieve data from metrics_preaggregated_5m for a given list of metric names

        :param metric_list:
        :return:
        """

        results = []

        for metric_name in metric_list:
            prepared_stmt = self.session.prepare("SELECT * FROM metrics_preaggregated_5m WHERE (key = ?)")
            bound_stmt = prepared_stmt.bind([metric_name])
            results = self.session.execute(bound_stmt)

        return self.format_results(results)

    def get_metrics_preaggregated_20m(self, metric_list=[]):
        """
        Retrieve data from metrics_preaggregated_20m for a given list of metric names

        :param metric_list:
        :return:
        """

        results = []

        for metric_name in metric_list:
            prepared_stmt = self.session.prepare("SELECT * FROM metrics_preaggregated_20m WHERE (key = ?)")
            bound_stmt = prepared_stmt.bind([metric_name])
            results = self.session.execute(bound_stmt)

        return self.format_results(results)

    def get_metrics_preaggregated_60m(self, metric_list=[]):
        """
        Retrieve data from metrics_preaggregated_60m for a given list of metric names

        :param metric_list:
        :return:
        """

        results = []

        for metric_name in metric_list:
            prepared_stmt = self.session.prepare("SELECT * FROM metrics_preaggregated_60m WHERE (key = ?)")
            bound_stmt = prepared_stmt.bind([metric_name])
            results = self.session.execute(bound_stmt)

        return self.format_results(results)

    def get_metrics_preaggregated_240m(self, metric_list=[]):
        """
        Retrieve data from metrics_preaggregated_240m for a given list of metric names

        :param metric_list:
        :return:
        """

        results = []

        for metric_name in metric_list:
            prepared_stmt = self.session.prepare("SELECT * FROM metrics_preaggregated_240m WHERE (key = ?)")
            bound_stmt = prepared_stmt.bind([metric_name])
            results = self.session.execute(bound_stmt)

        return self.format_results(results)

    def get_metrics_preaggregated_1440m(self, metric_list=[]):
        """
        Retrieve data from metrics_preaggregated_1440m for a given list of metric names

        :param metric_list:
        :return:
        """

        results = []

        for metric_name in metric_list:
            prepared_stmt = self.session.prepare("SELECT * FROM metrics_preaggregated_1440m WHERE (key = ?)")
            bound_stmt = prepared_stmt.bind([metric_name])
            results = self.session.execute(bound_stmt)

        return self.format_results(results)

    def get_excess_enums_relevant_data(self, metric_name=None):
        """
        Returns a list of all excess enums related data in a dictionary. If metric_name is not sent,
        returns relevant data for all metrics in metrics_excess_enums table.

        :param metric_name:
        :return:
        """

        excess_enums = self.get_metrics_excess_enums(metric_name)

        metric_list = [x[0] for x in excess_enums]

        excess_enum_related_dict = {
            "metrics_excess_enums"          : excess_enums,
            "metrics_preaggregated_full"    : self.get_metrics_preaggregated_full(metric_list),
            "metrics_preaggregated_5m"      : self.get_metrics_preaggregated_5m(metric_list),
            "metrics_preaggregated_20m"     : self.get_metrics_preaggregated_20m(metric_list),
            "metrics_preaggregated_60m"     : self.get_metrics_preaggregated_60m(metric_list),
            "metrics_preaggregated_240m"    : self.get_metrics_preaggregated_240m(metric_list),
            "metrics_preaggregated_1440m"   : self.get_metrics_preaggregated_1440m(metric_list),
        }

        return excess_enum_related_dict

    def delete_metrics_excess_enums(self, metric_name):
        prepared_stmt = self.session.prepare("DELETE FROM metrics_excess_enums WHERE (key = ?)")
        bound_stmt = prepared_stmt.bind([metric_name])
        self.session.execute(bound_stmt)

        print 'Deleted [%s] from metrics_excess_enums' % metric_name

    def delete_metrics_preaggregated_full(self, metric_name):
        prepared_stmt = self.session.prepare("DELETE FROM metrics_preaggregated_full WHERE (key = ?)")
        bound_stmt = prepared_stmt.bind([metric_name])
        self.session.execute(bound_stmt)

        print 'Deleted [%s] from metrics_preaggregated_full' % metric_name

    def delete_metrics_preaggregated_5m(self, metric_name):
        prepared_stmt = self.session.prepare("DELETE FROM metrics_preaggregated_5m WHERE (key = ?)")
        bound_stmt = prepared_stmt.bind([metric_name])
        self.session.execute(bound_stmt)

        print 'Deleted [%s] from metrics_preaggregated_5m' % metric_name

    def delete_metrics_preaggregated_20m(self, metric_name):
        prepared_stmt = self.session.prepare("DELETE FROM metrics_preaggregated_20m WHERE (key = ?)")
        bound_stmt = prepared_stmt.bind([metric_name])
        self.session.execute(bound_stmt)

        print 'Deleted [%s] from metrics_preaggregated_20m' % metric_name

    def delete_metrics_preaggregated_60m(self, metric_name):
        prepared_stmt = self.session.prepare("DELETE FROM metrics_preaggregated_60m WHERE (key = ?)")
        bound_stmt = prepared_stmt.bind([metric_name])
        self.session.execute(bound_stmt)

        print 'Deleted [%s] from metrics_preaggregated_60m' % metric_name

    def delete_metrics_preaggregated_240m(self, metric_name):
        prepared_stmt = self.session.prepare("DELETE FROM metrics_preaggregated_240m WHERE (key = ?)")
        bound_stmt = prepared_stmt.bind([metric_name])
        self.session.execute(bound_stmt)

        print 'Deleting [%s] from metrics_preaggregated_240m' % metric_name

    def delete_metrics_preaggregated_1440m(self, metric_name):
        prepared_stmt = self.session.prepare("DELETE FROM metrics_preaggregated_1440m WHERE (key = ?)")
        bound_stmt = prepared_stmt.bind([metric_name])
        self.session.execute(bound_stmt)

        print 'Deleted [%s] from metrics_preaggregated_1440m' % metric_name