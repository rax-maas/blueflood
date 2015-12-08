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
        """

        return [(x.key, x.column1, x.value) for x in results]

    def get_all_metrics_excess_enums(self):
        """
        return all metrics excess enums present in the table metrics_excess_enums
        """
        prepared_stmt = self.session.prepare("SELECT * FROM metrics_excess_enums")
        results = self.session.execute(prepared_stmt)

        return self.format_results(results)

    def get_metrics_excess_enums(self, tenant_id, metric_name):
        """
        Retrive data from metrics_excess_enums
        """

        key = tenant_id + '.' + metric_name
        return self.__get_metrics(key, "metrics_excess_enums")

    def get_metrics_preaggregated_full(self, key):
        """
        Retrieve data from metrics_preaggregated_full
        """

        return self.__get_metrics(key, "metrics_preaggregated_full")

    def get_metrics_preaggregated_5m(self, key):
        """
        Retrieve data from metrics_preaggregated_5m for a given key
        """

        return self.__get_metrics(key, "metrics_preaggregated_5m")

    def get_metrics_preaggregated_20m(self, key):
        """
        Retrieve data from metrics_preaggregated_20m for a given key
        """

        return self.__get_metrics(key, "metrics_preaggregated_20m")

    def get_metrics_preaggregated_60m(self, key):
        """
        Retrieve data from metrics_preaggregated_60m for a given key
        """

        return self.__get_metrics(key, "metrics_preaggregated_60m")

    def get_metrics_preaggregated_240m(self, key):
        """
        Retrieve data from metrics_preaggregated_240m for a given key
        """

        return self.__get_metrics(key, "metrics_preaggregated_240m")

    def get_metrics_preaggregated_1440m(self, key):
        """
        Retrieve data from metrics_preaggregated_1440m for a given key
        """

        return self.__get_metrics(key, "metrics_preaggregated_1440m")

    def __get_metrics(self, key, table_name):
        """
        get metrics data for the given key and table_name
        """

        prepared_stmt = self.session.prepare("SELECT * FROM %s WHERE (key = ?)" % table_name)
        bound_stmt = prepared_stmt.bind([key])
        results = self.session.execute(bound_stmt)

        return self.format_results(results)

    def get_excess_enums_relevant_data(self, tenant_id, metric_name):
        """
        Returns a list of all excess enums related data in a dictionary. If metric_name is not sent,
        returns relevant data for all metrics in metrics_excess_enums table.
        """
        excess_enums = self.get_metrics_excess_enums(tenant_id, metric_name)
        excess_enum_related_dict = {}

        if excess_enums:
            excess_enum_related_dict["metrics_excess_enums"] = excess_enums

            key = excess_enums[0][0]  # Grabbing the key from the first row
            excess_enum_related_dict["metrics_preaggregated_full"] = self.get_metrics_preaggregated_full(key)
            excess_enum_related_dict["metrics_preaggregated_5m"] = self.get_metrics_preaggregated_5m(key)
            excess_enum_related_dict["metrics_preaggregated_20m"] = self.get_metrics_preaggregated_20m(key)
            excess_enum_related_dict["metrics_preaggregated_60m"] = self.get_metrics_preaggregated_60m(key)
            excess_enum_related_dict["metrics_preaggregated_240m"] = self.get_metrics_preaggregated_240m(key)
            excess_enum_related_dict["metrics_preaggregated_1440m"] = self.get_metrics_preaggregated_1440m(key)

        return excess_enum_related_dict

    def delete_metrics_excess_enums(self, key):
        self.__delete_metrics_data(key=key, table_name="metrics_excess_enums")

    def delete_metrics_preaggregated_full(self, key):
        self.__delete_metrics_data(key=key, table_name="metrics_preaggregated_full")

    def delete_metrics_preaggregated_5m(self, key):
        self.__delete_metrics_data(key=key, table_name="metrics_preaggregated_5m")

    def delete_metrics_preaggregated_20m(self, key):
        self.__delete_metrics_data(key=key, table_name="metrics_preaggregated_20m")

    def delete_metrics_preaggregated_60m(self, key):
        self.__delete_metrics_data(key=key, table_name="metrics_preaggregated_60m")

    def delete_metrics_preaggregated_240m(self, key):
        self.__delete_metrics_data(key=key, table_name="metrics_preaggregated_240m")

    def delete_metrics_preaggregated_1440m(self, key):
        self.__delete_metrics_data(key=key, table_name="metrics_preaggregated_1440m")

    def __delete_metrics_data(self, key, table_name):
        """
        Delete metrics data based on key and table_name
        """

        prepared_stmt = self.session.prepare("DELETE FROM %s WHERE (key = ?)" % table_name)
        bound_stmt = prepared_stmt.bind([key])
        self.session.execute(bound_stmt)

        print 'Deleted [%s] from [%s]' % (key, table_name)
