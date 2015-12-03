import sys
import argparse
import json
import dbclient as db
import esclient as es
import config


def parseArguments(args):
    """Parses the supplied arguments"""
    parser = argparse.ArgumentParser(prog="clear_errant_enums.py", description='Script to delete errant enums')

    parser.add_argument('--dryrun',
                        action='store_true', help='Display errant enums related data for the given metric name.')
    parser.add_argument('-m', '--metricName',
                        required=True, help='metric name to be deleted')
    parser.add_argument('-t', '--tenantId',
                        required=True, help='tenantId corresponding to the metric name to be deleted')

    args = parser.parse_args(args)
    print args

    return args


def clear_data(args):
    total_records_deleted = clear_from_db(cassandra_nodes=config.cassandra_nodes, metric_name=args.metricName,
                                          tenant_id=args.tenantId, dryrun=args.dryrun)

    is_excess_enum = True if total_records_deleted else False

    clear_from_es(es_nodes=config.es_nodes, metric_name=args.metricName, tenant_id=args.tenantId,
                  dryrun=args.dryrun, is_excess_enum=is_excess_enum)


def clear_from_db(cassandra_nodes, metric_name, tenant_id, dryrun):
    """

    :param nodes:
    :param metric_name:
    :param tenant_id:
    :param dryrun:
    :return: total number of records being deleted from all tables.
    """
    print '\n***** Deleting from Cassandra *****\n'

    client = db.DBClient()
    client.connect(cassandra_nodes)

    excess_enum_related_dict = client.get_excess_enums_relevant_data(tenant_id=tenant_id, metric_name=metric_name)
    print_excess_enums_relevant_data(excess_enum_related_dict, tenant_id=tenant_id, metric_name=metric_name)

    if not dryrun:
        delete_excess_enums_relevant_data(client, excess_enum_related_dict)

    client.close()
    return sum(len(x) for x in excess_enum_related_dict.itervalues())


def clear_from_es(es_nodes, metric_name, tenant_id, dryrun, is_excess_enum):
    print '\n***** Deleting from Elastic Cluster *****\n'
    es_client = es.ESClient(es_nodes)

    metric_metadata = es_client.get_metric_metadata(metric_name=metric_name, tenant_id=tenant_id)
    enums_data = es_client.get_enums_data(metric_name=metric_name, tenant_id=tenant_id)

    if (metric_metadata['found'] or enums_data['found']) and not is_excess_enum:
        print "**** WARNING: Records exists in ES but is not an excess enum as per cassandra. " \
              "Data wont be deleted from ES. ****"

    print_enum_related_data(metric_metadata, enums_data)

    # Delete from ES only if it is an excess_enum as per cassandra
    if (not dryrun) and is_excess_enum:
        if metric_metadata['found']:
            es_client.delete_metric_metadata(metric_name=metric_name, tenant_id=tenant_id)
        else:
            print 'Document NOT FOUND in index metric_metadata for id: [%s] routing: [%s]' % \
                  (metric_metadata['_id'], tenant_id)

        if enums_data['found']:
            es_client.delete_enums_data(metric_name=metric_name, tenant_id=tenant_id)
        else:
            print 'Document NOT FOUND in index enums for id: [%s] routing: [%s]' % \
                  (enums_data['_id'], tenant_id)


def delete_excess_enums_relevant_data(client, excess_enum_related_dict):
    if excess_enum_related_dict:
        key = excess_enum_related_dict['metrics_excess_enums'][0][0]  # Grabbing the key from the first row
        print 'Deleting metrics data related to excess enum key: [%s] \n' % key

        client.delete_metrics_excess_enums(key)
        client.delete_metrics_preaggregated_full(key)
        client.delete_metrics_preaggregated_5m(key)
        client.delete_metrics_preaggregated_20m(key)
        client.delete_metrics_preaggregated_60m(key)
        client.delete_metrics_preaggregated_240m(key)
        client.delete_metrics_preaggregated_1440m(key)
        print '\nDeleted successfully metrics data related to excess enum key: [%s] \n' % key


def print_excess_enums_relevant_data(excess_enum_related_dict, tenant_id, metric_name):
    print '\nColumn family: metrics_excess_enums'
    if not excess_enum_related_dict:
        print 'Row NOT FOUND for tenant_id: [%s] metric_name: [%s] ' % (tenant_id, metric_name)
    else:
        for x in excess_enum_related_dict['metrics_excess_enums']: print x

        key = excess_enum_related_dict['metrics_excess_enums'][0][0]  # Grabbing the key from the first row
        print '\nColumn family: metrics_preaggregated_full'
        if not excess_enum_related_dict['metrics_preaggregated_full']:
            print 'Row corresponding to excess_enums [%s] NOT FOUND' % key
        for x in excess_enum_related_dict['metrics_preaggregated_full']: print x

        print '\nColumn family: metrics_preaggregated_5m'
        if not excess_enum_related_dict['metrics_preaggregated_5m']:
            print 'Row corresponding to excess_enums [%s] NOT FOUND' % key
        for x in excess_enum_related_dict['metrics_preaggregated_5m']: print x

        print '\nColumn family: metrics_preaggregated_20m'
        if not excess_enum_related_dict['metrics_preaggregated_20m']:
            print 'Row corresponding to excess_enums [%s] NOT FOUND' % key
        for x in excess_enum_related_dict['metrics_preaggregated_20m']: print x

        print '\nColumn family: metrics_preaggregated_60m'
        if not excess_enum_related_dict['metrics_preaggregated_60m']:
            print 'Row corresponding to excess_enums [%s] NOT FOUND' % key
        for x in excess_enum_related_dict['metrics_preaggregated_60m']: print x

        print '\nColumn family: metrics_preaggregated_240m'
        if not excess_enum_related_dict['metrics_preaggregated_240m']:
            print 'Row corresponding to excess_enums [%s] NOT FOUND' % key
        for x in excess_enum_related_dict['metrics_preaggregated_240m']: print x

        print '\nColumn family: metrics_preaggregated_1440m'
        if not excess_enum_related_dict['metrics_preaggregated_1440m']:
            print 'Row corresponding to excess_enums [%s] NOT FOUND' % key
        for x in excess_enum_related_dict['metrics_preaggregated_1440m']: print x

    print '\n'


def print_enum_related_data(metric_meta_data, enums_data):
    print '\nmetric_metadata:' if metric_meta_data['found'] else 'metric_metadata NOT FOUND: '
    print json.dumps(metric_meta_data, indent=2)

    print '\nenums:' if enums_data['found'] else 'enums NOT FOUND: '
    print json.dumps(enums_data, indent=2)


def main():
    args = parseArguments(sys.argv[1:])

    clear_data(args)


if __name__ == "__main__":
    main()
