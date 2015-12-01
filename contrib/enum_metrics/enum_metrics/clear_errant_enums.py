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


def clear_from_db(nodes, metric_name, tenant_id, dryrun):
    print '\n***** Deleting from Cassandra *****\n'

    client = db.DBClient()
    client.connect(nodes)

    key = tenant_id + '.' + metric_name
    excess_enum_related_dict = client.get_excess_enums_relevant_data(key)

    print_excess_enums_relevant_data(excess_enum_related_dict, key)
    if not dryrun:
        delete_excess_enums_relevant_data(client, excess_enum_related_dict)

    client.close()


def delete_excess_enums_relevant_data(client, excess_enum_related_dict):
    for excess_enum in excess_enum_related_dict['metrics_excess_enums']:
        print 'Deleting metrics data related to excess enum: %s \n' % excess_enum[0]

        client.delete_metrics_excess_enums(excess_enum[0])
        client.delete_metrics_preaggregated_full(excess_enum[0])
        client.delete_metrics_preaggregated_5m(excess_enum[0])
        client.delete_metrics_preaggregated_20m(excess_enum[0])
        client.delete_metrics_preaggregated_60m(excess_enum[0])
        client.delete_metrics_preaggregated_240m(excess_enum[0])
        client.delete_metrics_preaggregated_1440m(excess_enum[0])

        print '\nDeleted successfully metrics data related to excess enum: %s ' % excess_enum[0]
        print '\n'


def print_excess_enums_relevant_data(excess_enum_related_dict, key):
    print '\nColumn family: metrics_excess_enums'
    if not excess_enum_related_dict['metrics_excess_enums']: print 'Key %s NOT FOUND' % key
    for x in excess_enum_related_dict['metrics_excess_enums']: print x

    print '\nColumn family: metrics_preaggregated_full'
    if not excess_enum_related_dict['metrics_preaggregated_full']: print 'Key %s NOT FOUND' % key
    for x in excess_enum_related_dict['metrics_preaggregated_full']: print x

    print '\nColumn family: metrics_preaggregated_5m'
    if not excess_enum_related_dict['metrics_preaggregated_5m']: print 'Key %s NOT FOUND' % key
    for x in excess_enum_related_dict['metrics_preaggregated_5m']: print x

    print '\nColumn family: metrics_preaggregated_20m'
    if not excess_enum_related_dict['metrics_preaggregated_20m']: print 'Key %s NOT FOUND' % key
    for x in excess_enum_related_dict['metrics_preaggregated_20m']: print x

    print '\nColumn family: metrics_preaggregated_60m'
    if not excess_enum_related_dict['metrics_preaggregated_60m']: print 'Key %s NOT FOUND' % key
    for x in excess_enum_related_dict['metrics_preaggregated_60m']: print x

    print '\nColumn family: metrics_preaggregated_240m'
    if not excess_enum_related_dict['metrics_preaggregated_240m']: print 'Key %s NOT FOUND' % key
    for x in excess_enum_related_dict['metrics_preaggregated_240m']: print x

    print '\nColumn family: metrics_preaggregated_1440m'
    if not excess_enum_related_dict['metrics_preaggregated_1440m']: print 'Key %s NOT FOUND' % key
    for x in excess_enum_related_dict['metrics_preaggregated_1440m']: print x

    print '\n'


def clear_from_es(es_nodes, metric_name, tenant_id, dryrun):
    print '\n***** Deleting from Elastic Cluster *****\n'
    es_client = es.ESClient(es_nodes)

    metric_metadata = es_client.get_metric_metadata(metric_name=metric_name, tenant_id=tenant_id)
    enums_data = es_client.get_enums_data(metric_name=metric_name, tenant_id=tenant_id)

    print_enum_related_data(metric_metadata, enums_data)

    if not dryrun:
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


def print_enum_related_data(metric_meta_data, enums_data):
    print '\nmetric_metadata:' if metric_meta_data['found'] else 'metric_metadata NOT FOUND: '
    print json.dumps(metric_meta_data, indent=2)

    print '\nenums:' if enums_data['found'] else 'enums NOT FOUND: '
    print json.dumps(enums_data, indent=2)


def main():
    args = parseArguments(sys.argv[1:])

    clear_from_db(config.cassandra_nodes, args.metricName, args.tenantId, args.dryrun)
    clear_from_es(es_nodes=config.es_nodes, metric_name=args.metricName, tenant_id=args.tenantId, dryrun=args.dryrun)


if __name__ == "__main__":
    main()
