import sys
import argparse
import dbclient as db


def parseArguments(args):
    """Parses the supplied arguments"""
    parser = argparse.ArgumentParser(prog="clear_errant_enums.py", description='Script to delete errant enums')
    parser.add_argument('--all', action='store_true', help='Delete all errant enums')
    parser.add_argument('--dryrun', action='store_true', help='Display all errant enums to be deleted')
    parser.add_argument('-m', '--metricName', help='metric name to be deleted')

    args = parser.parse_args(args)
    print args

    # Either --all or -m option must be specified but not both
    if (args.all is True and args.metricName) or (args.all is False and (not args.metricName)):
        parser.error("--all and -m are mutually exclusive options and atleast one of them should be specified.")
    return args


def clear_from_db(nodes, metric_name, dryrun):
    client = db.DBClient()
    client.connect(nodes)

    excess_enum_related_dict = client.get_excess_enums_relevant_data(metric_name)

    if dryrun:
        print_excess_enums_relevant_data(excess_enum_related_dict)
    else:
        delete_excess_enums_relevant_data(client, excess_enum_related_dict)

    client.close()


def delete_excess_enums_relevant_data(client, excess_enum_related_dict):
    for excess_enum in excess_enum_related_dict['metrics_excess_enums']:
        print 'Deleting metrics data related to excess enum: %s ' % excess_enum[0]

        client.delete_metrics_excess_enums(excess_enum[0])
        client.delete_metrics_preaggregated_full(excess_enum[0])
        client.delete_metrics_preaggregated_5m(excess_enum[0])
        client.delete_metrics_preaggregated_20m(excess_enum[0])
        client.delete_metrics_preaggregated_60m(excess_enum[0])
        client.delete_metrics_preaggregated_240m(excess_enum[0])
        client.delete_metrics_preaggregated_1440m(excess_enum[0])

        print 'Deleted successfully metrics data related to excess enum: %s ' % excess_enum[0]
        print '\n'


def print_excess_enums_relevant_data(excess_enum_related_dict):
    for x in excess_enum_related_dict['metrics_excess_enums']: print x
    for x in excess_enum_related_dict['metrics_preaggregated_full']: print x
    for x in excess_enum_related_dict['metrics_preaggregated_5m']: print x
    for x in excess_enum_related_dict['metrics_preaggregated_20m']: print x
    for x in excess_enum_related_dict['metrics_preaggregated_60m']: print x
    for x in excess_enum_related_dict['metrics_preaggregated_240m']: print x
    for x in excess_enum_related_dict['metrics_preaggregated_1440m']: print x


def main():
    args = parseArguments(sys.argv[1:])
    print args

    nodes = ['127.0.0.1']
    clear_from_db(nodes, args.metricName, args.dryrun)


if __name__ == "__main__":
    main()
