import sys
import argparse
import json
import dbclient as db
import esclient as es
import config as cf
from datetime import datetime

LOCALHOST = 'localhost'
PROGRESS_MARKER_NUM = 1000000
errant_ttl_in_seconds = 157680000  # 5 years
is_verbose = False


def parse_arguments(args):
    """Parses the supplied arguments"""

    parser = argparse.ArgumentParser(prog="errant_ttl.py", description='show and repair cassandra records with errant ttl, e.g. null')

    subparsers = parser.add_subparsers(help='commands')

    show_errant_ttl = subparsers.add_parser('show', help='get all records in a table with errant ttl')
    repair_errant_ttl = subparsers.add_parser('repair', help='repair all records in a table with errant ttl')

    for p in [show_errant_ttl, repair_errant_ttl]:
        p.add_argument('-e', '--env', choices=cf.Config.get_environments(),
                           default=LOCALHOST, help='Environment we are pointing to')
        p.add_argument('-cf', '--columnFamily',
                           required=True, help='name of column family (table) to retrieve rows from')
        p.add_argument("-l", "--limit", required=False, default=500,
                            help='limit number of records to retrieve at a time for analyzing; default to 500, use 0 for no limit;')
        p.add_argument("-ttl", "--ttlInSeconds", required=False, help='the ttl threshold in seconds to compare against, default to 157680000 seconds (5 years)')
        p.add_argument("-v", "--verbose", action="store_true")

    repair_errant_ttl.add_argument('--dryrun', action='store_true')

    args = parser.parse_args()

    if args.ttlInSeconds:
        global errant_ttl_in_seconds
        errant_ttl_in_seconds = int(args.ttlInSeconds)

    global is_verbose
    if args.verbose: is_verbose = True
    if is_verbose: print "args: " + str(args)

    return args


def get_rows_with_ttl(db_client, args):
    """
    return all records with tll for columnFamily
    """
    cqlstr = "SELECT key, column1, value, ttl(value) as ttl_val FROM " + args.columnFamily
    if int(args.limit) > 0: cqlstr += " limit " + str(args.limit)

    if is_verbose: print "cqlstr: " + cqlstr
    results = ()
    try:
        prepared_stmt = db_client.session.prepare(cqlstr)
        results = db_client.session.execute(prepared_stmt)
    except Exception as e:
        print "Error (" + type(e).__name__ + " Exception): " + e.message

    return results


def delete_errant_ttl(db_client, args, record):
    """
    delete record where key,column1
    """
    key = str(record[0])
    column1 = str(record[1])

    cqlstr = "DELETE FROM %s where key=%r and column1=%s;" % (args.columnFamily, key, column1)
    if is_verbose: print "cqlstr: " + cqlstr

    success = False
    if not args.dryrun:
        try:
            prepared_stmt = db_client.session.prepare(cqlstr)
            result = db_client.session.execute(prepared_stmt)
            success = True
        except Exception as e:
            print "Error (" + type(e).__name__ + " Exception): " + e.message
    return success


def show_errant_ttl(db_client, args):
    if is_verbose: print "\nShowing errant ttls records for " + args.columnFamily
    results = get_rows_with_ttl(db_client, args)

    num_record = 0
    num_errant_results = 0
    if is_verbose: print "fields: key, column1, value, ttl"
    for r in results:
        num_record += 1
        # print the record in result regardless of validity if reached PROGRESS_MARKER_NUM
        if is_verbose and (num_record % PROGRESS_MARKER_NUM == 0):
            print "Marker %i reached with record %i: %s" % (int(num_record / PROGRESS_MARKER_NUM), num_record, str(r))
        # print and append to errant_results if ttl is null or > errant_ttl_in_seconds threshold
        if (r.ttl_val == None) or (int(r.ttl_val) > errant_ttl_in_seconds):
            print "%r, %s, %r, %s" % (str(r[0]), str(r[1]), str(r[2]), str(r[3]))
            num_errant_results += 1

    if is_verbose:
        print "total number of records analyzed: " + str(num_record)
        print "total number of errant records: " + str(num_errant_results)


def repair_errant_ttl(db_client, args):
    """
    repair all records with errant ttl in columnFamily by setting a ttl
    """
    if is_verbose: print "\nDeleting errant ttls records for " + args.columnFamily

    results = get_rows_with_ttl(db_client, args)
    num_record = 0
    numDeleted = 0
    for r in results:
        num_record += 1
        if is_verbose and (num_record % PROGRESS_MARKER_NUM == 0):
            print "Marker %i reached with record %i: %s" % (int(num_record / PROGRESS_MARKER_NUM), num_record, str(r))
        # delete record if ttl is invalid
        if (r.ttl_val == None) or (int(r.ttl_val) > errant_ttl_in_seconds):
            if delete_errant_ttl(db_client, args, r):
                numDeleted += 1

    if is_verbose:
        print "total number of records analyzed: " + str(num_record)
        print "number of errant records deleted: " + str(numDeleted) + "\n"


def main():
    args = parse_arguments(sys.argv[1:])
    if is_verbose: print("\nstart: " + str(datetime.now()))

    config = cf.Config(args.env.lower())

    db_client = db.DBClient()
    db_client.connect(config.get_cassandra_nodes())

    if sys.argv[1] == 'show':
        show_errant_ttl(db_client, args)
    elif sys.argv[1] == 'repair':
        repair_errant_ttl(db_client, args)

    db_client.close()
    if is_verbose: print("\nend: " + str(datetime.now()))

if __name__ == "__main__":
    main()
