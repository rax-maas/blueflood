from __future__ import print_function
import json
import argparse
import requests
import re
import logging as log
from datetime import datetime, timedelta


def generate_range_repair_query (node, start_time, current_time):
  query_json = {
    "query": {
        "filtered": {
            "query": {
                "bool": {
                    "should": [
                        {
                            "query_string": {
                                "query": 'level:"info" AND (*)'
                            }
                        }
                    ]
                }
            },
            "filter": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "@timestamp": {
                                    "from": start_time,
                                    "to": current_time
                                }
                            }
                        },
                        {
                            "fquery": {
                                "query": {
                                    "query_string": {
                                        "query": 'host:("' + node + '")'
                                    }
                                },
                                "_cache": True
                            }
                        },
                        {
                            "fquery": {
                                "query": {
                                    "query_string": {
                                        "query": 'method:("syncComplete")'
                                    }
                                },
                                "_cache": True
                            }
                        },
                        {
                            "fquery": {
                                "query": {
                                    "query_string": {
                                        "query": 'message:("is fully synced")'
                                    }
                                },
                                "_cache": True
                            }
                        }
                    ]
                }
            }
        }
    }
  }

  return json.dumps(query_json)


# Execute an ES _count to return the number of ranges that have finished repairing
def calculate_range_repairs(host, port, node, start_time, current_time):
  url = 'http://%(host)s:%(port)s/_count' % dict(host=host, port=port)
  query = generate_range_repair_query(node, start_time, current_time)
  
  log.info("Ranges completed: curl %(url)s -d '%(query)s'" % dict(url=url, query=query)) 
  r = requests.get(url, data=query)
  
  if r.status_code == 200:
    response = json.loads(r.text)
    return response['count']


def generate_total_ranges_query(node, start_time, current_time):
  query_json = {
    "fields": "message",
    "query": {
      "filtered": {
        "filter": {
          "bool": {
            "must": [
              {
                "range": {
                  "@timestamp": {
                    "to": current_time,
                    "from": start_time
                  }
                }
              },
              {
                "fquery": {
                  "_cache": True,
                  "query": {
                    "query_string": {
                      "query": 'host:("' + node + '")'
                    }
                  }
                }
              }
            ]
          }
        },
        "query": {
          "query_string": {
            "query": 'message: "Starting repair"'
          }
        }
      }
    }
  }

  return json.dumps(query_json)


# Execute an ES _search to return the total number of ranges that need repairing
def calculate_total_ranges(host, port, node, start_time, current_time):  
  url = 'http://%(host)s:%(port)s/_search' % dict(host=host, port=str(port))
  query = generate_total_ranges_query(node, start_time, current_time)

  log.info("Total ranges: curl %(url)s -d '%(query)s'" % dict(url=url, query=query))
  r = requests.get(url, data=query )
  
  if r.status_code == 200:
    response = json.loads(r.text)
    message = json.dumps(response['hits']['hits'][0]['fields']['message'])    
    total_ranges = int(re.search('(?<=repairing\s)\d+(?=\sranges)', message).group(0))
    return total_ranges


def print_totals(node, days, total_successful_range_repairs, total_to_repair):
  percent_complete = total_successful_range_repairs * 100 / total_to_repair
  print ("Checking repair messages for", node, "for the past", days, "days")
  print ("Count is: " + str(total_successful_range_repairs))
  print ("Total to repair is: " + str(total_to_repair))
  print ("Percent Complete: " + str(percent_complete) + "%")


def main():
  parser = argparse.ArgumentParser(
    description = '''This script shows the current status of a repair running on a Cassandra node.  
                    Elasticsearch is required so that we can query efficiently.

                    Note that NODE is required, and it should be any part of the hostname
                    of the node that will uniquely identify the node.''')
  parser.add_argument('-n', '--node',     type=str,
                                          required=True, 
                                          help='''Which node to check. This can be any portion 
                                                  of the hostname that uniquely identifies the node. 
                                                  (required)''')
  parser.add_argument('--host',           type=str, 
                                          help='Hostname for Elasticsearch (required)', 
                                          default='localhost')
  parser.add_argument('-p', '--port',     type=int, 
                                          help='''Port on host where Elasticsearch is running 
                                                  (default: 9200)''', 
                                          default=9200)
  parser.add_argument('-d', '--days',     type=int, 
                                          help='''How many days of logs to search. If the script fails to return 
                                                  data, try increasing the default value here 
                                                  (default: 3)''', 
                                          default=3)
  parser.add_argument('-v', '--verbose',  action='store_true',
                                          help='Display verbose information')
  
  args = parser.parse_args()

  if args.verbose:
      log.basicConfig(format="%(levelname)s: %(message)s", level=log.DEBUG)
  else:
      log.basicConfig(format="%(levelname)s: %(message)s")
  
  # Set the current time and the point in time to start searching from
  current_time=datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.000Z')
  start_time=(datetime.utcnow() - timedelta(days=args.days)).strftime('%Y-%m-%dT%H:%M:%S.000Z')

  # Calculate how many 
  total_successful_range_repairs = calculate_range_repairs(args.host, args.port, args.node, start_time, current_time)
  total_to_repair = calculate_total_ranges(args.host, args.port, args.node, start_time, current_time)

  # Print totals
  print_totals(args.node, args.days, total_successful_range_repairs, total_to_repair)


if __name__ == '__main__': main()
