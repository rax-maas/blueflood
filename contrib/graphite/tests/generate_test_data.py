import os
import json
import time
from random import randint
import requests

tenant_id = 836986

num_parent_elements = 1
num_grand_children = 3

child_element_suffix = ['A', 'B', 'C']
num_docs = num_parent_elements * len(child_element_suffix) * num_grand_children

bf_ingest_url = 'http://localhost:19000/v2.0/836986/ingest/multi'
bf_enum_ingest_url = 'http://localhost:19000/v2.0/836986/ingest/aggregated/multi'
bf_query_url = 'http://localhost:20000/v2.0/836986/metrics/search?include_enum_values=true'


def generate_metric_names():
    # The below code generate metric names's which match the below regex.
    #         one\.two\.three00\.four[A-C]\.five[0-2]
    #
    # Examples:
    #         one.two.three00.fourA.five0
    #         one.two.three00.fourB.five2
    metric_names = []
    for i in xrange(0, num_parent_elements):
        for j in child_element_suffix:
            for k in xrange(0, num_grand_children):
                metric_names.append("one.two.three%02d.four%s.five%s" % (i, j, k))

    # Additional metrics
    metric_names.append('one.two.three00.fourA')
    metric_names.append('one.two.three00.fourD')

    return metric_names


def generate_enum_metrics():
    metric_data = [('one.two.three00.fourA.five100', ['ev1-1', 'ev2-1']),
                   ('one.two.three00.fourD.five100', ['ev1-2', 'ev2-2']),
                   ('one.two.three00.fourE',         ['ev1', 'ev2']),
                   ('one.two.three00',               ['fourA', 'fourB']),
                   ('foo1.bar2',                     ['ev1', 'ev2'])]

    # metric with enum values
    return metric_data


def ingest_metric_data(url, payload):
    try:
        r = requests.post(url, data=json.dumps(payload))
        if r.status_code == 200:
            print("Success: Ingestion of test metrics")
        else:
            print "Failure: Ingestion of test metrics to endpoint [%s] failed with return code [%s]" % \
                  (url, r.status_code)
    except requests.exceptions.RequestException as e:
        print e


def insert_metrics(metric_names):
    print "Ingesting regular metrics of size %s" % len(metric_names)
    epoch_time = get_epoch_time() - 500

    payload = []
    for metric_name in metric_names:
        payload.append({
            "tenantId": tenant_id,
            "collectionTime": epoch_time,
            "ttlInSeconds": 172800,
            "metricValue": randint(1, 100),
            "metricName": metric_name
        })

        epoch_time += 1

    print payload
    ingest_metric_data(bf_ingest_url, payload)


def get_epoch_time():
    return int(round(time.time() * 1000))


def insert_enum_metrics():
    epoch_time = get_epoch_time()
    metric_data = generate_enum_metrics()

    print "Ingesting enum metrics of size %s" % len(metric_data)
    payload = []
    for (metric_name, enum_values) in metric_data:
        enums_list = []
        for enum_value in enum_values:
            enums_list.append({
                "name": metric_name,
                "value": enum_value
            })
        payload.append({
            "tenantId": tenant_id,
            "timestamp": epoch_time,
            "enums": enums_list
        })

        epoch_time += 1

    print payload
    ingest_metric_data(bf_enum_ingest_url, payload)


if __name__ == '__main__':
    insert_metrics(generate_metric_names())
    insert_enum_metrics()
