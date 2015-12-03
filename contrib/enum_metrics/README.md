enum_metrics python utilities
=============================

This project contains some python utilities to work with enum metrics. 

### Setup

Get the [blue flood](https://github.com/rackerlabs/blueflood) repo from github. Execute the following commands

```
cd $BLUEFLOOD_REPO_LOCATION/contrib/enum_metrics
virtualenv enums
source enums/bin/activate
pip install .
```

### Clear errant enums
Blueflood stores the enum metrics with more than allowed number of enum values in metrics_excess_enums column family. 
This utility can be used to remove that row from the excess enums table in Cassandra and the document from the 
"enums" and "metrics_metadata" indexes in ElasticSearch.

Note that data from cassandra and elastic seach will be removed only if the metric is already classified as an
excess enum (i.e. if its present in metrics_excess_enums table)


    
    usage: clear_errant_enums.py [-h] [--dryrun] -m METRICNAME -t TENANTID
    
    Script to delete errant enums
    
    optional arguments:
      -h, --help            show this help message and exit
      --dryrun              Display errant enums related data for the given metric
                            name.
      -m METRICNAME, --metricName METRICNAME
                            metric name to be deleted
      -t TENANTID, --tenantId TENANTID
                            tenantId corresponding to the metric name to be
                            deleted

 Example Usage:
 
    python enum_metrics/clear_errant_enums.py -m mytest.enum.excess -t 836986 --dryrun