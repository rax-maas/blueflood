enum_metrics python utilities
=============================

This project contains some python utilities to work with enum metrics. 

### Setup

Get the [blueflood](https://github.com/rackerlabs/blueflood) repo from github. Execute the following commands

    cd $BLUEFLOOD_REPO_LOCATION/contrib/enum_metrics
    virtualenv enums
    source enums/bin/activate
    pip install .

Now you can run the ``enum_metrics/errant_enums.py`` script with the appropriate options.

### Clear errant enums
Blueflood stores the enum metrics with more than allowed number of enum values in metrics_excess_enums column family. 
This utility can be used to remove that row from the excess enums table in Cassandra and the document from the 
"enums" and "metrics_metadata" indexes in ElasticSearch.

Note that data from cassandra and elastic seach will be removed only if the metric is already classified as an
excess enum (i.e. if its present in metrics_excess_enums table)

    
    usage: errant_enums.py [-h] {list,delete} ...

    Script to delete errant enums
    
    positional arguments:
      {list,delete}  commands
        list         List all excess enums
        delete       Delete errant enum
    
    optional arguments:
      -h, --help     show this help message and exit

####Delete command

    usage: errant_enums.py delete [-h] [--dryrun] -m METRICNAME -t TENANTID
                                  [-e {localhost}]
    
    optional arguments:
      -h, --help            show this help message and exit
      --dryrun              Display errant enums related data for the given metric
                            name.
      -m METRICNAME, --metricName METRICNAME
                            metric name to be deleted
      -t TENANTID, --tenantId TENANTID
                            tenantId corresponding to the metric name to be
                            deleted
      -e {localhost}, --env {localhost}
                            Environment we are pointing to

Example Usage:
 
    python enum_metrics/errant_enums.py delete -m mytest.enum.excess -t 836986 --dryrun


####List command

    usage: errant_enums.py list [-h] [-e {localhost}]
    
    optional arguments:
      -h, --help            show this help message and exit
      -e {localhost}, --env {localhost}
                            Environment we are pointing to


Example Usage:
 
    python enum_metrics/errant_enums.py list    
    
###Tests

To run tests, you can simply do:    
    
    nosetests
    
###Packaging and distributing code
    
To package code, use the following commands.     

    cd $BLUEFLOOD_REPO_LOCATION/contrib/enum_metrics
    python setup.py sdist
    
This will generate a file ``$BLUEFLOOD_REPO_LOCATION/contrib/enum_metrics/dist/enum_metrics-x.y.z.tar.gz``. SCP this 
to the destination folder and enter the following commands.
     
     tar -zxvf enum_metrics-x.y.z.tar.gz
     cd enum_metrics-x.y.z
     
Follow the instructions under setup to run the ``enum_metrics/errant_enums.py`` script.     