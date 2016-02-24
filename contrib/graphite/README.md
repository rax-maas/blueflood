Blueflood Finder
================

The BF finder is the graphite plugin that allows graphite and grafana to use blueflood as a backend.

### Setup

Get the [blueflood](https://github.com/rackerlabs/blueflood) repo from github. Execute the following commands

    cd $BLUEFLOOD_REPO_LOCATION/contrib/graphite
    virtualenv bf-finder
    source bf-finder/bin/activate
    pip install .


###Tests

After finishing setup above you can do the below command to install test dependencies.

    pip install -r test_requirements.txt

The auth tests require env variables to be set as follows:

       For no-auth tests:

       NO_AUTH_URL=<no auth url>
       NO_AUTH_TENANT=836986

       For auth tests:

       AUTH_URL=https://staging.metrics.api.rackspacecloud.com
       AUTH_TENANT=887463
       AUTH_USER_NAME=cmetrixqe_cmsvcadmin
       AUTH_API_KEY=<cmetrixqe_cmsvcadmin's api key>


To run test, run the below command

    coverage run --source . tests.py; coverage html -d /tmp/test

To run tests you can also run

    nosetests -v
