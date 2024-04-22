## Blueflood Repose Spock Tests

This module is written to test the Repose various filters functionalities we are using in ingest and query nodes.
This test can be run against local vagrant or stage or prod blueflood endpoints


## Pre-requisites
We need below environment variables to be set before running the respose tests:

* TENANT_ID - It's your cloud account number. For dedicated users, it's the core account number prefixed with hybrid:.
* X-Auth-Token - Rackspace identity token to work with keystone-v2 filter config
* RATE_LIMIT_COUNT (Optional) - If this is set, we will be hitting request to this no of times to verify we reached the threshold of no of request in a minute.

    This is used to test against vagrant or staging with configurable rate limit filters. If this is not set, then teh rate limiting test case will be ignored.

* ENV_TO_RUN - local/stage/prod (Defaults to stage)


### Running

You can easily run the Blueflood repose tests by running the command below:

    mvn test -P respose-tests

This will run all the unit test cases and repose spock tests also.

However, if we write any other spock unit test in other moudule, they will run with unit tests without specifying this profile.


### More Spock Unit Tests

We had included spock and groovy dependencies in parent pom xml file. 
Also groovy compiler plugin is in parent pom xml file. 

So in future if we need to write spock unit test in any module, we need not make any change related to groovy/spock dependencies or plugin.
