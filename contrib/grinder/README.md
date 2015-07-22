# Blueflood Grinder Integration
##Intro
Grinder is a distributed load testing tool described [here](http://grinder.sourceforge.net/g3/getting-started.html)

This code defines implementations of grinder worker threads meant to repeatedly invoke the required number of BF api calls during each "reporting interval"

It also includes the infrastructure to divide the total work described in the grinder properties file across all the workers in the distributed system.

##Installing
```bash
cd /tmp
wget http://iweb.dl.sourceforge.net/project/grinder/The%20Grinder%203/3.11/grinder-3.11-binary.zip
wget http://opensource.xhaus.com/attachments/download/3/jyson-1.0.2.zip
cd $BLUEFLOOD_INSTALL/contrib/grinder
mkdir resources
cd resources
unzip /tmp/grinder-3.11-binary.zip
unzip /tmp/jyson-1.0.2.zip
```

Note this needs to be run on each node in the cluster, as well as the console.

##Starting the console
The GUI is started like so:
```bash
cd $BLUEFLOOD_INSTALL/contrib/grinder
java -cp  resources/grinder-3.11/lib/grinder.jar:resources/jyson-1.0.2/lib/jyson-1.0.2.jar net.grinder.Console
```
The console can be run headless, like so:
```bash
java -cp  resources/grinder-3.11/lib/grinder.jar:resources/jyson-1.0.2/lib/jyson-1.0.2.jar net.grinder.Console -headless
```

and you interact with a rest api like so:
```bash
curl -X POST http://localhost:6373/agents/stop-workers
curl -X POST http://localhost:6373/agents/start-workers
```
The graphical console gives some useful status info, so you may prefer using that.


##Starting the agents
Each agent is started like so:
```bash
java -cp  resources/grinder-3.11/lib/grinder.jar:resources/jyson-1.0.2/lib/jyson-1.0.2.jar net.grinder.Grinder $GRINDER_PROPERTIES_FILE
```
There are currently some example properties files here:
```bash
$BLUEFLOOD_INSTALL/contrib/grinder/properties/
```

grinder.properties runs the unit tests
grinder.properties.local has some configs for running on your localhost
grinder.properties.staging runs the staging configuration and is meant to be run on two nodes

##Coverage tool
The coverage tool invocation is hardcoded into the tests.  (It's the only way I could get it
to work with Jython.)  To generate the corresponding html output, run:
```bash
coverage html -d /tmp/grinder
```
