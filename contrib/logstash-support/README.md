# Blueflood Logstash Support
Blueflood can be configured to send its application logging to Logstash. This contrib module
lets you build a logstash support jar file that enables blueflood to send logs to Logstash.
Simply add the jar file in the classpath when you start blueflood.

## How To Build
```
cd contrib/logstash-support
mvn clean package
```
The resulting jar file should be in this project's ```target/``` directory.

## Updating 3rd party jar files

### logit
We use [logit](http://github.com/stuart-warren/logit) 3rd party jar file. Unfortunately, this 3rd party jar is not Maven artifact, is not hosted in any Maven repository. If you need to update it, follow these instructions:

* Download logit
```
wget https://github.com/stuart-warren/logit/releases/download/v0.5.11/logit-0.5.11-jar-with-dependencies.jar
```

* Install it as an artifact in local repository
```
mvn install:install-file -Dfile=target/logit/logit-0.5.11.jar -DgroupId=com.stuartwarren -DartifactId=logit -Dversion=0.5.11 -Dpackaging=jar -DlocalRepositoryPath=./mavenrepo
```

* Build and check in your files

