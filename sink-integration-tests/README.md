# Sink - Source Integration Test Module

This project provides mechanisim which enables you to run sink and source connectors in tandem
. Idea is to run the sink connector to generate the files of the records pushed into Kafka and
 then run the source to read the generate files of the sink and push it back into Kafka. This
  validates/verifies that your sink and source works in sync.

# Need for this module.

There exists situations where there are common classes across the sink and source connectors, but
 their implementations differ. Therefore, if you were to include both the dependencies into the
  same project, you run into classpath issues. 
  
To overcome this, we have this moduke `sink-integration-tests` which basically houses only the
 sink dependencies. Here you would have your sink to start running and generate the records. Now
  in the respective source modules you would have the equivalent tests which picks up from where
   the sink had left off. Detailed example below. 

# Development/Testing the Integration
Assume you would need to write the sink + source integration tests.

Sink: 
- Under the `sink-integration-module`, create a package under `test` folder and create mechanisim
 to run the sink by providing the connector configurations. 
- You can use `RunSink` which is a generic class which can be re-used across the various
 connectors.
- For GCS, the implemented Class to run the sink is `SinkIT`.
- For HDFS3, the implemented Class to run the sink is `Hdfs3SinkIT`.
  
Source:
- Under the respective cloud sub module, create a way to run the Source connector.
- We have `SourceIT` class under `kafka-connect-gcs-source` sub module which runs the Source
 Integration after the sink. 

Running everything together:
- Update your credentials under the `creds` folder. Ex: for GCP, update `gcpcreds.json`
- Update the script under `bin/scriptsIT/runITtests.sh`
- Execute `sh runITtests.sh`
- If the scripts exists normally, your integration works as expected.

#Miscellaneous
- You can add multiple scripts under `scriptsIT` folder to run individual storage connector
 seperately.
- You can design different flavours to run the integration. Ex: In GCS, we run the sink for all
 the partitioners and formatters post which is when we run the source connector for all the
  paritioners and formatters. You can have multiple tests which executes individual tests
   seperately as well, for example, have a script which only verifies default paritioners etc.
   
#Running HDFS3 Sink-source integeration test
-For running Hdfs3-sink tests you have to update the `hdfs.url` property in /sink-integration-tests/scriptsIT/properties/hdfs3.properties
-For running Hdfs3SourceIT tests you have to update the `hdfs.url` property in /kafka-connect-hdfs3-source/src/test/resources/hdfs3.properties

Running Hdfs3ITtests script:
- Update Hdfs3 properties under the `properties` folder. Ex: for HDFS url, update `hdfs3.url`
- Execute `sh runHdfs3ITtests.sh`
- If the scripts exists normally, your integration works as expected.