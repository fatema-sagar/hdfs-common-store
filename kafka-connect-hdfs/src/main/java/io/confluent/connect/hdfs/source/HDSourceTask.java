/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.hdfs.source;

import io.confluent.connect.utils.Version;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class HDSourceTask extends SourceTask {
  /*
    Your connector should never use System.out for logging. All of your classes should use slf4j
    for logging
  */
  static final Logger log = LoggerFactory.getLogger(HDSourceTask.class);

  HDSourceConnectorConfig config;

  @Override
  public String version() {
    return Version.forClass(this.getClass());
  }

  @Override
  public void start(Map<String, String> settings) {
    config = new HDSourceConnectorConfig(settings);

    //TODO: Do things here that are required to start your task.
    //This could be open a connection to a database, etc.
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    //TODO: Create SourceRecord objects that will be sent the kafka cluster.
    // Throw ConnectException if there's a failure
    // Throw RetriableException if Connect should try again
    return Collections.emptyList();
  }

  @Override
  public void stop() {
    //TODO: Do whatever is required to stop your task.
  }
}