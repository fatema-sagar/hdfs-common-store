/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.hdfs.source;

import io.confluent.connect.hdfs.source.HDSourceConnectorConfig;
import io.confluent.connect.hdfs.source.HDSourceTask;
import io.confluent.connect.utils.licensing.LicenseConfigUtil;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class HDSourceTaskTest {

  Map<String, String> settings;
  HDSourceTask task;

  @Before
  public void before() {
    settings = new HashMap<>();
    settings.put(HDSourceConnectorConfig.MY_SETTING_CONFIG, "local://localhost:2876");
    settings.put(LicenseConfigUtil.CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    task = new HDSourceTask();
    task.config = new HDSourceConnectorConfig(settings);
  }

  static final String KAFKA_TOPIC = "topic";

  @Test
  public void shouldReturnNonNullVersion() {
    assertNotNull(task.version());
  }

  @Test
  public void shouldStopAndDisconnect() {
    task.stop();
    //TODO: Ensure the task stopped
  }

  @Test
  public void shouldProduceRecords() throws ConnectException, InterruptedException {
    final int count = 0;

    List<SourceRecord> records = task.poll();
    //assertNotNull("records should not be null", records);
    //assertEquals("number of records does not match", count, records.size());

    for (SourceRecord record : records) {
      //TODO: check the record key, value, and maybe headers
    }
  }

//  @Test
//  public void version() {
//    assertNotNull(task.version());
//    assertFalse(task.version().equals("0.0.0.0"));
//    assertTrue(task.version().matches("^(\\d+\\.)?(\\d+\\.)?(\\*|\\d+)(-\\w+)?$"));
//  }
}