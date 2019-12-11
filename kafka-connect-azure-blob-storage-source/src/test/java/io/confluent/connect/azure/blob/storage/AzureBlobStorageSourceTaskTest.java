/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.azure.blob.storage;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.Before;
import org.junit.Test;

import io.confluent.connect.utils.licensing.LicenseConfigUtil;

public class AzureBlobStorageSourceTaskTest extends AzureBlobStorageSourceConnectorTestBase {
  
  private static final String BYTE_ARRAY_FORMAT_CLASS = "io.confluent.connect.cloud.storage.source.format.ByteArrayFormat";
  private static final String JSON_FORMAT_CLASS = "io.confluent.connect.cloud.storage.source.format.JsonFormat";

  Map<String, String> settings;
  AzureBlobStorageSourceTask task;

  @Before
  public void before() throws Exception {
    super.setUp();
    settings = new HashMap<>();
    //settings.put(AzureBlobStorageSourceConnectorConfig.MY_SETTING_CONFIG, "local://localhost:2876");
    settings.put(LicenseConfigUtil.CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    task = new AzureBlobStorageSourceTask();
    //task.config = new AzureBlobStorageSourceConnectorConfig(settings);
    task = setupTask(properties);
  }

  static final String KAFKA_TOPIC = "topic";

  @Test
  public void shouldReturnNonNullVersion() {
    assertNotNull(task.version());
  }

  @Test
  public void shouldStopAndDisconnect() {
    task.stop();
  }
  
  @Test
  public void testTaskType() throws Exception {
    setUp();
    task = new AzureBlobStorageSourceTask();
    SourceTask.class.isAssignableFrom(task.getClass());
  }

  @Test
  public void version() {
    assertNotNull(task.version());
    assertFalse(task.version().equals("0.0.0.0"));
    assertTrue(task.version().matches("^(\\d+\\.)?(\\d+\\.)?(\\*|\\d+)(-\\w+)?$"));
  }
  
  @Test
  public void shouldHaveDefaultConstructor() {
    AzureBlobStorageSourceTask aureSourceTask = new AzureBlobStorageSourceTask();
    assertNotNull(aureSourceTask);
  }

  @Test(expected = ConnectException.class)
  public void shouldThrowExceptionIfBucketDoesNotExist() {
    AzureBlobSourceStorage mockStorage = mock(AzureBlobSourceStorage.class);
    when(mockStorage.bucketExists()).thenReturn(false);

    AzureBlobStorageSourceTask task = setupTask(mockStorage);
    task.doStart(noContext());
  }
  
  private AzureBlobStorageSourceTask setupTask(AzureBlobSourceStorage storage) {
    final AzureBlobStorageSourceTask task = new AzureBlobStorageSourceTask(properties, storage);

    return task;
  }
  
  private AzureBlobStorageSourceTask setupTask(Map<String, String> overrides) {
    final HashMap<String, String> propsWithOverrides = new HashMap<>(properties);
    propsWithOverrides.putAll(overrides);

    final AzureBlobStorageSourceTask task = new AzureBlobStorageSourceTask(propsWithOverrides, storage);

    return task;
  }
  
  private SourceTaskContext noContext() {
    OffsetStorageReader mockOffsetStorageReader = mock(OffsetStorageReader.class);
    when(mockOffsetStorageReader
        .offset(Collections.singletonMap("folder", AZURE_TEST_FOLDER_TOPIC_NAME)))
        .thenReturn(null);

    SourceTaskContext mockSourceTaskContext = mock(SourceTaskContext.class);
    when(mockSourceTaskContext.offsetStorageReader()).thenReturn(mockOffsetStorageReader);
    return mockSourceTaskContext;
  }

}