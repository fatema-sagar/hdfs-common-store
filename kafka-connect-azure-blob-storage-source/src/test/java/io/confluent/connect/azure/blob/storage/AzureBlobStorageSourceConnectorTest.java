/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.azure.blob.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.source.SourceConnector;
import org.junit.Before;
import org.junit.Test;

import io.confluent.connect.azure.blob.storage.format.json.JsonFormat;
import io.confluent.connect.utils.licensing.ConnectLicenseManager;

public class AzureBlobStorageSourceConnectorTest extends AzureBlobStorageSourceConnectorTestBase {

  Map<String, String> settings;
  AzureBlobStorageSourceConnector connector;
  ConnectLicenseManager licenseMgr;

  @Before
  public void before() throws Exception {
    super.setUp();
    settings = new HashMap<>();
    // Use a mocked license manager by default
    licenseMgr = mock(ConnectLicenseManager.class);
    AzureBlobSourceStorage mockStorage = mock(AzureBlobSourceStorage.class);
    connector = new AzureBlobStorageSourceConnector(mockStorage, licenseMgr);
    connector.licenseManager = licenseMgr;

    settings.put("format.class", JsonFormat.class.getName());
    settings.put("flush.size", "1000");
    settings.put("storage.class", AzureBlobSourceStorage.class.getName());
    settings.put("confluent.topic.bootstrap.servers", "localhost:9092");
    settings.put("azblob.account.name", "default");
    settings.put("azblob.account.key", "password");
  }

  @Test
  public void shouldReturnNonNullVersion() {
    assertNotNull(connector.version());
  }

  @Test
  public void connectorType() {
    assertTrue(SourceConnector.class.isAssignableFrom(connector.getClass()));
  }

  
  @Test 
  public void shouldStartWithoutError() { 
    startConnector();
    verify(connector.licenseManager, times(1)).registerOrValidateLicense(); 
  }
  
  @Test
  public void shouldHaveAnEmptyConstructorAndReturnConfig() {
    ConfigDef configDef = new AzureBlobStorageSourceConnector().config();
    assertNotNull(configDef);
  }

  @Test
  public void shouldReturnSourceTask() {
    assertEquals(AzureBlobStorageSourceTask.class, connector.taskClass());
  }

  @Test 
  public void shouldStartAndStop() { 
    startConnector(); 
    connector.stop();
  }

  @Test
  public void shouldNotHaveNullConfigDef() {
    // ConfigDef objects don't have an overridden equals() method; just make sure it's non-null
    assertNotNull(connector.config());
  }

  /**
   * Start the connector, via the {@link AzureBlobStorageSinkConnector#doStart()} method that uses
   * our mock license manager.
   */
  protected void startConnector() {
    connector.setConfig(properties);
    connector.doStart();
  }
  
}