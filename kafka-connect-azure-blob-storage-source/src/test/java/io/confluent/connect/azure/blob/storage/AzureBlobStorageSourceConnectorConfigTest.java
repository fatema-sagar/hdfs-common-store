/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.azure.blob.storage;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class AzureBlobStorageSourceConnectorConfigTest extends AzureBlobStorageSourceConnectorTestBase{

  private static final String VALID_ACCOUNT_NAME = "myaccount123";
  private static final String INVALID_ACCOUNT_NAME_TOO_LONG = "cmlvqsrpsn7lxyk1sd92wmzd";
  private static final String INVALID_ACCOUNT_NAME_TOO_SHORT = "no";
  private static final String INVALID_ACCOUNT_NAME_WITH_SPECIAL_CHARS = "my@ccount!23";
  private static final String INVALID_ACCOUNT_NAME_UPPERCASE = "MYACCOUNT123";
  private static final String VALID_CONTAINER_NAME = "my-container-123";
  private static final String INVALID_CONTAINER_NAME_TOO_LONG =
      "pjfctewakspkjbuipx8bpqtygujjslsk0524prcl63q9jweoqafre6urdidngdqd";
  private static final String INVALID_CONTAINER_NAME_TOO_SHORT = "no";
  private static final String INVALID_CONTAINER_NAME_SPECIAL_CHARS = "my#cont@iner!23";
  private static final String INVALID_CONTAINER_NAME_UPPERCASE = "MYACCOUNT123";
  @Rule
  public ExpectedException thrown = ExpectedException.none();
  private Map<String, String> settings;
  private AzureBlobStorageSourceConnectorConfig config;

  @Before
  public void setUp() throws Exception {
    settings = super.createProps();
    config = null;
  }

  @Test
  public void shouldAcceptValidConfig() {
    settings.put(AzureBlobStorageSourceConnectorConfig.AZ_RETRY_BACKOFF_MS_CONFIG, "10");
    config = new AzureBlobStorageSourceConnectorConfig(settings);
    assertNotNull(config);
  }

  @Test
  public void shouldAllowValidAccountName() {
    Map<String, String> newSettings = new HashMap<>(settings);
    newSettings.put("azblob.account.name", VALID_ACCOUNT_NAME);

    AzureBlobStorageSourceConnectorConfig config =
        new AzureBlobStorageSourceConnectorConfig(newSettings);

    assertThat(config.getAccountName(), is(VALID_ACCOUNT_NAME));
  }
  
  @Test(expected = ConfigException.class)
  public void shouldNotAllowAccountNameWithLessThan3Characters() {
    Map<String, String> newSettings = new HashMap<>(settings);
    newSettings.put("azblob.account.name", INVALID_ACCOUNT_NAME_TOO_SHORT);

    new AzureBlobStorageSourceConnectorConfig(newSettings);
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowAccountNameWithGreaterThan23Characters() {
    Map<String, String> newSettings = new HashMap<>(settings);
    // Generated a 24 character string
    newSettings.put("azblob.account.name", INVALID_ACCOUNT_NAME_TOO_LONG);

    new AzureBlobStorageSourceConnectorConfig(newSettings);
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowAccountNameWithSpecialCharacters() {
    Map<String, String> newSettings = new HashMap<>(settings);
    newSettings.put("azblob.account.name", INVALID_ACCOUNT_NAME_WITH_SPECIAL_CHARS);

    new AzureBlobStorageSourceConnectorConfig(newSettings);
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowAccountNameWithUppercaseCharacters() {
    Map<String, String> newSettings = new HashMap<>(settings);
    newSettings.put("azblob.account.name", INVALID_ACCOUNT_NAME_UPPERCASE);

    new AzureBlobStorageSourceConnectorConfig(newSettings);
  }

  @Test
  public void shouldAllowValidContainerName() {
    Map<String, String> newSettings = new HashMap<>(settings);
    // Alphanumeric with dashes allowed
    newSettings.put("azblob.container.name", VALID_CONTAINER_NAME);

    AzureBlobStorageSourceConnectorConfig config =
        new AzureBlobStorageSourceConnectorConfig(newSettings);

    assertThat(config.getContainerName(), is(VALID_CONTAINER_NAME));
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowContainerNameWithLessThan3Characters() {
    Map<String, String> newSettings = new HashMap<>(settings);
    newSettings.put("azblob.container.name", INVALID_CONTAINER_NAME_TOO_SHORT);

    new AzureBlobStorageSourceConnectorConfig(newSettings);
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowContainerNameWithGreaterThan63Characters() {
    Map<String, String> newSettings = new HashMap<>(settings);
    // Generated a 64 character string
    newSettings.put("azblob.container.name", INVALID_CONTAINER_NAME_TOO_LONG);

    new AzureBlobStorageSourceConnectorConfig(newSettings);
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowContainerNameWithSpecialCharacters() {
    Map<String, String> newSettings = new HashMap<>(settings);
    // Generated a 64 character string
    newSettings.put("azblob.container.name", INVALID_CONTAINER_NAME_SPECIAL_CHARS);

    new AzureBlobStorageSourceConnectorConfig(newSettings);
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowContainerNameWithUppercaseCharacters() {
    Map<String, String> newSettings = new HashMap<>(settings);
    // Generated a 64 character string
    newSettings.put("azblob.container.name", INVALID_CONTAINER_NAME_UPPERCASE);

    new AzureBlobStorageSourceConnectorConfig(newSettings);
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowNullAccountKey() {
    Map<String, String> newSettings = new HashMap<>(settings);
    newSettings.put("azblob.account.key", null);

    new AzureBlobStorageSourceConnectorConfig(newSettings);
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowInvalidProxyURI() {
    Map<String, String> newSettings = new HashMap<>(settings);
    newSettings.put("azblob.proxy.url", "_abc/");

    new AzureBlobStorageSourceConnectorConfig(newSettings);
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowInvalidProxySchemeUri() {
    Map<String, String> newSettings = new HashMap<>(settings);
    newSettings.put("azblob.proxy.url", "ftp://localhost");

    new AzureBlobStorageSourceConnectorConfig(newSettings);
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowInvalidProxyWithoutHost() {
    Map<String, String> newSettings = new HashMap<>(settings);
    newSettings.put("azblob.proxy.url", "http://");

    new AzureBlobStorageSourceConnectorConfig(newSettings);
  }

  @Test
  public void shouldAllowSchemeAndHosts() {
    Map<String, String> newSettings = new HashMap<>(settings);
    newSettings.put("azblob.proxy.url", "http://localhost:3000");

    new AzureBlobStorageSourceConnectorConfig(newSettings);
  }

  @Test()
  public void shouldReturnDefaultStorageURL(){
    Map<String, String> newSettings = new HashMap<>(settings);
    newSettings.put("azblob.account.name", "testaccount");

    AzureBlobStorageSourceConnectorConfig config =
        new AzureBlobStorageSourceConnectorConfig(newSettings);

    assertEquals(config.getUrl(), "https://testaccount.blob.core.windows.net");
  }

  @Test()
  public void shouldReturnStorageURL(){
    Map<String, String> newSettings = new HashMap<>(settings);
    newSettings.put("store.url", "http://localhost");

    AzureBlobStorageSourceConnectorConfig config =
        new AzureBlobStorageSourceConnectorConfig(newSettings);

    assertEquals(config.getUrl(), "http://localhost");
  }
  
}