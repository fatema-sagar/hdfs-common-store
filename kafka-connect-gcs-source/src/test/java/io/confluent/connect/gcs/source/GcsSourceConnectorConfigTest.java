/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.gcs.source;

import org.apache.kafka.common.config.ConfigException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.auth.oauth2.GoogleCredentials;

public class GcsSourceConnectorConfigTest extends GcsTestUtils {

  private Map<String, String> settings;
  private GcsSourceConnectorConfig config;

  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @Before
  public void before() {
    settings = getCommonConfig();
    config = new GcsSourceConnectorConfig(settings);
  }

  @Test(expected = ConfigException.class)
  public void credentialsMustBePresent() throws IOException {
    settings.remove("gcs.credentials.path");
    config = new GcsSourceConnectorConfig(settings);
    config.getCredentials();
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowEmptyBucketName() {
    settings.remove(BUCKECT_PROPRTY);
    new GcsSourceConnectorConfig(settings);
  }

  /**
   * Currently this test is checking for ConfigException.
   * 
   * The credential files present at src/test/resources contains GCS credential files
   * To make this test pass without exception you will have to update those credential files.
   * 
   * After updating gcpcreds.properties and gcpcreds.json files with valid credentials, remove
   * (expected = ConfigException.class) from @Test annotation and run the test again. 
   * It should pass successfully.
   */
  @Test(expected = ConfigException.class)
  public void shouldReturnCredentialsProvider() throws IOException {
    GoogleCredentials credentialsProvider = config.getCredentials();
    assertNotNull(credentialsProvider);
  }

  @Test
  public void shouldReturnConfiguredBucket() {
    assertEquals(config.getBucketName(), BUCKECT_NAME);
  }

  /**
   * Currently this test is checking for ConfigException.
   * 
   * The credential files present at src/test/resources contains GCS credential files
   * To make this test pass without exception you will have to update those credential files.
   * 
   * After updating gcpcreds.properties and gcpcreds.json files with valid credentials, remove
   * (expected = ConfigException.class) from @Test annotation and run the test again. 
   * It should pass successfully.
   */
  @Test(expected = ConfigException.class)
  public void validatePropertyBasedCredentials() throws IOException {
    settings.put("gcs.credentials.path", PATH_TO_CREDENTIALS_PROPS);
    config = new GcsSourceConnectorConfig(settings);
    GoogleCredentials credentialsProvider = config.getCredentials();
    assertNotNull(credentialsProvider);
  }

  /**
   * Currently this test is checking for ConfigException.
   * 
   * The credential files present at src/test/resources contains GCS credential files
   * To make this test pass without exception you will have to update those credential files.
   * 
   * After updating gcpcreds.properties and gcpcreds.json files with valid credentials, remove
   * (expected = ConfigException.class) from @Test annotation and run the test again. 
   * It should pass successfully.
   */
  @Test(expected = ConfigException.class)
  public void validateEnvironmentBasedCredentials() throws IOException {
    environmentVariables.set("GOOGLE_APPLICATION_CREDENTIALS", PATH_TO_CREDENTIALS_JSON);
    settings.remove("gcs.credentials.path");
    config = new GcsSourceConnectorConfig(settings);
    GoogleCredentials credentialsProvider = config.getCredentials();
    assertNotNull(credentialsProvider);
  }
}