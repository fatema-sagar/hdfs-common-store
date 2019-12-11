/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.s3.source;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import com.amazonaws.services.s3.model.SSECustomerKey;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class S3SourceConnectorConfigTest {

  private static final String AVRO_FORMAT_CLASS = "io.confluent.connect.s3.format.avro.AvroFormat";
  private Map<String, String> settings;
  private S3SourceConnectorConfig config;

  @Before
  public void before() {
    settings = minimumSettings();
    config = null;
  }

  @Test
  public void shouldValidateWithMinimumAllowedConfigSettings() {
    config = new S3SourceConnectorConfig(settings);
    assertNotNull(config);
  }

  @Test(expected = ConfigException.class)
  public void shouldInvalidateIfMinimumConfigSettingsNotSet() {
    settings.remove("s3.bucket.name");
    config = new S3SourceConnectorConfig(settings);
  }

  @Test
  public void shouldReturnCredentialsProvider() {
    config = new S3SourceConnectorConfig(settings);

    AWSCredentialsProvider credentialsProvider = config.getCredentialsProvider();

    assertNotNull(credentialsProvider);
  }

  @Test
  public void shouldReturnIsExpectedContinueToBeTrue() {
    settings.put("s3.http.send.expect.continue", "true");
    config = new S3SourceConnectorConfig(settings);

    boolean useExpectContinue = config.useExpectContinue();

    assertThat(useExpectContinue, is(true));
  }

  @Test
  public void shouldReturnIsExpectedContinueToBeFalse() {
    settings.put("s3.http.send.expect.continue", "false");
    config = new S3SourceConnectorConfig(settings);

    boolean useExpectContinue = config.useExpectContinue();

    assertThat(useExpectContinue, is(false));
  }

  @Test
  public void shouldReturnSsea() {
    String sseaName = "ssea-name";
    settings.put("s3.ssea.name", sseaName);
    config = new S3SourceConnectorConfig(settings);

    String ssea = config.getSsea();

    assertThat(ssea, is(sseaName));
  }

  @Test
  public void shouldReturnBucketName() {
    String bucketName = "bucket-name";
    settings.put("s3.bucket.name", bucketName);
    config = new S3SourceConnectorConfig(settings);

    String ssea = config.getBucketName();

    assertThat(ssea, is(bucketName));
  }

  @Test
  public void shouldReturnS3PartRetries() {
    settings.put("s3.part.retries", "5");
    config = new S3SourceConnectorConfig(settings);

    int s3PartRetries = config.getS3PartRetries();

    assertThat(s3PartRetries, is(5));
  }

  @Test
  public void shouldGetCustomerKey() {
    SSEAlgorithm algorithm = SSEAlgorithm.AES256;
    settings.put("s3.ssea.name", algorithm.getAlgorithm());
    settings.put("s3.sse.customer.key", "my-password");
    config = new S3SourceConnectorConfig(settings);

    SSECustomerKey customerKey = config.getCustomerKey();
    assertNotNull(customerKey);
    assertThat(customerKey.getAlgorithm(), is(algorithm.getAlgorithm()));
  }

  @Test
  public void shouldReturnNullForInvalidCustomerKeyAlgorithmn() {
    SSEAlgorithm algorithm = SSEAlgorithm.KMS;
    settings.put("s3.ssea.name", algorithm.getAlgorithm());
    config = new S3SourceConnectorConfig(settings);

    SSECustomerKey customerKey = config.getCustomerKey();
    assertNull(customerKey);
  }


  @Test
  public void shouldPrintConfig() {
    S3SourceConnectorConfig.main(new String[0]);
  }

  @Test
  public void shouldGetLineSeparator() {
    String lineSeparator = System.lineSeparator();
    settings.put("format.bytearray.separator", lineSeparator);
    config = new S3SourceConnectorConfig(settings);

    String separator = config.getFormatByteArrayLineSeparator();

    assertThat(separator, is(lineSeparator));
  }

  private Map<String, String> minimumSettings() {
    Map<String, String> minSettings = new HashMap<>();
    minSettings.put("confluent.topic.bootstrap.servers", "localhost:9092");
    minSettings.put("s3.bucket.name", "my-bucket");
    minSettings.put("format.class", AVRO_FORMAT_CLASS);
    return minSettings;
  }
}