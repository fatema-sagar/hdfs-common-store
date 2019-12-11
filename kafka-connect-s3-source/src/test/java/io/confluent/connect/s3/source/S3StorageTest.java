/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.s3.source;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.retry.PredefinedBackoffStrategies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Region;
import org.apache.kafka.common.config.types.Password;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class S3StorageTest {

  private S3Storage storage;
  private S3SourceConnectorConfig mockConfig;
  private AmazonS3 mockS3;
  private String testUrl = "my-storage-url";
  private String testBucket = "my-bucket";

  @Before
  public void setup() {
    mockConfig = mock(S3SourceConnectorConfig.class);
    mockS3 = mock(AmazonS3.class);
    storage = new S3Storage(mockConfig, testUrl, testBucket, mockS3);
  }

  @Test
  public void shouldCreateNewClientWithoutUrlSpecifiedAndNonStandardRegion() {
    String proxyProtocol = "http://";
    String proxyHost = "proxyUrl";
    String proxyUrl = proxyProtocol + proxyHost;
    String proxyUser = "me";
    String proxyPass = "secret";
    String region = "us-west-2";
    boolean expectContinue = true;
    long backoffMs = 1000L;

    when(mockConfig.getString("s3.proxy.url")).thenReturn(proxyUrl);
    when(mockConfig.getString("s3.proxy.user")).thenReturn(proxyUser);
    when(mockConfig.getPassword("s3.proxy.password")).thenReturn(new Password(proxyPass));

    when(mockConfig.useExpectContinue()).thenReturn(expectContinue);
    when(mockConfig.getLong("s3.retry.backoff.ms")).thenReturn(backoffMs);
    when(mockConfig.getString("s3.region")).thenReturn(region);

    S3Storage storage = new S3Storage(mockConfig, null, testBucket, mockS3);
    AmazonS3 client = storage.newS3Client(mockConfig);
    assertNotNull(client);

    assertThat(client.getRegion(), is(Region.US_West_2));
  }

  // Due to SDK not setting region string for the standard region (us-east-1) there needs to be
  // a special case to handle this hence this test
  @Test
  public void shouldCreateNewClientWithoutUrlSpecifiedAndStandardRegion() {
    String proxyProtocol = "http://";
    String proxyHost = "proxyUrl";
    String proxyUrl = proxyProtocol + proxyHost;
    String proxyUser = "me";
    String proxyPass = "secret";
    String region = "us-east-1";
    boolean expectContinue = true;
    long backoffMs = 1000L;

    when(mockConfig.getString("s3.proxy.url")).thenReturn(proxyUrl);
    when(mockConfig.getString("s3.proxy.user")).thenReturn(proxyUser);
    when(mockConfig.getPassword("s3.proxy.password")).thenReturn(new Password(proxyPass));

    when(mockConfig.useExpectContinue()).thenReturn(expectContinue);
    when(mockConfig.getLong("s3.retry.backoff.ms")).thenReturn(backoffMs);
    when(mockConfig.getString("s3.region")).thenReturn(region);

    S3Storage storage = new S3Storage(mockConfig, null, testBucket, mockS3);
    AmazonS3 client = storage.newS3Client(mockConfig);
    assertNotNull(client);

    assertThat(client.getRegion(), is(Region.US_Standard));
  }

  @Test
  public void shouldConstructS3StorageWithoutError() {
    boolean expectContinue = true;
    long backoffMs = 1000L;

    when(mockConfig.useExpectContinue()).thenReturn(expectContinue);
    when(mockConfig.getLong("s3.retry.backoff.ms")).thenReturn(backoffMs);

    new S3Storage(mockConfig, testBucket);
  }

  @Test
  public void shouldCreateNewClientBasedOnConfig() {
    String proxyProtocol = "http://";
    String proxyHost = "proxyUrl";
    String proxyUrl = proxyProtocol + proxyHost;
    String proxyUser = "me";
    String proxyPass = "secret";
    boolean expectContinue = true;
    long backoffMs = 1000L;

    when(mockConfig.getString("s3.proxy.url")).thenReturn(proxyUrl);
    when(mockConfig.getString("s3.proxy.user")).thenReturn(proxyUser);
    when(mockConfig.getPassword("s3.proxy.password")).thenReturn(new Password(proxyPass));

    when(mockConfig.useExpectContinue()).thenReturn(expectContinue);
    when(mockConfig.getLong("s3.retry.backoff.ms")).thenReturn(backoffMs);

    AmazonS3 client = storage.newS3Client(mockConfig);
    assertNotNull(client);
  }

  @Test
  public void shouldCreateNewClientConfiguration() {
    String proxyProtocol = "http://";
    String proxyHost = "proxyUrl";
    String proxyUrl = proxyProtocol + proxyHost;
    String proxyUser = "me";
    String proxyPass = "secret";
    boolean expectContinue = true;
    long backoffMs = 1000L;

    when(mockConfig.getString("s3.proxy.url")).thenReturn(proxyUrl);
    when(mockConfig.getString("s3.proxy.user")).thenReturn(proxyUser);
    when(mockConfig.getPassword("s3.proxy.password")).thenReturn(new Password(proxyPass));

    when(mockConfig.useExpectContinue()).thenReturn(expectContinue);
    when(mockConfig.getLong("s3.retry.backoff.ms")).thenReturn(backoffMs);

    ClientConfiguration clientConfiguration = storage.newClientConfiguration(mockConfig);

    assertThat(clientConfiguration.isUseExpectContinue(), is(expectContinue));
    assertThat(clientConfiguration.getProtocol(), is(Protocol.HTTP));
    assertThat(clientConfiguration.getProxyHost(), is(proxyHost));

    RetryPolicy.BackoffStrategy backoffStrategy =
        clientConfiguration.getRetryPolicy().getBackoffStrategy();
    assertThat(
        backoffStrategy,
        instanceOf(PredefinedBackoffStrategies.FullJitterBackoffStrategy.class)
    );
  }

  @Test
  public void shouldCreateNewClientConfigurationWithNoProxy() {
    boolean expectContinue = true;
    long backoffMs = 1000L;

    when(mockConfig.useExpectContinue()).thenReturn(expectContinue);
    when(mockConfig.getLong("s3.retry.backoff.ms")).thenReturn(backoffMs);

    ClientConfiguration clientConfiguration = storage.newClientConfiguration(mockConfig);

    assertThat(clientConfiguration.isUseExpectContinue(), is(expectContinue));

    RetryPolicy.BackoffStrategy backoffStrategy =
        clientConfiguration.getRetryPolicy().getBackoffStrategy();
    assertThat(
        backoffStrategy,
        instanceOf(PredefinedBackoffStrategies.FullJitterBackoffStrategy.class)
    );
  }

  @Test
  public void shouldCheckObjectExistenceWithObjectThatDoesExist() {
    String objectKey = "my-key";
    boolean exists = true;
    when(mockS3.doesObjectExist(testBucket, objectKey)).thenReturn(exists);

    boolean objectExists = storage.exists(objectKey);

    assertThat(objectExists, is(exists));
  }

  @Test
  public void shouldCheckObjectExistenceWithObjectThatDoesNotExist() {
    String objectKey = "my-key";
    boolean exists = false;
    when(mockS3.doesObjectExist(testBucket, objectKey)).thenReturn(exists);

    boolean objectExists = storage.exists(objectKey);

    assertThat(objectExists, is(exists));
  }

  @Test
  public void shouldCheckObjectExistenceForInvalidObjectKey() {
    String objectKey = null;

    boolean objectExists = storage.exists(objectKey);

    assertThat(objectExists, is(false));
  }

  @Test
  public void shouldCheckBucketExistenceWithBucketThatDoesExist() {
    boolean exists = true;
    when(mockS3.doesBucketExist(testBucket)).thenReturn(exists);

    boolean objectExists = storage.bucketExists();

    assertThat(objectExists, is(exists));
  }

  @Test
  public void shouldCheckBucketExistenceWithBucketThatDoesNotExist() {
    boolean exists = false;
    when(mockS3.doesBucketExist(testBucket)).thenReturn(exists);

    boolean objectExists = storage.bucketExists();

    assertThat(objectExists, is(exists));
  }

  @Test
  public void shouldCheckBucketExistenceForInvalidBucketKey() {
    S3Storage storage = new S3Storage(mockConfig, testUrl, null, mockS3);

    boolean objectExists = storage.bucketExists();

    assertThat(objectExists, is(false));
  }

  @Test
  public void shouldDeleteObject() {
    String objectKey = "my-key";

    storage.delete(objectKey);

    verify(mockS3).deleteObject(testBucket, objectKey);
  }

  @Test
  public void shouldNotBeAbleToDeleteBucket() {
    storage.delete(testBucket);

    verify(mockS3, never()).deleteObject(anyString(), anyString());
  }

  @Test
  public void shouldNotCallAnythingOnClose() {
    storage.close();

    verifyNoMoreInteractions(mockS3);
  }

  @Test
  public void shouldListObjectsOnS3() {
    String path = "my-path";

    storage.list(path);

    verify(mockS3).listObjectsV2(testBucket, path);
  }

  @Test
  public void shouldReturnConfig() {
    S3SourceConnectorConfig conf = storage.conf();

    assertThat(conf, is(mockConfig));
  }

  @Test
  public void shouldReturnUrl() {
    String url = storage.url();

    assertThat(url, is(testUrl));
  }

  @Test
  public void shouldOpenFileWithCustomerKey() {
    String customerKey = "customer-key";
    when(mockConfig.getSseCustomerKey()).thenReturn(customerKey);

    String path = "my-path";

    storage.open(path);

    verify(mockConfig).getCustomerKey();
  }

  @Test
  public void shouldOpenFileWithoutCustomerKey() {
    String customerKey = null;
    when(mockConfig.getSseCustomerKey()).thenReturn(customerKey);

    String path = "my-path";

    storage.open(path);

    verify(mockConfig, never()).getCustomerKey();
  }
}