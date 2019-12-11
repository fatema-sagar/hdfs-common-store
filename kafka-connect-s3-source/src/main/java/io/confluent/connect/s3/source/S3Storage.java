/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.s3.source;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.PredefinedClientConfigurations;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.retry.PredefinedBackoffStrategies;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import io.confluent.connect.cloud.storage.source.CloudSourceStorage;
import io.confluent.connect.cloud.storage.source.StorageObject;
import io.confluent.connect.s3.source.util.S3ProxyConfig;
import io.confluent.connect.utils.Version;
import io.confluent.license.util.StringUtils;

import static io.confluent.connect.s3.source.S3SourceConnectorConfig.REGION_CONFIG;
import static io.confluent.connect.s3.source.S3SourceConnectorConfig.S3_PROXY_URL_CONFIG;
import static io.confluent.connect.s3.source.S3SourceConnectorConfig.S3_RETRY_BACKOFF_CONFIG;
import static io.confluent.connect.s3.source.S3SourceConnectorConfig.S3_RETRY_MAX_BACKOFF_TIME_MS;
import static io.confluent.connect.s3.source.S3SourceConnectorConfig.WAN_MODE_CONFIG;

/**
 * It implements CloudSourceStorage interface.
 * This has implementation of methods needed to interact with 
 * S3 bucket and S3 object.
 */
public class S3Storage implements CloudSourceStorage {

  private static final Logger log = LoggerFactory.getLogger(S3Storage.class);

  private static final String VERSION_FORMAT = "APN/1.0 Confluent/1.0 KafkaS3Connector/%s";
  private final String url;
  private final String bucketName;
  private final AmazonS3 s3;
  private final S3SourceConnectorConfig conf;

  /**
   * Construct an S3 storage class given a configuration and an AWS S3 address.
   *
   * @param conf the S3 configuration.
   * @param url the S3 address.
   */
  public S3Storage(S3SourceConnectorConfig conf, String url) {
    this.url = url;
    this.conf = conf;
    this.bucketName = conf.getBucketName();
    this.s3 = newS3Client(conf);
  }

  // Visible for testing.
  S3Storage(S3SourceConnectorConfig conf, String url, String bucketName, AmazonS3 s3) {
    this.url = url;
    this.conf = conf;
    this.bucketName = bucketName;
    this.s3 = s3;
  }

  @Override
  public StorageObject getStorageObject(String key) {
    return new S3StorageObject(s3.getObject(bucketName, key));
  }
 
  /**
   * As of now the implementation of this method is not added.
   * S3 source connect is not using it anywhere.
   * In future, if this method is required then implementation can be added here.
   */
  @Override
  public List<StorageObject> getListOfStorageObjects(String path) {
    throw new UnsupportedOperationException(
        "Implementation not added in S3 Storage class.");
  }
  
  @Override
  public boolean exists(String name) {
    return StringUtils.isNotBlank(name) && s3.doesObjectExist(bucketName, name);
  }

  @Override
  public boolean bucketExists() {
    return StringUtils.isNotBlank(bucketName) && s3.doesBucketExist(bucketName);
  }
  
  @Override
  public void delete(String name) {
    if (bucketName.equals(name)) {
      // TODO: decide whether to support delete for the top-level bucket.
      // s3.deleteBucket(name);
      return;
    } else {
      s3.deleteObject(bucketName, name);
    }
  }

  @Override
  public StorageObject open(String path) {
    GetObjectRequest request = new GetObjectRequest(bucketName, path);
    // TODO On recovery can potentially skip to part of file required request.setRange(0);
    if (conf.getSseCustomerKey() != null) {
      request.withSSECustomerKey(conf.getCustomerKey());
    }

    log.trace("Opening file stream to bucket {} and path {}", bucketName, path);
    return new S3StorageObject(s3.getObject(request));
  }

  public S3SourceConnectorConfig conf() {
    return conf;
  }

  public String url() {
    return url;
  }

  /**
   * Creates and configures S3 client. Visible for testing.
   *
   * @param config the S3 configuration.
   * @return S3 client
   */
  AmazonS3 newS3Client(S3SourceConnectorConfig config) {
    ClientConfiguration clientConfiguration = newClientConfiguration(config);
    AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
        .withAccelerateModeEnabled(config.getBoolean(WAN_MODE_CONFIG))
        .withPathStyleAccessEnabled(true).withCredentials(config.getCredentialsProvider())
        .withClientConfiguration(clientConfiguration);

    String region = config.getString(REGION_CONFIG);
    if (StringUtils.isBlank(url)) {
      // us-east-1 is the standard region and must be handled differently due to issue with it
      // not setting region id
      builder = "us-east-1".equals(region) ? builder.withRegion(Regions.US_EAST_1)
                                           : builder.withRegion(region);
    } else {
      builder = builder
          .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(url, region));
    }

    return builder.build();
  }

  /**
   * Creates S3 client's configuration. This method currently configures the AWS client retry policy
   * to use full jitter. Visible for testing.
   *
   * @param config the S3 configuration.
   * @return S3 client's configuration
   */
  ClientConfiguration newClientConfiguration(S3SourceConnectorConfig config) {
    String version = String.format(VERSION_FORMAT, Version.forClass(S3Storage.class));

    ClientConfiguration clientConfiguration = PredefinedClientConfigurations.defaultConfig();
    clientConfiguration.withUserAgentPrefix(version)
        .withRetryPolicy(newFullJitterRetryPolicy(config));
    if (StringUtils.isNotBlank(config.getString(S3_PROXY_URL_CONFIG))) {
      S3ProxyConfig proxyConfig = new S3ProxyConfig(config);
      clientConfiguration.withProtocol(proxyConfig.protocol()).withProxyHost(proxyConfig.host())
          .withProxyPort(proxyConfig.port()).withProxyUsername(proxyConfig.user())
          .withProxyPassword(proxyConfig.pass());
    }
    clientConfiguration.withUseExpectContinue(config.useExpectContinue());

    return clientConfiguration;
  }


  /**
   * Creates a retry policy, based on full jitter backoff strategy and default retry condition.
   * Visible for testing.
   *
   * @param config the S3 configuration.
   * @return retry policy
   * @see com.amazonaws.retry.PredefinedRetryPolicies.SDKDefaultRetryCondition
   * @see PredefinedBackoffStrategies.FullJitterBackoffStrategy
   */
  protected RetryPolicy newFullJitterRetryPolicy(S3SourceConnectorConfig config) {

    PredefinedBackoffStrategies.FullJitterBackoffStrategy backoffStrategy =
        new PredefinedBackoffStrategies.FullJitterBackoffStrategy(
            config.getLong(S3_RETRY_BACKOFF_CONFIG).intValue(), S3_RETRY_MAX_BACKOFF_TIME_MS);

    RetryPolicy retryPolicy =
        new RetryPolicy(PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION, backoffStrategy,
            conf.getS3PartRetries(), false
        );
    return retryPolicy;
  }

  public void close() {
  }

  public ListObjectsV2Result list(String path) {
    log.trace("Listing objects on s3 with path {}", path);
    return s3.listObjectsV2(bucketName, path);
  }

  public ListObjectsV2Result listFolders(String path, String delimiter) {
    log.trace("Listing folders on s3 with path {} and delimiter {}", path, delimiter);
    try {
      return s3.listObjectsV2(new ListObjectsV2Request().withPrefix(path).withBucketName(bucketName)
          .withDelimiter(delimiter));
    } catch (Exception ex) {
      throw new ConnectException("Failed to get list of folders from "
          + "S3 bucket - " + bucketName + " for key path - " + path 
          + " and delimiter - " + delimiter, ex);
    }
  }

  public ListObjectsV2Result listFiles(String path, String continuationToken) {
    log.trace("Listing files on s3 with path {} and continuation token {}", 
        path, continuationToken);

    try {
      return s3.listObjectsV2(new ListObjectsV2Request().withPrefix(path).withBucketName(bucketName)
          .withContinuationToken(continuationToken));
    } catch (Exception ex) {
      throw new ConnectException("Failed to get list of folders from "
          + "S3 bucket - " + bucketName + " for key path - " + path 
          + " and continuationToken - " + continuationToken, ex);
    }
  }

  public String getNextFileName(String path, String startAfterThisFile, String fileSuffix) {
    log.trace("Listing objects on s3 with path {} starting after file {} and having suffix {}",
        path, startAfterThisFile, fileSuffix
    );

    ListObjectsV2Result filterBySuffix = s3.listObjectsV2(
        new ListObjectsV2Request().withStartAfter(startAfterThisFile).withBucketName(bucketName)
            .withPrefix(path).withDelimiter(fileSuffix).withMaxKeys(1));
    
    ListObjectsV2Result result = null;

    if (filterBySuffix.getCommonPrefixes().size() > 0) {
      String getKeyMinusExtension =
          getFirstObjectAlphabetically(filterBySuffix.getCommonPrefixes())
              .substring(0, filterBySuffix.getCommonPrefixes().get(0).lastIndexOf(fileSuffix));
      
      ListObjectsV2Result resultList = s3.listObjectsV2(
          new ListObjectsV2Request().withBucketName(bucketName).withPrefix(getKeyMinusExtension));
      result = resultList;
      
    } else {
      result = filterBySuffix;
    }
    
    if (result.getObjectSummaries().isEmpty()) {
      // return a empty filename.
      return "";
    }

    return result.getObjectSummaries().get(0).getKey();
  }

  // Only required due to s3 mock not supporting this. S3 proper should always return keys in
  // alphabetical order
  private String getFirstObjectAlphabetically(List<String> commonPrefixes) {
    if (commonPrefixes.size() == 1) {
      return commonPrefixes.get(0);
    }

    return commonPrefixes.stream().sorted(String::compareTo).findFirst().get();
  }

}
