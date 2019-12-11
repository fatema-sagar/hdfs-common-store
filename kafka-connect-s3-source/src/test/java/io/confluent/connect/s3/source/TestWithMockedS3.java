/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.s3.source;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.DeleteBucketRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertTrue;

public class TestWithMockedS3 extends S3SourceConnectorTestBase {

  private static final Logger log = LoggerFactory.getLogger(TestWithMockedS3.class);

  @ClassRule
  public static TemporaryFolder s3mockRoot = new TemporaryFolder();

  @ClassRule
  public static S3MockRule S3_MOCK_RULE;

  static {
    try {
      s3mockRoot.create();
      File s3mockDir = s3mockRoot.newFolder("s3-tests-" + UUID.randomUUID().toString());
      System.out.println("Create folder: " + s3mockDir.getCanonicalPath());
      S3_MOCK_RULE = S3MockRule.builder()
          .withHttpPort(S3_TEST_PORT)
          .withRootFolder(s3mockDir.getAbsolutePath())
          .silent()
          .build();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  protected AmazonS3 s3;
  protected S3Storage storage;

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    return props;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    s3 = newS3Client(connectorConfig);
    storage = new S3Storage(connectorConfig, url, S3_TEST_BUCKET_NAME, s3);
    s3.createBucket(S3_TEST_BUCKET_NAME);
    assertTrue(s3.doesBucketExist(S3_TEST_BUCKET_NAME));
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    s3.deleteBucket(new DeleteBucketRequest(S3_TEST_BUCKET_NAME));
  }

  public static List<S3ObjectSummary> listObjects(String bucket, String prefix, AmazonS3 s3) {
    List<S3ObjectSummary> objects = new ArrayList<>();
    ObjectListing listing;

    try {
      if (prefix == null) {
        listing = s3.listObjects(bucket);
      } else {
        listing = s3.listObjects(bucket, prefix);
      }

      objects.addAll(listing.getObjectSummaries());
      while (listing.isTruncated()) {
        listing = s3.listNextBatchOfObjects(listing);
        objects.addAll(listing.getObjectSummaries());
      }
    } catch (AmazonS3Exception e) {
      log.warn("listObjects for bucket '{}' prefix '{}' returned error code: {}", bucket, prefix,
          e.getStatusCode()
      );
    }

    return objects;
  }

  @Override
  public AmazonS3 newS3Client(S3SourceConnectorConfig config) {
    final AWSCredentialsProvider provider = new AWSCredentialsProvider() {
      private final AnonymousAWSCredentials credentials = new AnonymousAWSCredentials();

      @Override
      public AWSCredentials getCredentials() {
        return credentials;
      }

      @Override
      public void refresh() {
      }
    };

    AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
        .withAccelerateModeEnabled(config.getBoolean(S3SourceConnectorConfig.WAN_MODE_CONFIG))
        .withPathStyleAccessEnabled(true).withCredentials(provider);

    builder =
        url == null ? builder.withRegion(config.getString(S3SourceConnectorConfig.REGION_CONFIG))
                    : builder
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(url, ""));

    return builder.build();
  }

}

