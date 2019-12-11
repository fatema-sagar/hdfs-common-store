/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.s3.source;

import static io.confluent.connect.utils.validators.Validators.instanceOf;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.errors.ConnectException;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import com.amazonaws.services.s3.model.SSECustomerKey;

import io.confluent.connect.cloud.storage.source.CloudSourceStorage;
import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.storage.partitioner.DailyPartitioner;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.FieldPartitioner;
import io.confluent.connect.storage.partitioner.HourlyPartitioner;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import io.confluent.connect.utils.recommenders.Recommenders;
import io.confluent.connect.utils.validators.Validators;
import io.confluent.license.util.StringUtils;

/**
 * Configuration class for {@link S3SourceConnector}.
 */
public class S3SourceConnectorConfig extends CloudStorageSourceConnectorCommonConfig {

  public static final String S3_POLL_INTERVAL_MS_CONFIG = "s3.poll.interval.ms";
  public static final Long S3_POLL_INTERVAL_MS_DEFAULT = TimeUnit.MINUTES.toMillis(1);
  public static final String S3_POLL_INTERVAL_MS_DOC =
      "Frequency in milliseconds to poll for new or removed folders. This may result in updated "
          + "task configurations starting to poll for data in added folders or stopping polling "
          + "for data in removed folders.";
  public static final String S3_POLL_INTERVAL_MS_DISPLAY =
      "Metadata Change Monitoring Interval (ms)";
  public static final String S3_BUCKET_CONFIG = "s3.bucket.name";
  public static final String S3_BUCKET_DOC = "The S3 Bucket.";
  public static final String S3_BUCKET_DISPLAY = "S3 Bucket";

  public static final String SSEA_CONFIG = "s3.ssea.name";
  public static final String SSEA_DOC = "The S3 Server-Side Encryption algorithm.";
  public static final String SSEA_DEFAULT = "";
  public static final String SSEA_DISPLAY = "S3 Server Side Encryption Algorithm";

  public static final String SSE_CUSTOMER_KEY_CONFIG = "s3.sse.customer.key";
  public static final String SSE_CUSTOMER_KEY_DOC =
      "The S3 Server-Side Encryption customer-provided key (SSE-C).";
  public static final Password SSE_CUSTOMER_KEY_DEFAULT = new Password(null);
  public static final String SSE_CUSTOMER_KEY_DISPLAY = "S3 Server Side Encryption "
      + "Customer-Provided Key (SSE-C)";

  public static final String WAN_MODE_CONFIG = "s3.wan.mode";
  public static final String WAN_MODE_DOC = "Use S3 accelerated endpoint.";
  private static final boolean WAN_MODE_DEFAULT = false;
  public static final String WAN_MODE_DISPLAY = "S3 accelerated endpoint enabled";

  public static final String REGION_CONFIG = "s3.region";
  public static final String REGION_DOC = "The AWS region to be used the connector.";
  public static final String REGION_DEFAULT = Regions.DEFAULT_REGION.getName();
  public static final String REGION_DISPLAY = "AWS region";


  public static final String S3_PART_RETRIES_CONFIG = "s3.part.retries";
  public static final String S3_PART_RETRIES_DOC =
      " Maximum number of retry attempts for failed requests. Zero means no retries. The actual"
          + " number of attempts is determined by the S3 client based on multiple factors, "
          + "including, but not limited to the value of this parameter, type of exception "
          + "occurred, throttling settings of the underlying S3 client, etc.";
  public static final int S3_PART_RETRIES_DEFAULT = 3;
  public static final String S3_PART__RETRIES_DISPLAY = "S3 Part Upload Retries";

  public static final String S3_PROXY_URL_CONFIG = "s3.proxy.url";
  public static final String S3_PROXY_URL_DOC =
      "S3 Proxy settings encoded in URL syntax. This property is meant to be used only if you"
          + " need to access S3 through a proxy.";
  public static final String S3_PROXY_URL_DEFAULT = "";
  public static final String S3_PROXY_URL_DISPLAY = "S3 Proxy Settings";

  public static final String S3_PROXY_USER_CONFIG = "s3.proxy.user";
  public static final String S3_PROXY_USER_DOC =
      "S3 Proxy User. This property is meant to be used only if you"
          + " need to access S3 through a proxy. Using ``" + S3_PROXY_USER_CONFIG
          + "`` instead of embedding the username and password in ``" + S3_PROXY_URL_CONFIG
          + "`` allows the password to be hidden in the logs.";
  public static final String S3_PROXY_USER_DEFAULT = null;
  public static final String S3_PROXY_USER_DISPLAY = "S3 Proxy User";

  public static final String S3_PROXY_PASS_CONFIG = "s3.proxy.password";
  public static final String S3_PROXY_PASS_DOC =
      "S3 Proxy Password. This property is meant to be used only if you"
          + " need to access S3 through a proxy. Using ``" + S3_PROXY_PASS_CONFIG
          + "`` instead of embedding the username and password in ``" + S3_PROXY_URL_CONFIG
          + "`` allows the password to be hidden in the logs.";
  public static final Password S3_PROXY_PASS_DEFAULT = new Password(null);
  public static final String S3_PROXY_PASS_DISPLAY = "S3 Proxy Password";

  public static final String S3_RETRY_BACKOFF_CONFIG = "s3.retry.backoff.ms";
  public static final String S3_RETRY_BACKOFF_DOC =
      "How long to wait in milliseconds before attempting the first retry "
          + "of a failed S3 request. Upon a failure, this connector may wait up to twice as "
          + "long as the previous wait, up to the maximum number of retries. "
          + "This avoids retrying in a tight loop under failure scenarios.";
  public static final int S3_RETRY_BACKOFF_DEFAULT = 200;
  public static final int S3_RETRY_MAX_BACKOFF_TIME_MS = (int) TimeUnit.HOURS.toMillis(24);
  public static final String S3_RETRY_BACKOFF_MS_DISPLAY = "Retry Backoff (ms)";

  public static final String CREDENTIALS_PROVIDER_CLASS_CONFIG = "s3.credentials.provider.class";
  public static final Class<? extends AWSCredentialsProvider> CREDENTIALS_PROVIDER_CLASS_DEFAULT =
      DefaultAWSCredentialsProviderChain.class;
  public static final String CREDENTIALS_PROVIDER_CLASS_DISPLAY = "AWS Credentials Provider Class";

  public static final String HEADERS_USE_EXPECT_CONTINUE_CONFIG = "s3.http.send.expect.continue";
  public static final String HEADERS_USE_EXPECT_CONTINUE_DOC =
      "Enable/disable use of the HTTP/1.1 handshake using EXPECT: 100-CONTINUE during multi-part"
          + " upload. If true, the client waits for a 100 (CONTINUE) response before sending the"
          + " request body. If false, the client uploads the entire request body without "
          + "checking if the server is willing to accept the request.";
  public static final boolean HEADERS_USE_EXPECT_CONTINUE_DEFAULT =
      ClientConfiguration.DEFAULT_USE_EXPECT_CONTINUE;
  public static final String HEADERS_USE_EXPECT_CONTINUE_DISPLAY = "S3 HTTP Send Uses Expect "
      + "Continue";

  /**
   * The properties that begin with this prefix will be used to configure a class, specified by
   * {@code s3.credentials.provider.class} if it implements {@link Configurable}.
   */
  public static final String CREDENTIALS_PROVIDER_CONFIG_PREFIX = CREDENTIALS_PROVIDER_CLASS_CONFIG
      .substring(0, CREDENTIALS_PROVIDER_CLASS_CONFIG.lastIndexOf(".") + 1);
  public static final String CREDENTIALS_PROVIDER_CLASS_DOC =
      "Credentials provider or provider chain to use for authentication to AWS. By default "
          + "the connector uses 'DefaultAWSCredentialsProviderChain'.";
  
  public static final String PARTITIONER_CLASS_CONFIG = "partitioner.class";
  public static final Class<DefaultPartitioner> PARTIONER_CLASS_DEFAULT = DefaultPartitioner.class;
  public static final String PARTITIONER_CLASS_DOC =
      "The partitioner to use when reading data to the store.";
  private static final String PARTITIONER_CLASS_DISPLAY = "Partitioner Class";

  public S3SourceConnectorConfig(Map<String, String> props) {
    super(config(), props);
  }

  protected S3SourceConnectorConfig(ConfigDef configDef, Map<String, String> props) {
    super(configDef, props);
  }

  @Override
  protected Long getPollInterval() {
    return getLong(S3_POLL_INTERVAL_MS_CONFIG);
  }

  public static ConfigDef config() {
    
    final String groupS3 = "S3";
    final String groupPartitioner = "Partitioner";
    final String groupConnector = "Connector";

    int orderInGroup = 0;
    ConfigDef configDef = CloudStorageSourceConnectorCommonConfig.config();

    configDef
        .define(
            S3_POLL_INTERVAL_MS_CONFIG,
            ConfigDef.Type.LONG,
            S3_POLL_INTERVAL_MS_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            S3_POLL_INTERVAL_MS_DOC,
            groupConnector,
            ++orderInGroup,
            ConfigDef.Width.MEDIUM,
            S3_POLL_INTERVAL_MS_DISPLAY);

    configDef
        .define(
            S3_BUCKET_CONFIG,
            ConfigDef.Type.STRING,
            ConfigDef.Importance.HIGH,
            S3_BUCKET_DOC,
            groupS3,
            ++orderInGroup,
            ConfigDef.Width.LONG,
            S3_BUCKET_DISPLAY);

    configDef
        .define(
            REGION_CONFIG,
            ConfigDef.Type.STRING,
            REGION_DEFAULT,
            Validators.oneStringOf(RegionUtils.getRegions(), false, Region::getName),
            ConfigDef.Importance.MEDIUM,
            REGION_DOC,
            groupS3,
            ++orderInGroup,
            ConfigDef.Width.LONG,
            REGION_DISPLAY,
            Recommenders.anyOf(RegionUtils.getRegions(), Region::getName));

    configDef
        .define(
            CREDENTIALS_PROVIDER_CLASS_CONFIG,
            ConfigDef.Type.CLASS,
            CREDENTIALS_PROVIDER_CLASS_DEFAULT,
            instanceOf(AWSCredentialsProvider.class),
            ConfigDef.Importance.LOW,
            CREDENTIALS_PROVIDER_CLASS_DOC,
            groupS3,
            ++orderInGroup,
            ConfigDef.Width.LONG,
            CREDENTIALS_PROVIDER_CLASS_DISPLAY);

    configDef
        .define(
            WAN_MODE_CONFIG,
            ConfigDef.Type.BOOLEAN,
            WAN_MODE_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            WAN_MODE_DOC,
            groupS3,
            ++orderInGroup,
            ConfigDef.Width.LONG,
            WAN_MODE_DISPLAY);

    configDef
        .define(
            S3_RETRY_BACKOFF_CONFIG,
            ConfigDef.Type.LONG,
            S3_RETRY_BACKOFF_DEFAULT,
            atLeast(0L),
            ConfigDef.Importance.LOW,
            S3_RETRY_BACKOFF_DOC,
            groupS3,
            ++orderInGroup,
            ConfigDef.Width.SHORT,
            S3_RETRY_BACKOFF_MS_DISPLAY);

    configDef
        .define(
            S3_PROXY_URL_CONFIG,
            ConfigDef.Type.STRING,
            S3_PROXY_URL_DEFAULT,
            ConfigDef.Importance.LOW,
            S3_PROXY_URL_DOC,
            groupS3,
            ++orderInGroup,
            ConfigDef.Width.LONG,
                S3_PROXY_URL_DISPLAY);

    configDef
        .define(
            S3_PROXY_USER_CONFIG,
            ConfigDef.Type.STRING,
            S3_PROXY_USER_DEFAULT,
            ConfigDef.Importance.LOW,
            S3_PROXY_USER_DOC,
            groupS3,
            ++orderInGroup,
            ConfigDef.Width.LONG,
            S3_PROXY_USER_DISPLAY);

    configDef
        .define(
            S3_PROXY_PASS_CONFIG,
            ConfigDef.Type.PASSWORD,
            S3_PROXY_PASS_DEFAULT,
            ConfigDef.Importance.LOW,
            S3_PROXY_PASS_DOC,
            groupS3,
            ++orderInGroup,
            ConfigDef.Width.LONG,
            S3_PROXY_PASS_DISPLAY);

    configDef
        .define(
            HEADERS_USE_EXPECT_CONTINUE_CONFIG,
            ConfigDef.Type.BOOLEAN,
            HEADERS_USE_EXPECT_CONTINUE_DEFAULT,
            ConfigDef.Importance.LOW,
            HEADERS_USE_EXPECT_CONTINUE_DOC,
            groupS3,
            ++orderInGroup,
            ConfigDef.Width.SHORT,
            HEADERS_USE_EXPECT_CONTINUE_DISPLAY);

    configDef
        .define(
            S3_PART_RETRIES_CONFIG,
            ConfigDef.Type.INT,
            S3_PART_RETRIES_DEFAULT,
            atLeast(0),
            ConfigDef.Importance.MEDIUM,
            S3_PART_RETRIES_DOC,
            groupS3,
            ++orderInGroup,
            ConfigDef.Width.LONG,
            S3_PART__RETRIES_DISPLAY);

    configDef
        .define(
            SSEA_CONFIG,
            ConfigDef.Type.STRING,
            SSEA_DEFAULT,
            Validators.anyOf(
                Validators.oneOf(SSEAlgorithm.class),
                Validators.nullOrBlank()
            ),
            ConfigDef.Importance.LOW,
            SSEA_DOC,
            groupS3,
            ++orderInGroup,
            ConfigDef.Width.LONG,
            SSEA_DISPLAY,
            Recommenders.enumValues(SSEAlgorithm.class));

    configDef
        .define(
            SSE_CUSTOMER_KEY_CONFIG,
            ConfigDef.Type.PASSWORD,
            SSE_CUSTOMER_KEY_DEFAULT,
            ConfigDef.Importance.LOW,
            SSE_CUSTOMER_KEY_DOC,
            groupS3,
            ++orderInGroup,
            ConfigDef.Width.LONG,
            SSE_CUSTOMER_KEY_DISPLAY);
    
    configDef
        .define(
            PARTITIONER_CLASS_CONFIG,
            ConfigDef.Type.CLASS,
            PARTIONER_CLASS_DEFAULT,
            ConfigDef.Importance.HIGH,
            PARTITIONER_CLASS_DOC,
            groupPartitioner,
            ++orderInGroup,
            ConfigDef.Width.LONG,
            PARTITIONER_CLASS_DISPLAY,
            Recommenders
                .anyOf(
                    DefaultPartitioner.class,
                    FieldPartitioner.class,
                    TimeBasedPartitioner.class,
                    DailyPartitioner.class,
                    HourlyPartitioner.class));

    return configDef;
  }

  public AWSCredentialsProvider getCredentialsProvider() {
    try {
      @SuppressWarnings("unchecked")
      AWSCredentialsProvider provider = (
          (Class<? extends AWSCredentialsProvider>) getClass(
              S3SourceConnectorConfig.CREDENTIALS_PROVIDER_CLASS_CONFIG)
      ).newInstance();

      if (provider instanceof Configurable) {
        Map<String, Object> configs = originalsWithPrefix(CREDENTIALS_PROVIDER_CONFIG_PREFIX);
        configs.remove(CREDENTIALS_PROVIDER_CLASS_CONFIG
            .substring(CREDENTIALS_PROVIDER_CONFIG_PREFIX.length()));
        ((Configurable) provider).configure(configs);
      }

      return provider;
    } catch (IllegalAccessException | InstantiationException e) {
      throw new ConnectException(
          "Invalid class for: " + S3SourceConnectorConfig.CREDENTIALS_PROVIDER_CLASS_CONFIG, e);
    }
  }
  
  public Partitioner getPartitioner(CloudSourceStorage s3Storage) {
    try {
      @SuppressWarnings("unchecked") Class<Partitioner> partitionerClass =
          (Class<Partitioner>) this.getClass(PARTITIONER_CLASS_CONFIG);
      return partitionerClass
          .getConstructor(S3SourceConnectorConfig.class, S3Storage.class)
          .newInstance(this, s3Storage);
    } catch (Exception e) {
      throw new ConnectException("Failed to instantiate Partitioner class ");
    }
  }

  public String getBucketName() {
    return getString(S3_BUCKET_CONFIG);
  }

  public int getS3PartRetries() {
    return getInt(S3_PART_RETRIES_CONFIG);
  }

  public boolean useExpectContinue() {
    return getBoolean(HEADERS_USE_EXPECT_CONTINUE_CONFIG);
  }

  public String getSsea() {
    return getString(SSEA_CONFIG);
  }

  public String getSseCustomerKey() {
    return getPassword(SSE_CUSTOMER_KEY_CONFIG).value();
  }

  public SSECustomerKey getCustomerKey() {
    return (
               SSEAlgorithm.AES256.toString().equalsIgnoreCase(getSsea()) && StringUtils
                   .isNotBlank(getSseCustomerKey())
           ) ? new SSECustomerKey(getSseCustomerKey()) : null;
  }

  public static void main(String[] args) {
    System.out.println(config().toEnrichedRst());
  }

}
