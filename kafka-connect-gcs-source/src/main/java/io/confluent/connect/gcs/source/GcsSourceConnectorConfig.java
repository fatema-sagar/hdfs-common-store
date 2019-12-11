/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.gcs.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.GoogleCredentials;

import io.confluent.connect.cloud.storage.source.CloudSourceStorage;
import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.gcs.GcsSourceConnector;
import io.confluent.connect.storage.partitioner.DailyPartitioner;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.FieldPartitioner;
import io.confluent.connect.storage.partitioner.HourlyPartitioner;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import io.confluent.connect.utils.recommenders.Recommenders;
import io.confluent.license.util.StringUtils;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Configuration class for {@link GcsSourceConnector}.
 * TODO: Fix doc according to GCS Source.
 */
public class GcsSourceConnectorConfig extends CloudStorageSourceConnectorCommonConfig {

  private static final Logger log = LoggerFactory.getLogger(GcsSourceConnectorConfig.class);

  public static final String GCS_POLL_INTERVAL_MS_CONFIG = "gcs.poll.interval.ms";
  public static final Long GCS_POLL_INTERVAL_MS_DEFAULT = TimeUnit.MINUTES.toMillis(1);
  public static final String GCS_POLL_INTERVAL_MS_DOC =
      "Frequency in milliseconds to poll for new or removed folders. This may result in updated "
          + "task configurations starting to poll for data in added folders or stopping polling "
          + "for data in removed folders.";
  public static final String GCS_POLL_INTERVAL_MS_DISPLAY =
      "Metadata Change Monitoring Interval (ms)";
  public static final String GCS_BUCKET_CONFIG = "gcs.bucket.name";

  public static final String PART_SIZE_CONFIG = "gcs.part.size";
  public static final int PART_SIZE_DEFAULT = 25 * 1024 * 1024;

  public static final String GCS_PART_RETRIES_CONFIG = "gcs.part.retries";
  public static final int GCS_PART_RETRIES_DEFAULT = 3;

  public static final long GCS_RETRY_MAX_BACKOFF_TIME_MS = TimeUnit.HOURS.toMillis(24);
  public static final String GCS_RETRY_BACKOFF_CONFIG = "gcs.retry.backoff.ms";
  public static final int GCS_RETRY_BACKOFF_DEFAULT = 200;
  private static final String GCS_RETRY_BACKOFF_CONFIG_DOC = 
      "How long to wait in milliseconds before attempting the first retry "
      + "of a failed GCS request. Upon a failure, this connector may wait up to twice as "
      + "long as the previous wait, up to the maximum number of retries. "
      + "This avoids retrying in a tight loop under failure scenarios.";

  private static final String GOOGLE_APPLICATION_CREDENTIALS = "GOOGLE_APPLICATION_CREDENTIALS";
  private static final String GOOGLE_ACCOUNT_TYPE = "type";
  private static final String PROJECT_ID = "project_id";
  private static final String PRIVATE_KEY_ID = "private_key_id";
  private static final String PRIVATE_KEY = "private_key";
  private static final String CLIENT_EMAIL = "client_email";
  private static final String CLIENT_ID = "client_id";

  public static final String COMPRESSION_TYPE_CONFIG = "gcs.compression.type";
  public static final String COMPRESSION_TYPE_DEFAULT = "none";

  public static final String GCS_PARTS_PREFIX = "gcs.parts.prefix";
  public static final String GCS_PARTS_PREFIX_DOC = "DEPRECATED. Multipart uploads no longer use a "
      + "part prefix.";
  public static final String GCS_PARTS_PREFIX_DEFAULT = "parts";
  
  public static final String CREDENTIALS_PATH_CONFIG = "gcs.credentials.path";
  public static final String CREDENTIALS_PATH_DEFAULT = "";

  private static final String GOOGLE_API_URL = "https://www.googleapis.com/auth/cloud-platform";
  
  public static final String PARTITIONER_CLASS_CONFIG = "partitioner.class";
  public static final Class<DefaultPartitioner> PARTIONER_CLASS_DEFAULT = DefaultPartitioner.class;
  public static final String PARTITIONER_CLASS_DOC =
      "The partitioner to use when reading data to the store.";
  private static final String PARTITIONER_CLASS_DISPLAY = "Partitioner Class";
  
  public GcsSourceConnectorConfig(Map<String, String> props) {
    super(config(), props);
  }

  protected GcsSourceConnectorConfig(ConfigDef configDef, Map<String, String> props) {
    super(configDef, props);
  }

  @Override
  protected Long getPollInterval() {
    return getLong(GCS_POLL_INTERVAL_MS_CONFIG);
  }

  public static ConfigDef config() {

    final String groupGcs = "GCS";
    final String groupPartitioner = "Partitioner";
    final String groupConnector = "Connector";

    int orderInGroup = 0;
    ConfigDef configDef = CloudStorageSourceConnectorCommonConfig.config();

    configDef
        .define(
            GCS_POLL_INTERVAL_MS_CONFIG,
            ConfigDef.Type.LONG,
            GCS_POLL_INTERVAL_MS_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            GCS_POLL_INTERVAL_MS_DOC,
            groupConnector,
            ++orderInGroup,
            ConfigDef.Width.MEDIUM,
            GCS_POLL_INTERVAL_MS_DISPLAY);

    configDef
        .define(GCS_BUCKET_CONFIG,
            Type.STRING,
            Importance.HIGH,
            "The name of the GCS bucket.",
            groupGcs,
            ++orderInGroup,
            Width.LONG,
            "GCS Bucket")
    
        .define(GCS_PART_RETRIES_CONFIG,
            Type.INT,
            GCS_PART_RETRIES_DEFAULT,
            atLeast(0L),
            Importance.MEDIUM,
            "Number of upload retries of a single GCS part. Zero means no retries.",
            groupGcs,
            ++orderInGroup,
            Width.LONG,
            "GCS Part Upload Retries")
    
        .define(GCS_RETRY_BACKOFF_CONFIG,
            Type.LONG,
            GCS_RETRY_BACKOFF_DEFAULT,
            atLeast(0L),
            Importance.LOW,
            GCS_RETRY_BACKOFF_CONFIG_DOC,
            groupGcs,
            ++orderInGroup,
            Width.SHORT,
            "Retry Backoff (ms)")
    
        .define(CREDENTIALS_PATH_CONFIG,
            Type.STRING,
            CREDENTIALS_PATH_DEFAULT,
            Importance.HIGH,
            "Path to credentials file. If empty, credentials are read from the "
            + "GOOGLE_APPLICATION_CREDENTIALS environment variable.",
            groupGcs,
            ++orderInGroup,
            Width.LONG,
            "GCS Credentials Path")
    
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
  
  public String getBucketName() {
    return getString(GCS_BUCKET_CONFIG);
  }
  
  public String credentialsPath() {
    return getString(CREDENTIALS_PATH_CONFIG);
  }
  
  public GoogleCredentials getCredentials() throws IOException {
    String pathToCredentials = credentialsPath();

    // Check if credentials path leads to valid json file
    if (isValidJsonFile(pathToCredentials)) {
      try {
        return GoogleCredentials
            .fromStream(new FileInputStream(pathToCredentials))
            .createScoped(Collections.singletonList(GOOGLE_API_URL));
      } catch (Exception ex) {
        log.error("Unable to build credentials from Credentials From Envirnoment", ex);
        throw new ConfigException(ex.getMessage(), ex);
      }
    } else if (!StringUtils.isBlank(System.getenv(GOOGLE_APPLICATION_CREDENTIALS))) {
      return getCredentialsFromEnv();
    } else {
      return buildCredentialsFromProperties(pathToCredentials);
    }
  }

  private static String readAllBytesFromFile(String filePath) throws IOException {
    String content = "";
    try {
      content = new String(Files.readAllBytes(
          Paths.get(filePath)), StandardCharsets.UTF_8.name());
    } catch (IOException e) {
      throw new ConfigException(
          "Unable to read contents of credentials file at: " + filePath, e);
    }
    return content;
  }
  
  private boolean isValidJsonFile(String pathToFile) {
    try {
      final ObjectMapper mapper = new ObjectMapper();
      mapper.readTree(readAllBytesFromFile(pathToFile));
      return true;
    } catch (IOException e) {
      log.debug("Unable to read JSON object from file at: " + pathToFile);
    } catch (Exception ex) {
      log.warn("Unexpected error while attempting to read from file at: " + pathToFile, ex);
    }
    return false;
  }
  
  public GoogleCredentials getCredentialsFromEnv() throws IOException {
    String pathToCredentials = System.getenv(GOOGLE_APPLICATION_CREDENTIALS);
    try {
      return GoogleCredentials
          .fromStream(new FileInputStream(pathToCredentials))
          .createScoped(Collections.singletonList(GOOGLE_API_URL));
    } catch (Exception ex) {
      log.error("Unable to build credentials from Credentials From Envirnoment", ex);
      throw new ConfigException(ex.getMessage(), ex);
    }
  }

  private GoogleCredentials buildCredentialsFromProperties(
      String pathToCredentials
  ) {
    try (FileInputStream credentialsStream = new FileInputStream(pathToCredentials)) {
      Properties prop = new Properties();
      prop.load(credentialsStream);
      Set<String> credentialsFields = new HashSet<>();
      credentialsFields.add(GOOGLE_ACCOUNT_TYPE);
      credentialsFields.add(PROJECT_ID);
      credentialsFields.add(PRIVATE_KEY_ID);
      credentialsFields.add(PRIVATE_KEY);
      credentialsFields.add(CLIENT_EMAIL);
      credentialsFields.add(CLIENT_ID);
      Map<String, String> valueMap = new HashMap<>();
      prop.forEach((k,v) -> valueMap.put((String)k, (String)v));
      Map<String, String> credentialsMap = new HashMap<>();
      for (Map.Entry<String,String> entry : valueMap.entrySet()) {
        String key = entry.getKey().replaceAll("\"", "").replaceAll(",", "");
        if (credentialsFields.contains(key)) {
          String val = entry.getValue().replaceAll("\"", "").replaceAll(",", "");
          credentialsMap.put(key, val);
        }
      }
      String jsonString = new ObjectMapper().writeValueAsString(credentialsMap);
      InputStream is = new ByteArrayInputStream(
          jsonString.getBytes(Charset.forName(StandardCharsets.UTF_8.name())));
      return GoogleCredentials.fromStream(is)
            .createScoped(Collections.singletonList(GOOGLE_API_URL));
    } catch (IOException e) {
      log.error("Unable to build credentials from properties file", e);
      throw new ConfigException(e.getMessage());
    }
  }
  
  public Partitioner getPartitioner(CloudSourceStorage s3storage) {
    try {
      @SuppressWarnings("unchecked") Class<Partitioner> partitionerClass =
          (Class<Partitioner>) this.getClass(PARTITIONER_CLASS_CONFIG);
      return partitionerClass
          .getConstructor(GcsSourceConnectorConfig.class, GcsSourceStorage.class)
          .newInstance(this, s3storage);
    } catch (Exception e) {
      throw new ConnectException("Failed to instantiate Partitioner class.", e);
    }
  }
  
  public static void main(String[] args) {
    System.out.println(config().toEnrichedRst());
  }
  
}
