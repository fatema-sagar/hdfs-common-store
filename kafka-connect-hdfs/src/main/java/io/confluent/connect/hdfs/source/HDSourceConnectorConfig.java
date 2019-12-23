/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.hdfs.source;

import io.confluent.connect.cloud.storage.source.CloudSourceStorage;
import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.Partitioner;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

/**
 * Configuration class for {@link HDSourceConnector}.
 */
public class HDSourceConnectorConfig extends CloudStorageSourceConnectorCommonConfig {

  public static final String HDF_POLL_INTERVAL_MS_CONFIG = "hdfs.poll.interval.ms";
  private static final String HDF_POLL_INTERVAL_MS_DOC = "Frequency in milliseconds to "
          + "poll for new or"
          + " removed folders. This may result in updated task configurations starting to poll "
          + "for data in added folders or stopping polling for data in removed folders.";
  public static final Long HDF_POLL_INTERVAL_MS_DEFAULT = TimeUnit.MINUTES.toMillis(1);
  public static final String HDF_POLL_INTERVAL_MS_DISPLAY = "Metadata Change Monitoring "
          + "Interval (ms)";

  public static final String HDF_RETRY_BACKOFF_CONFIG = "hdfs.retry.backoff.ms";
  private static final String HDF_RETRY_BACKOFF_DISPLAY = "Retry Backoff (ms)";
  private static final String HDF_RETRY_BACKOFF_DOC = "How long to wait in milliseconds "
          + "before attempting the first retry of a failed S3 request. Upon a failure, this"
          + " connector may wait up to twice as long as the previous wait, up to the maximum"
          + " number of retries. This avoids retrying in a tight loop under failure scenarios.";
  static final int HDF_RETRY_BACKOFF_DEFAULT = 200;
  public static final int HDF_RETRY_MAX_BACKOFF_TIME_MS = (int) TimeUnit.HOURS.toMillis(24);

  public static final String PARTITIONER_CLASS_CONFIG = "partitioner.class";
  public static final Class<DefaultPartitioner> PARTIONER_CLASS_DEFAULT = DefaultPartitioner.class;
  public static final String PARTITIONER_CLASS_DOC = "The partitioner to use when reading data"
          + "to the store.";
  private static final String PARTITIONER_CLASS_DISPLAY = "Partitioner Class";

  public HDSourceConnectorConfig(Map<String, String> props) {
    super(config(), props);
  }

  @Override
  protected Long getPollInterval() {
    return getLong(HDF_POLL_INTERVAL_MS_CONFIG);
  }

  public static ConfigDef config() {
    final String groupPartitioner = "Partitioner";
    int orderInGroup = 0;
    ConfigDef configDef = new ConfigDef();

    configDef
        .define(
        HDF_POLL_INTERVAL_MS_CONFIG,
        Type.LONG,
        HDF_POLL_INTERVAL_MS_DEFAULT,
        Importance.HIGH,
        HDF_POLL_INTERVAL_MS_DOC,
        "Connection",
        orderInGroup++,
        Width.MEDIUM,
        HDF_POLL_INTERVAL_MS_DISPLAY);

    configDef
        .define(
        HDF_RETRY_BACKOFF_CONFIG,
        Type.LONG,
        HDF_RETRY_BACKOFF_DEFAULT,
        atLeast(0L),
        Importance.LOW,
        HDF_RETRY_BACKOFF_DOC,
        "Connection",
        orderInGroup++,
        Width.SHORT,
        HDF_RETRY_BACKOFF_DISPLAY);

    configDef
        .define(
        PARTITIONER_CLASS_CONFIG,
        Type.CLASS,
        PARTIONER_CLASS_DEFAULT,
        Importance.MEDIUM,
        PARTITIONER_CLASS_DOC,
        groupPartitioner,
        orderInGroup++,
        Width.LONG,
        PARTITIONER_CLASS_DISPLAY);

    return configDef;
  }

  public Partitioner getPartitioner(CloudSourceStorage hdStorage) {
    try {
      @SuppressWarnings("unchecked") Class<Partitioner> partitionerClass =
              (Class<Partitioner>) this.getClass(PARTITIONER_CLASS_CONFIG);
      return partitionerClass
              .getConstructor(HDSourceConnectorConfig.class, HDStorage.class)
              .newInstance(this, hdStorage);
    } catch (Exception e) {
      throw new ConnectException("Failed to instantiate Partitioner class ");
    }
  }

  public String getHdfsUrl() {
    return getString(STORE_URL_CONFIG);
  }

  public static void main(String[] args) {
    System.out.println(config().toEnrichedRst());
  }
}
