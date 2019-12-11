/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.cloud.storage.source;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.cloud.storage.source.util.PartitionCheckingTask;
import io.confluent.connect.utils.Version;
import io.confluent.connect.utils.licensing.ConnectLicenseManager;
import io.confluent.connect.utils.licensing.LicenseConfigUtil;

import static io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig.FOLDERS_CONFIG;

/**
 * Abstract Cloud Storage source connector class.
 * Source Connector class of cloud storage source connector like S3, GCS, Azure blob has 
 * to extend this class.
 */
public abstract class AbstractCloudStorageSourceConnector extends SourceConnector {

  public static final int MAX_TIMEOUT = 10;

  private static Logger log = LoggerFactory.getLogger(AbstractCloudStorageSourceConnector.class);

  protected CloudStorageSourceConnectorCommonConfig config;
  protected CloudSourceStorage storage;
  protected ConnectLicenseManager licenseManager;
  protected Partitioner partitioner;
  private Set<String> partitions;
  private ScheduledExecutorService executor;
  private AtomicBoolean isConnectorRunning;

  public AbstractCloudStorageSourceConnector() {
  }
  
  protected abstract CloudStorageSourceConnectorCommonConfig createConfig(
      Map<String, String> props);
  
  protected abstract CloudSourceStorage createStorage();
  
  protected abstract Partitioner getPartitioner(CloudSourceStorage storage);

  void setConfig(CloudStorageSourceConnectorCommonConfig config) {
    this.config = config;
  }

  @Override
  public void start(Map<String, String> map) {
    this.config = createConfig(map);
    this.storage = createStorage();

    // Start the Confluent license manager
    licenseManager = LicenseConfigUtil.createLicenseManager(config);
    doStart();
    log.info("Starting source connector");
  }

  // visible for testing
  public void doStart() {
    // First, ensure the license is valid or register a trial license
    licenseManager.registerOrValidateLicense();
    this.partitioner = getPartitioner(storage);
    partitions = partitioner.getPartitions();
    final Long pollMs = config.getPollInterval();
    isConnectorRunning = new AtomicBoolean(true);
    final PartitionCheckingTask partitionCheckingTask =
        new PartitionCheckingTask(partitioner, context, isConnectorRunning);
    executor = Executors.newSingleThreadScheduledExecutor();
    executor.scheduleAtFixedRate(partitionCheckingTask, 1000L, pollMs, TimeUnit.MILLISECONDS);
  }
  
  //visible for testing
  protected AbstractCloudStorageSourceConnector(CloudSourceStorage storage, 
      ConnectLicenseManager licenseManager) {
    this.storage = storage;
    this.licenseManager = licenseManager;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    Set<String> partitions = partitioner.getPartitions();
    final List<String> folders = new ArrayList<>(partitions);
    final List<List<String>> groups =
        ConnectorUtils.groupPartitions(folders, Math.min(maxTasks, folders.size()));
    final List<Map<String, String>> configs = new ArrayList<>(groups.size());
    for (List<String> group : groups) {
      Map<String, String> taskProps = new HashMap<>(config.originalsStrings());
      taskProps.put(FOLDERS_CONFIG, Utils.join(group, ","));
      configs.add(taskProps);
    }

    return configs;
  }

  @Override
  public void stop() {
    log.info("Shutting down source connector.");
    if (Objects.nonNull(isConnectorRunning)) {
      isConnectorRunning.set(false);
    }
    if (Objects.nonNull(executor)) {
      log.info("Stopping partition monitoring thread");
      try {
        executor.shutdownNow();
        executor.awaitTermination(MAX_TIMEOUT, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        log.error("Was interrupted during shutdown.", e);
      }
    }
    if (Objects.nonNull(storage)) {
      storage.close();
    }
  }
  
  @Override
  public String version() {
    return Version.forClass(this.getClass());
  }

  @Override
  public Class<? extends Task> taskClass() {
    return AbstractCloudStorageSourceTask.class;
  }

}
