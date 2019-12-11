/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.storage.partitioner;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;

import io.confluent.connect.gcs.source.GcsSourceConnectorConfig;
import io.confluent.connect.gcs.source.GcsSourceStorage;

/**
 * This class is for Default Partitioner logic and it implements 
 * Partitioner interface.
 * DefaultPartitioner will have folder structure similar to 
 *  - "topics/gcs_topic/partition=0/gcs_topic+0+0000000000.avro".
 */
public class DefaultPartitioner implements Partitioner {

  private static final Logger log = LoggerFactory.getLogger(DefaultPartitioner.class);

  private final GcsSourceConnectorConfig config;
  private final GcsSourceStorage gcsStorage;
  
  private final String extension;
  private Set<String> partitions = Collections.emptySet();

  public DefaultPartitioner(GcsSourceConnectorConfig config, GcsSourceStorage gcsStorage) {
    this.config = config;
    this.gcsStorage = gcsStorage;
    extension = config.getInstantiatedStorageObjectFormat().getExtension();
  }

  @Override
  public Set<String> getPartitions() {
    partitions = calculatePartitions();
    return partitions;
  }

  @Override
  public String getNextObjectName(String topic, String previousObject) {
    Blob blobObject = gcsStorage.getNextObject(topic, previousObject, extension);

    if (blobObject != null && blobObject.getName().endsWith(extension)) {
      return blobObject.getName();
    }

    return "";
  }

  @Override
  public boolean shouldReconfigure() {
    return !partitions.equals(calculatePartitions());
  }

  /**
   * Calculates number of partitions from given topic folder.
   * <p> Example : if file is located at - 
   * "topics/gcs_topic/partition=0/gcs_topic+0+0000000000.json" then 
   * partition value will be "topics/gcs_topic/partition=0/" </p>
   * @return : List of partitions.
   */
  private Set<String> calculatePartitions() {
    Set<String> partitions = new TreeSet<>();
    Page<Blob> blobs = gcsStorage.list(config.getTopicsFolder());

    for (Blob blob : blobs.getValues()) {
      Page<Blob> blobList = gcsStorage.list(blob.getName());
      for (Blob item : blobList.getValues()) {
        partitions.add(item.getName());
      }
    }

    log.debug("Got partitions {}", partitions);
    return partitions;

  }
  
  public static boolean isBlank(String string) {
    return string == null || string.isEmpty() || string.trim().isEmpty();
  }
}
