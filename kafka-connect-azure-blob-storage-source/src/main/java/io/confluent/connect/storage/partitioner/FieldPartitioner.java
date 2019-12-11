/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.storage.partitioner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.azure.blob.storage.AzureBlobSourceStorage;
import io.confluent.connect.azure.blob.storage.AzureBlobStorageSourceConnectorConfig;
import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;

/**
 * This class is for Field Partitioner logic and it implements 
 * Partitioner interface.
 * 
 * <p>Folder structure is based on the partition from the field within each record 
 * identified by the connector's partition.field.name configuration 
 * property, which has no default value.</p>
 */
public class FieldPartitioner implements Partitioner {

  private static final Logger log = LoggerFactory.getLogger(FieldPartitioner.class);

  private final AzureBlobStorageSourceConnectorConfig config;
  private final AzureBlobSourceStorage storage;
  private Set<String> partitions = Collections.emptySet();
  private final String extension;

  public FieldPartitioner(AzureBlobStorageSourceConnectorConfig config, 
      AzureBlobSourceStorage storage) {
    this.config = config;
    this.storage = storage;
    extension = config.getInstantiatedStorageObjectFormat().getExtension();
  }

  @Override
  public Set<String> getPartitions() {
    partitions = calculatePartitions();
    return partitions;
  }

  private Set<String> calculatePartitions() {
    List<String> topics = new
        ArrayList<>(storage.listBaseFolders(config.getTopicsFolder(),
        config.getStorageDelimiter()));
    
    List<String> partitions = recursiveTraverseFolders(topics,
        config.getFieldNameList().size());
    
    log.debug("Got partitions {}", partitions);
     
    return new HashSet<>(partitions);
  }

  private List<String> recursiveTraverseFolders(List<String> pathsSoFar, Integer depth) {
    if (0 == depth) {
      return pathsSoFar;
    } else if (pathsSoFar.isEmpty()) {
      throw new ConnectException(CloudStorageSourceConnectorCommonConfig.PARTITION_FIELD_NAME_CONFIG
          + " indicated more folders than there were as such is probably incorrectly setup.");
    } else {
      List<String> paths = new ArrayList<>();
      for (String path : pathsSoFar) {
        paths.addAll(new
            ArrayList<>(storage.listBaseFolders(path, config.getStorageDelimiter())));
      }

      return recursiveTraverseFolders(paths, depth - 1);
    }
  }

  @Override
  public String getNextObjectName(String topic, String previousObject) {
    log.trace("Get next object from topic {} using previous object {} and extension {}", topic,
        previousObject, extension
    );

    return storage.getNextFileName(topic, previousObject, extension);
  }

  @Override
  public boolean shouldReconfigure() {
    return !partitions.equals(calculatePartitions());
  }
  
}
