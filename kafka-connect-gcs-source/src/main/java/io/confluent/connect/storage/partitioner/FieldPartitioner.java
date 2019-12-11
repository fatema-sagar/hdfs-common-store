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

import com.google.cloud.storage.Blob;

import io.confluent.connect.gcs.source.GcsSourceConnectorConfig;
import io.confluent.connect.gcs.source.GcsSourceStorage;

/**
 * This class is for Field Partitioner logic and it implements 
 * Partitioner interface.
 * 
 * <p>Folder structure is based on the partition from the field within each record 
 * identified by the connector's partition.field.name configuration 
 * property, which has no default.</p>
 */
public class FieldPartitioner implements Partitioner {

  private static final Logger log = LoggerFactory.getLogger(FieldPartitioner.class);

  private final GcsSourceConnectorConfig config;
  private final GcsSourceStorage gcsStorage;
  private final String extension;
  private Set<String> partitions = Collections.emptySet();
  
  public FieldPartitioner(GcsSourceConnectorConfig config, GcsSourceStorage gcsStorage) {
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
  public boolean shouldReconfigure() {
    return !partitions.equals(calculatePartitions());
  }

  private Set<String> calculatePartitions() {
    List<String> topics = gcsStorage.getListOfBlobs(config.getTopicsFolder(),
            config.getStorageDelimiter());

    Set<String> partitions = new HashSet<>();
    List<String> blobs = recursiveTraverseFolders(topics, config.getFieldNameList().size());
    
    blobs.forEach(blob -> partitions.add(blob));

    log.debug("Got partitions {}", partitions);
    return partitions;
  }
  
  private List<String> recursiveTraverseFolders(List<String> pathsSoFar, Integer depth) {
    if (0 == depth) {
      return pathsSoFar;
    } else if (pathsSoFar.isEmpty()) {
      throw new ConnectException("partition.field.name"
          + " indicated more folders than there were as such is probably incorrectly setup.");
    } else {
      List<String> paths = new ArrayList<>();
      pathsSoFar.forEach(
          blob -> paths.addAll(gcsStorage.getListOfBlobs(blob, config.getStorageDelimiter()))
      );
      
      return recursiveTraverseFolders(paths, depth - 1);
    }
  }

  @Override
  public String getNextObjectName(String topic, String previousObject) {
    Blob blobObject = gcsStorage.getNextObject(topic, previousObject, extension);

    if (blobObject != null && blobObject.getName().endsWith(extension)) {
      return blobObject.getName();
    }
    return "";
  }
}
