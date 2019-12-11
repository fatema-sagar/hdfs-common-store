/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.cloud.storage.source.util;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

import io.confluent.connect.storage.partitioner.Partitioner;

/**
 * This Class is used for monitoring if new partition has been added or not.
 * If yes then it requests TaskReconfiguration. 
 */
public class PartitionCheckingTask implements Runnable {

  public static final Logger log = LoggerFactory.getLogger(PartitionCheckingTask.class);

  private final Partitioner partitioner;
  private final ConnectorContext context;
  private final AtomicBoolean isConnectorRunning;

  public PartitionCheckingTask(
      Partitioner partitioner, ConnectorContext context,
      AtomicBoolean isConnectorRunning
  ) {
    this.partitioner = partitioner;
    this.context = context;
    this.isConnectorRunning = isConnectorRunning;
  }

  @Override
  public void run() {
    log.info("PartitionCheckingTask - Checking if Partitions have changed.");
    try {
      if (partitioner.shouldReconfigure() && isConnectorRunning.get()) {
        context.requestTaskReconfiguration();
      }
    } catch (Exception e) {
      //Swallowing as either this is transient issue or the connector will break in the task.
      //Also the getPartititons() must've passed at least once for the task to be created
      log.error("Issue with checking for changes with Partitions", e);
    }
  }


}
