/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.cloud.storage.source;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.junit.Test;
import org.mockito.Mockito;

import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.cloud.storage.source.util.PartitionCheckingTask;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;

public class PartitionCheckingTaskTest {

  @Test
  public void monitorThreadShouldNotTriggerAReconfigurationIfTheTaskIsStopped() {
    final Partitioner partitioner = mock(Partitioner.class);
    Mockito.when(partitioner.shouldReconfigure()).thenReturn(true);
    final ConnectorContext mockContext = mock(ConnectorContext.class);
    final AtomicBoolean mockIsConnectorRunning = new AtomicBoolean(true);
    PartitionCheckingTask partitionCheckingTask = new PartitionCheckingTask(partitioner,
        mockContext,
        mockIsConnectorRunning
    );

    partitionCheckingTask.run();

    Mockito.verify(mockContext, timeout(5000).times(1)).requestTaskReconfiguration();

    Mockito.reset(mockContext);
    mockIsConnectorRunning.set(false);

    partitionCheckingTask.run();

    Mockito.verify(mockContext, timeout(5000).times(0)).requestTaskReconfiguration();
  }

}