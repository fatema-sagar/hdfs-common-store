/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.storage.partitioner;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.azure.storage.blob.ContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;

import io.confluent.connect.azure.blob.storage.AzureBlobSourceStorage;
import io.confluent.connect.azure.blob.storage.AzureBlobSourceStorageHelper;
import io.confluent.connect.azure.blob.storage.AzureBlobStorageSourceConnectorTestBase;

public class DefaultPartitionerTest extends AzureBlobStorageSourceConnectorTestBase {

  AzureBlobSourceStorage storage = null;

  @Before
  public void setup() throws Exception {
    super.setUp();
  }

  /**
   * This method will return the single partition present under the root folder.
   * e.g for the storage topics/abs-topic/partition=0/abs-topic+0+0000000000.avro
   * it will return topics/abs-topic/partition=0/
   */
  @Test
  public void getSinglePartitionUnderGivenPath() throws IOException {
    ContainerClient mockContainerClient = mock(ContainerClient.class);
    
    BlobItem testBlob = new BlobItem();
    testBlob.name("topics/abs-topic/partition=0/");
    testBlob.isPrefix(true);
    List<BlobItem> blobList = new ArrayList<>();
    blobList.add(testBlob);
    
    AzureBlobSourceStorageHelper storageHelper = mock(AzureBlobSourceStorageHelper.class);
    
    when(storageHelper.getBlobList(Mockito.anyString(), Mockito.any(BlobItem.class)))
    .thenReturn(blobList.iterator());
    
    when(storageHelper.getBlobList(Mockito.anyString(), Mockito.anyString()))
    .thenReturn(blobList.iterator());
    
    storage = new AzureBlobSourceStorage(connectorConfig, mockContainerClient, storageHelper);
    
    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    Set<String> partitions = partitioner.getPartitions();
    assertTrue(partitioner instanceof DefaultPartitioner);
    assertThat(partitions.size(), is(1));
    assertTrue(partitioner instanceof DefaultPartitioner);
    assertThat(partitions.iterator().next(), is("topics/abs-topic/partition=0/"));
     
  }

  /**
   * This method will return the single partition present under the root folder.
   * e.g for the storage topics/abs-topic/partition=0/abs-topic+0+0000000000.avro,
   * topics/abs-topic/partition=1/abs-topic+1+0000000000.avro
   * it will return topics/abs-topic/partition=0/, topics/abs-topic/partition=1/
   */
  @Test
  public void getTwoPartitionsUnderGivenPath() throws IOException {
    ContainerClient mockContainerClient = mock(ContainerClient.class);
    
    BlobItem testBlob1 = new BlobItem();
    testBlob1.name("topics/abs-topic/partition=0");
    testBlob1.isPrefix(true);
    BlobItem testBlob2 = new BlobItem();
    testBlob2.name("topics/abs-topic/partition=1");
    testBlob2.isPrefix(true);
    List<BlobItem> blobList = new ArrayList<>();
    blobList.add(testBlob1);
    blobList.add(testBlob2);
    
    AzureBlobSourceStorageHelper storageHelper = mock(AzureBlobSourceStorageHelper.class);
    
    when(storageHelper.getBlobList(Mockito.anyString(), Mockito.any(BlobItem.class)))
    .thenReturn(blobList.iterator());
    
    when(storageHelper.getBlobList(Mockito.anyString(), Mockito.anyString()))
    .thenReturn(blobList.iterator());
    
    storage = new AzureBlobSourceStorage(connectorConfig, mockContainerClient, storageHelper);
    
    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    Set<String> partitions = partitioner.getPartitions();
    assertTrue(partitioner instanceof DefaultPartitioner);
    assertThat(partitions.size(), is(2));
    assertTrue(partitioner instanceof DefaultPartitioner);
    assertTrue(partitions.contains("topics/abs-topic/partition=0"));
    assertTrue(partitions.contains("topics/abs-topic/partition=1"));
  }

  /**
   * This method will return the first file present under the given partition.
   * e.g for the storage topics/abs-topic/partition=0/abs-topic+0+0000000000.avro
   * it will return abs-topic+0+0000000000.avro for partition topics/abs-topic/partition=0
   */
  @Test
  public void getFirstFile() throws IOException {
    ContainerClient mockContainerClient = mock(ContainerClient.class);
    
    BlobItem testBlob1 = new BlobItem();
    testBlob1.name("topics/abs-topic/partition=0/abs-topic+0+0000000000.avro");
    testBlob1.isPrefix(true);
    List<BlobItem> blobList = new ArrayList<>();
    blobList.add(testBlob1);
    
    AzureBlobSourceStorageHelper storageHelper = mock(AzureBlobSourceStorageHelper.class);
    
    
    when(storageHelper.getFlatBlobList(Mockito.any(ListBlobsOptions.class)))
    .thenReturn(blobList.iterator());
    
    storage = new AzureBlobSourceStorage(connectorConfig, mockContainerClient, storageHelper);
    
    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    String firstObject = partitioner.getNextObjectName("topics/abs-topic/partition=0/", null);
    assertTrue(partitioner instanceof DefaultPartitioner);
    assertNotNull(firstObject);
    assertThat(
        firstObject,
        is("topics/abs-topic/partition=0/abs-topic+0+0000000000.avro"));
  }

  /**
   * This method will return the second file present under the given partition.
   * e.g for the storage topics/abs-topic/partition=0/abs-topic+0+0000000000.avro,
   * topics/abs-topic/partition=0/abs-topic+0+0000000001.avro
   * it will return abs-topic+0+0000000001.avro for partition topics/abs-topic/partition=0
   */
  @Test
  public void getSecondFileSameFolder() throws IOException {
    ContainerClient mockContainerClient = mock(ContainerClient.class);
    
    BlobItem testBlob1 = new BlobItem();
    testBlob1.name("topics/abs-topic/partition=0/abs-topic+0+0000000000.avro");
    testBlob1.isPrefix(true);
    BlobItem testBlob2 = new BlobItem();
    testBlob2.name("topics/abs-topic/partition=0/abs-topic+0+0000000001.avro");
    testBlob2.isPrefix(true);
    List<BlobItem> blobList = new ArrayList<>();
    blobList.add(testBlob1);
    blobList.add(testBlob2);
    
    AzureBlobSourceStorageHelper storageHelper = mock(AzureBlobSourceStorageHelper.class);
    
    
    when(storageHelper.getFlatBlobList(Mockito.any(ListBlobsOptions.class)))
    .thenReturn(blobList.iterator());
    
    storage = new AzureBlobSourceStorage(connectorConfig, mockContainerClient, storageHelper);
    
    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    String secondObject = partitioner.getNextObjectName("topics/abs-topic/partition=0/", 
        "topics/abs-topic/partition=0/abs-topic+0+0000000000.avro");
    assertTrue(partitioner instanceof DefaultPartitioner);
    assertNotNull(secondObject);
    assertThat(
        secondObject,
        is("topics/abs-topic/partition=0/abs-topic+0+0000000001.avro"));
  }

  /**
   * This method will return the second file present under the other partition.
   * e.g for the storage topics/abs-topic/partition=0/abs-topic+0+0000000000.avro,
   * topics/abs-topic/partition=1/abs-topic+1+0000000001.avro
   * it will return abs-topic+1+0000000000.avro for partition topics/abs-topic/partition=0
   */
  @Test
  public void getSecondFileDifferentFolder() throws IOException {
    ContainerClient mockContainerClient = mock(ContainerClient.class);
    
    BlobItem testBlob1 = new BlobItem();
    testBlob1.name("topics/abs-topic/partition=0/abs-topic+0+0000000000.avro");
    testBlob1.isPrefix(true);
    BlobItem testBlob2 = new BlobItem();
    testBlob2.name("topics/abs-topic/partition=1/abs-topic+1+0000000000.avro");
    testBlob2.isPrefix(true);
    List<BlobItem> blobList = new ArrayList<>();
    blobList.add(testBlob1);
    blobList.add(testBlob2);
    
    AzureBlobSourceStorageHelper storageHelper = mock(AzureBlobSourceStorageHelper.class);
    
    
    when(storageHelper.getFlatBlobList(Mockito.any(ListBlobsOptions.class)))
    .thenReturn(blobList.iterator());
    
    storage = new AzureBlobSourceStorage(connectorConfig, mockContainerClient, storageHelper);
    
    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    String secondObject = partitioner.getNextObjectName("topics/abs-topic/partition=0/", 
        "topics/abs-topic/partition=0/abs-topic+0+0000000000.avro");
    assertTrue(partitioner instanceof DefaultPartitioner);
    assertNotNull(secondObject);
    assertThat(
        secondObject,
        is("topics/abs-topic/partition=1/abs-topic+1+0000000000.avro"));
  }

}