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
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.azure.storage.blob.ContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;

import io.confluent.connect.azure.blob.storage.AzureBlobSourceStorage;
import io.confluent.connect.azure.blob.storage.AzureBlobSourceStorageHelper;
import io.confluent.connect.azure.blob.storage.AzureBlobStorageSourceConnectorConfig;
import io.confluent.connect.azure.blob.storage.AzureBlobStorageSourceConnectorTestBase;

public class FieldPartitionerTest extends AzureBlobStorageSourceConnectorTestBase {
  
  AzureBlobSourceStorage storage = null;

  @Before
  public void setup() throws Exception {
    super.setUp();
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.put(AzureBlobStorageSourceConnectorConfig.PARTITIONER_CLASS_CONFIG, FieldPartitioner.class.getCanonicalName());
    props.put(AzureBlobStorageSourceConnectorConfig.PARTITION_FIELD_NAME_CONFIG, "firstName,surname,age");
    return props;
  }

  /**
   * This method will return the single partition present under the root folder.
   * e.g for the storage topics/abs-topic/John/Smith/21/abs-topic+0+0000000000.avro
   * it will return topics/abs-topic/John/Smith/21
   */
  @Test
  public void getPartitionUnderGivenPath() throws IOException {
    ContainerClient mockContainerClient = mock(ContainerClient.class);
    
    BlobItem testBlob = new BlobItem();
    testBlob.name("topics/abs-topic/");
    testBlob.isPrefix(true);
    List<BlobItem> blobList = new ArrayList<>();
    blobList.add(testBlob);
    
    AzureBlobSourceStorageHelper storageHelper = mock(AzureBlobSourceStorageHelper.class);

    when(storageHelper.getBlobList("/", "topics/"))
    .thenReturn(blobList.iterator());
    
    BlobItem testBlob1 = new BlobItem();
    testBlob1.name("topics/abs-topic/John/"); testBlob1.isPrefix(true);
    List<BlobItem> blobList1 = new ArrayList<>(); blobList1.add(testBlob1);
    
    when(storageHelper.getBlobList("/", "topics/abs-topic/"))
    .thenReturn(blobList1.iterator());
    
    BlobItem testBlob2 = new BlobItem();
    testBlob2.name("topics/abs-topic/John/Smith/"); testBlob2.isPrefix(true);
    List<BlobItem> blobList2 = new ArrayList<>(); blobList2.add(testBlob2);
    
    when(storageHelper.getBlobList("/", "topics/abs-topic/John/"))
    .thenReturn(blobList2.iterator());
    
    BlobItem testBlob3 = new BlobItem();
    testBlob3.name("topics/abs-topic/John/Smith/21/"); testBlob3.isPrefix(true);
    List<BlobItem> blobList3 = new ArrayList<>(); blobList3.add(testBlob3);
    
    when(storageHelper.getBlobList("/", "topics/abs-topic/John/Smith/"))
    .thenReturn(blobList3.iterator());
   
    storage = new AzureBlobSourceStorage(connectorConfig, mockContainerClient, storageHelper);
    
    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    Set<String> partitions = partitioner.getPartitions();
    assertTrue(partitioner instanceof FieldPartitioner);
    assertThat(partitions.size(), is(1));
    assertTrue(partitioner instanceof FieldPartitioner);
    assertThat(partitions.iterator().next(), is("topics/abs-topic/John/Smith/21/"));
  }

  /**
   * This method will return the first file present under the given partition.
   * e.g for the storage topics/abs-topic/John/Smith/21/abs-topic+0+0000000000.avro
   * it will return abs-topic+0+0000000000.avro for partition topics/abs-topic/John/Smith/21
   */
  @Test
  public void getFirstFile() throws IOException {
    ContainerClient mockContainerClient = mock(ContainerClient.class);
    
    BlobItem testBlob1 = new BlobItem();
    testBlob1.name("topics/abs-topic/John/Smith/21/abs-topic+0+0000000000.avro");
    testBlob1.isPrefix(true);
    List<BlobItem> blobList = new ArrayList<>();
    blobList.add(testBlob1);
    
    AzureBlobSourceStorageHelper storageHelper = mock(AzureBlobSourceStorageHelper.class);
    
    
    when(storageHelper.getFlatBlobList(Mockito.any(ListBlobsOptions.class)))
    .thenReturn(blobList.iterator());
    
    storage = new AzureBlobSourceStorage(connectorConfig, mockContainerClient, storageHelper);
    
    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    String firstObject = partitioner.getNextObjectName("topics/abs-topic/John/Smith/21/", null);
    assertTrue(partitioner instanceof FieldPartitioner);
    assertNotNull(firstObject);
    assertThat(
        firstObject,
        is("topics/abs-topic/John/Smith/21/abs-topic+0+0000000000.avro"));
  }

  /**
   * This method will return the second file present under the given partition.
   * e.g for the storage topics/abs-topic/John/Smith/21/abs-topic+0+0000000000.avro,
   * topics/abs-topic/John/Smith/21/abs-topic+0+0000000001.avro
   * it will return abs-topic+0+0000000000.avro for partition topics/abs-topic/John/Smith/21
   */
  @Test
  public void getSecondFileSameFolder() throws IOException {
    ContainerClient mockContainerClient = mock(ContainerClient.class);
    
    BlobItem testBlob1 = new BlobItem();
    testBlob1.name("topics/abs-topic/John/Smith/21/abs-topic+0+0000000000.avro");
    testBlob1.isPrefix(true);
    BlobItem testBlob2 = new BlobItem();
    testBlob2.name("topics/abs-topic/John/Smith/21/abs-topic+0+0000000001.avro");
    testBlob2.isPrefix(true);
    List<BlobItem> blobList = new ArrayList<>();
    blobList.add(testBlob1);
    blobList.add(testBlob2);
    
    AzureBlobSourceStorageHelper storageHelper = mock(AzureBlobSourceStorageHelper.class);
    
    when(storageHelper.getFlatBlobList(Mockito.any(ListBlobsOptions.class)))
    .thenReturn(blobList.iterator());
    
    storage = new AzureBlobSourceStorage(connectorConfig, mockContainerClient, storageHelper);
    
    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    String secondObject = partitioner.getNextObjectName("topics/abs-topic/John/Smith/21/", 
        "topics/abs-topic/John/Smith/21/abs-topic+0+0000000000.avro");
    assertTrue(partitioner instanceof FieldPartitioner);
    assertNotNull(secondObject);
    assertThat(
        secondObject,
        is("topics/abs-topic/John/Smith/21/abs-topic+0+0000000001.avro"));
  }

}
