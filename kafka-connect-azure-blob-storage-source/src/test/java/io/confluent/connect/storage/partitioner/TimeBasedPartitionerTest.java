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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
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

public class TimeBasedPartitionerTest extends AzureBlobStorageSourceConnectorTestBase {

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
    props.put(AzureBlobStorageSourceConnectorConfig.PARTITIONER_CLASS_CONFIG, TimeBasedPartitioner.class.getCanonicalName());
    props.put(AzureBlobStorageSourceConnectorConfig.PATH_FORMAT_CONFIG, "'second'=ss/'month'=MM/'day'=dd/'hour'=HH/'year'=YYYY");
    return props;
  }

  /**
   * This method will return the single topic folder present under the root folder.
   * e.g for the storage topics/abs-topic/second=45/month=12/day=01/hour=23/year=2015/abs-topic+0+0000000000.avro
   * it will return topics/abs-topic/
   */
  @Test
  public void getSinglePartitionsUnderGivenPath() throws IOException {
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
    testBlob1.name("topics/abs-topic/second=45/month=12/day=01/hour=23/year=2015/abs-topic+0+0000000000.avro");
    testBlob1.isPrefix(true);
    List<BlobItem> blobList1 = new ArrayList<>();
    blobList1.add(testBlob1);
    
    when(storageHelper.getListOfBlobResponse("topics/abs-topic/"))
    .thenReturn(blobList1);
    
    storage = new AzureBlobSourceStorage(connectorConfig, mockContainerClient, storageHelper);
    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    assertTrue(partitioner instanceof TimeBasedPartitioner);
    Set<String> partitions = partitioner.getPartitions();
    assertThat(partitions.size(), is(1));
    assertThat(partitions.iterator().next(), is("topics/abs-topic/"));
  }

  /**
   * This method will return the two topic folder present under the root folder.
   * e.g for the storage topics/abs-topic/second=45/month=12/day=01/hour=23/year=2015/abs-topic+0+0000000000.avro,
   * topics/abs-other-topic/second=45/month=12/day=01/hour=23/year=2015/abs-other-topic+0+0000000000.avro
   * it will return topics/abs-topic/, topics/abs-other-topic/
   */
  @Test
  public void getTwoPartitionsUnderGivenPath() throws IOException {
    ContainerClient mockContainerClient = mock(ContainerClient.class);
    
    BlobItem testBlob = new BlobItem();
    testBlob.name("topics/abs-topic/");
    testBlob.isPrefix(true);
    BlobItem testBlobOther = new BlobItem();
    testBlobOther.name("topics/abs-other-topic/");
    testBlobOther.isPrefix(true);
    List<BlobItem> blobList = new ArrayList<>();
    blobList.add(testBlob);
    blobList.add(testBlobOther);
    
    AzureBlobSourceStorageHelper storageHelper = mock(AzureBlobSourceStorageHelper.class);
    
    when(storageHelper.getBlobList("/", "topics/"))
    .thenReturn(blobList.iterator());
    
    BlobItem testBlob1 = new BlobItem();
    testBlob1.name("topics/abs-topic/second=45/month=12/day=01/hour=23/year=2015/abs-topic+0+0000000000.avro");
    testBlob1.isPrefix(true);
    BlobItem testBlob2 = new BlobItem();
    testBlob2.name("topics/abs-other-topic/second=45/month=12/day=01/hour=23/year=2015/abs-other-topic+0+0000000000.avro");
    testBlob2.isPrefix(true);
    List<BlobItem> blobList1 = new ArrayList<>();
    blobList1.add(testBlob1);
    blobList1.add(testBlob2);
    
    when(storageHelper.getListOfBlobResponse("topics/"))
    .thenReturn(blobList1);
    
    storage = new AzureBlobSourceStorage(connectorConfig, mockContainerClient, storageHelper);
    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    assertTrue(partitioner instanceof TimeBasedPartitioner);
    Set<String> partitions = partitioner.getPartitions();
    final Iterator<String> iterator = partitions.iterator();
    assertThat(partitions.size(), is(2));
    assertThat(iterator.next(), is("topics/abs-topic/"));
    assertThat(iterator.next(), is("topics/abs-other-topic/"));
  }

  /**
   * This method will return the first file present under the given partition.
   * e.g for the storage topics/abs-topic/second=45/month=12/day=01/hour=23/year=2015/abs-topic+0+0000000000.avro
   * it will return abs-topic+0+0000000000.avro for partition topics/abs-topic/
   */
  @Test
  public void getFirstFile() throws IOException {
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
    testBlob1.name("topics/abs-topic/second=45/month=12/day=01/hour=23/year=2015/abs-topic+0+0000000000.avro");
    testBlob1.isPrefix(true);
    List<BlobItem> blobList1 = new ArrayList<>();
    blobList1.add(testBlob1);
    
    when(storageHelper.getListOfBlobResponse("topics/abs-topic/"))
    .thenReturn(blobList1);
    
    BlobItem testFileBlob = new BlobItem();
    testFileBlob.name("topics/abs-topic/second=45/month=12/day=01/hour=23/year=2015/abs-topic+0+0000000000.avro");
    testFileBlob.isPrefix(true);
    List<BlobItem> blobFileList = new ArrayList<>();
    blobFileList.add(testFileBlob);
    
    when(storageHelper.getFlatBlobList(Mockito.any(ListBlobsOptions.class)))
    .thenReturn(blobFileList.iterator());
    
    storage = new AzureBlobSourceStorage(connectorConfig, mockContainerClient, storageHelper);
    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    assertTrue(partitioner instanceof TimeBasedPartitioner);
    Set<String> partitions = partitioner.getPartitions();
    String firstObject =
        partitioner.getNextObjectName(partitions.iterator().next(), "");
    assertNotNull(firstObject);
    assertThat(
        firstObject,
        is("topics/abs-topic/second=45/month=12/day=01/hour=23/year=2015/abs-topic+0+0000000000.avro")
    );
  }

  /**
   * This method will covert folders to date.
   * e.g for the storage second=45/month=12/day=01/hour=23/year=2015/test-topic+0+0000000000.avro,
   * it should return in Date format
   */
  @Test
  public void shouldConvertAzureObjectKeyToDate() {
    TimeBasedPartitioner partitioner =
        (TimeBasedPartitioner) connectorConfig.getPartitioner(storage);

    String topic = "topics/test-topic/";
    DateTime dateTime = partitioner.dateFromString(
        topic,
        topic + "second=45/month=12/day=01/hour=23/year=2015/test-topic+0+0000000000.avro"
    );

    assertThat(dateTime.getYear(), is(2015));
    assertThat(dateTime.getMonthOfYear(), is(12));
    assertThat(dateTime.getDayOfMonth(), is(1));
    assertThat(dateTime.getHourOfDay(), is(23));
    assertThat(dateTime.getSecondOfMinute(), is(45));
  }
   
}
