/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.gcs.source;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import static io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig.STORE_URL_CONFIG;
import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.confluent.connect.cloud.storage.source.CloudSourceStorage;
import io.confluent.connect.cloud.storage.source.StorageObject;

public class GcsSourceTaskTest extends GcsTestUtils {

  Map<String, String> settings;
  GcsSourceTask task;
  private GcsSourceConnectorConfig config;
  protected GcsSourceStorage storage;

  @Before
  public void before() {
    settings = getCommonConfig();
    settings.put("format.class", AVRO_FORMAT_CLASS);
    settings.put("folders", GCS_TEST_FOLDER_TOPIC_NAME);
    config = new GcsSourceConnectorConfig(settings);
    task = setupTask(settings);
  }

  @Test
  public void shouldReturnNonNullVersion() throws IOException {
    Storage gcs = uploadAvroDataToMockGcsBucket(TOPIC_NAME + "/partition=0", "1");
    storage = new GcsSourceStorage(config.getString(STORE_URL_CONFIG), BUCKECT_NAME, gcs);
    task = setupTask(settings);
    assertNotNull(task.version());
  }

  @Test
  public void shouldStopAndDisconnect() throws IOException {
    Storage gcs = uploadAvroDataToMockGcsBucket(TOPIC_NAME + "partition=0", "1");
    storage = new GcsSourceStorage(config.getString(STORE_URL_CONFIG), BUCKECT_NAME, gcs);
    task = setupTask(settings);
    task.stop();
    //TODO: Ensure the task stopped
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void inMemoryGoogleStorageTest() throws IOException {
    Storage storage = LocalStorageHelper.getOptions().getService();

    final String blobPath = "topics/gcs_topic/partition=0/gcs_topic+0+0000000000.avro";
    final String testBucketName = "test-bucket";
    String fileExtension = "avro";
    
    // Create file with respective dummy data.
    Path fileName1 = GcsAvroTestUtils.writeDataToAvroFile(15, 0);
    Path fileName2 = GcsAvroTestUtils.writeDataToAvroFile(30, 15);

    final String filePrefix = "topics";

    final String gcsObjectKey1 =
        format("%s/gcs_topic/partition=0/gcs_topic+0+0000000000.%s", filePrefix, fileExtension);
    final String gcsObjectKey2 =
        format("%s/gcs_topic/partition=1/gcs_topic+1+0000000015.%s", filePrefix, fileExtension);

    BlobId blobId1 = BlobId.of(testBucketName, gcsObjectKey1);
    BlobId blobId2 = BlobId.of(testBucketName, gcsObjectKey2);

    BlobInfo blobInfo1 = BlobInfo.newBuilder(blobId1).setContentType("text/plain").build();
    BlobInfo blobInfo2 = BlobInfo.newBuilder(blobId2).setContentType("text/plain").build();

    storage.create(blobInfo1, new FileInputStream(fileName1.toString()));
    storage.create(blobInfo2, new FileInputStream(fileName2.toString()));

    Iterable<Blob> allBlobsIter = storage.list(testBucketName).getValues();

    // expect to find the blob we saved when iterating over bucket blobs
    assertTrue(
        StreamSupport.stream(allBlobsIter.spliterator(), false)
            .map(BlobInfo::getName)
            .anyMatch(blobPath::equals)
    );
  }

  @Test
  public void shouldProduceAvroRecords() throws ConnectException, InterruptedException, IOException {
    Storage gcs = uploadAvroDataToMockGcsBucket( "partition=0", "1");
    storage = new GcsSourceStorage(config.getString(STORE_URL_CONFIG), BUCKECT_NAME, gcs);
    task = setupTask(settings);

    int count = 15;

    for (int i = 0; i < count; i++) {
      List<SourceRecord> records = task.poll();

      assertThat("number of records does not match", records.size(), is(BATCH_SIZE));

      SourceRecord record = records.get(0);
      GcsUserObjectTestUtils.User user = GcsAvroTestUtils.fromAvro(record);
      assertThat(user, is(new GcsUserObjectTestUtils.User("Virat", "Kohli", i)));
    }
  }

  @Test
  public void version() {
    assertNotNull(task.version());
    assertFalse(task.version().equals("0.0.0.0"));
    assertTrue(task.version().matches("^(\\d+\\.)?(\\d+\\.)?(\\*|\\d+)(-\\w+)?$"));
  }

  private GcsSourceTask setupTask(Map<String, String> overrides) {
    final HashMap<String, String> propsWithOverrides = new HashMap<>(settings);
    propsWithOverrides.putAll(overrides);

    final GcsSourceTask task = new GcsSourceTask(propsWithOverrides, storage);

    return task;
  }

  @Test
  public void validateJsonRecords() throws IOException {

    settings.put("format.class", JSON_FORMAT_CLASS);
    config = new GcsSourceConnectorConfig(settings);

    Storage jsonStorage = createStorageAndUploadJsonRecords(3);
    this.storage = new GcsSourceStorage(config.getString(STORE_URL_CONFIG), BUCKECT_NAME, jsonStorage);
    task = setupTask(settings);

    for (int i = 0; i < 3; i++) {
      List<SourceRecord> records = task.poll();
      assertThat("number of records does not match", records.size(), is(BATCH_SIZE));

      SourceRecord record = records.get(0);
      GcsUserObjectTestUtils.User user = GcsJsonTestUtils.fromJson(record);
      assertThat(user, is(new GcsUserObjectTestUtils.User("John", "Smith", i)));
    }
  }

  @Test
  public void validateBinaryRecords() throws IOException {

    settings.put("format.class", BYTE_ARRAY_FORMAT_CLASS);
    config = new GcsSourceConnectorConfig(settings);

    Storage binStorage = createStorageAndUploadByteRecords(3);
    this.storage = new GcsSourceStorage(config.getString(STORE_URL_CONFIG), BUCKECT_NAME, binStorage);
    task = setupTask(settings);

    for (int i = 0; i < 3; i++) {
      List<SourceRecord> records = task.poll();
      assertThat("number of records does not match", records.size(), is(BATCH_SIZE));

      SourceRecord record = records.get(0);
      GcsUserObjectTestUtils.User user = GcsByteArrayTestUtils.fromByteArray(record);
      assertThat(user, is(new GcsUserObjectTestUtils.User("John", "Smith", i)));
    }
  }

  @Test
  public void testStorageFunctions() throws IOException {

    settings.put("format.class", BYTE_ARRAY_FORMAT_CLASS);
    config = new GcsSourceConnectorConfig(settings);

    Storage binStorage = createStorageAndUploadByteRecords(3);
    this.storage = new GcsSourceStorage(config.getString(STORE_URL_CONFIG), BUCKECT_NAME, binStorage);
    List<String> blobs = this.storage.getListOfBlobs("topics", config.getStorageDelimiter());
    assertEquals(1, blobs.size());

    blobs = this.storage.getListOfBlobs("topics/gcs_topic/partition=0/", config.getStorageDelimiter());
    assertEquals(0, blobs.size());

    addNewByteRecords(binStorage, 3, 2);
    blobs = this.storage.getListOfBlobs("topics/gcs_topic/partition=0/", config.getStorageDelimiter());
    assertEquals(0, blobs.size());

    addNewByteRecords(binStorage, 3, 3);
    blobs = this.storage.getListOfBlobs("topics/gcs_topic/partition=0/", config.getStorageDelimiter());
    assertEquals(0, blobs.size());

    Blob nextObject = this.storage.getNextObject("topics/gcs_topic/partition=0/",
        "topics/gcs_topic/partition=0/gcs_topic+0+0000000001.bin", "bin");
    assertEquals("topics/gcs_topic/partition=0/gcs_topic+0+0000000002.bin", nextObject.getName());

    nextObject = this.storage.getNextObject("topics/gcs_topic/partition=0/",
        "topics/gcs_topic/partition=0/gcs_topic+0+0000000002.bin", "bin");
    assertEquals("topics/gcs_topic/partition=0/gcs_topic+0+0000000003.bin", nextObject.getName());

    nextObject = this.storage.getNextObject("topics/gcs_topic/partition=0/",
        "topics/gcs_topic/partition=0/gcs_topic+0+0000000003.bin", "bin");
    assertEquals(null, nextObject);
  }

  // The following test case throws connect exception since the mock storage implementation
  // does not make an actual call and instead throws a not supported exception.
  @Test(expected = ConnectException.class)
  public void testGcsClientGet() throws IOException {
    settings.put("format.class", BYTE_ARRAY_FORMAT_CLASS);
    config = new GcsSourceConnectorConfig(settings);

    Storage binStorage = createStorageAndUploadByteRecords(3);
    this.storage = new GcsSourceStorage(config.getString(STORE_URL_CONFIG), BUCKECT_NAME, binStorage);

    StorageObject newStorage = this.storage.getStorageObject("topics/gcs_topic/partition=0");
    assertNotNull(newStorage);
  }

  // The following test case throws connect exception since the code looks for a valid gcs
  // credentials file.
  @Test(expected = ConnectException.class)
  public void testTaskCreateStorage() throws IOException {
    settings.put("format.class", BYTE_ARRAY_FORMAT_CLASS);
    config = new GcsSourceConnectorConfig(settings);

    Storage binStorage = createStorageAndUploadByteRecords(3);
    this.storage = new GcsSourceStorage(config.getString(STORE_URL_CONFIG), BUCKECT_NAME, binStorage);

    task = setupTask(settings);
    CloudSourceStorage newStorage = this.task.createStorage();
    assertNotNull(newStorage);
  }
}