/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.s3.source;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.s3.model.S3ObjectSummary;

import io.confluent.connect.cloud.storage.source.StorageObject;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.cloud.storage.source.util.SourceRecordOffset;
import io.confluent.connect.cloud.storage.source.util.StorageObjectSourceReader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.confluent.connect.s3.source.S3AvroTestUtils.fromAvro;
import static io.confluent.connect.s3.source.S3AvroTestUtils.writeAvroFile;
import static io.confluent.connect.s3.source.S3ByteArrayTestUtils.fromByteArray;
import static io.confluent.connect.s3.source.S3ByteArrayTestUtils.writeByteArrayFile;
import static io.confluent.connect.s3.source.S3JsonTestUtils.fromJsonRecord;
import static io.confluent.connect.s3.source.S3JsonTestUtils.writeJsonFile;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class S3SourceTaskTest extends TestWithMockedS3 {

  private static final String BYTE_ARRAY_FORMAT_CLASS = "io.confluent.connect.s3.format.bytearray.ByteArrayFormat";
  private static final String JSON_FORMAT_CLASS = "io.confluent.connect.s3.format.json.JsonFormat";

  private S3SourceTask task;

  @Before
  public void setup() throws Exception {
    super.setUp();
    task = setupTask(properties);
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void shouldReturnNonNullVersion() {
    assertNotNull(task.version());
  }

  @Test
  public void shouldStopAndDisconnect() {
    task.stop();
    //TODO: Ensure the task stopped
  }

  @Test
  public void shouldProduceRecordsFromOneS3Object() throws ConnectException, IOException {
    final int count = 20;

    Path avroFile = writeAvroFile(count);
    s3.createBucket(S3_TEST_BUCKET_NAME);
    s3.putObject(
        S3_TEST_BUCKET_NAME,
        S3_TEST_FOLDER_TOPIC_NAME + "/partition=0/test-topic+0+0000000000.avro",
        avroFile.toFile()
    );

    for (int i = 0; i < count; i++) {
      List<SourceRecord> records = task.poll();
      assertThat("number of records does not match", records.size(), is(1));

      SourceRecord record = records.get(0);
      S3TestUtils.User user = S3AvroTestUtils.fromAvro(record);
      assertThat(user, is(new S3TestUtils.User("John", "Smith", i)));
    }
  }

  @Test
  public void shouldProduceRecordsFromMultipleS3Objects() throws IOException {
    final int files = 3;
    final int count = 20;
    s3.createBucket(S3_TEST_BUCKET_NAME);

    for (int fileNum = 0; fileNum < files; fileNum++) {
      Path avroFile = writeAvroFile(count);
      String s3ObjectKey =
          format("%s/partition=0/test-topic+0+000000000%d.avro", S3_TEST_FOLDER_TOPIC_NAME,
              fileNum
          );
      s3.putObject(S3_TEST_BUCKET_NAME, s3ObjectKey, avroFile.toFile());
    }

    for (int fileNum = 0; fileNum < files; fileNum++) {
      for (int msgNum = 0; msgNum < count; msgNum++) {
        List<SourceRecord> records = task.poll();
        assertThat("should return 1 record", records.size(), is(1));
        SourceRecord record = records.get(0);
        S3TestUtils.User user = S3AvroTestUtils.fromAvro(record);

        assertThat(user, is(new S3TestUtils.User("John", "Smith", msgNum)));
        assertThat(record.sourcePartition().get("folder"), is(S3_TEST_FOLDER_TOPIC_NAME));
        assertThat(
            record.sourceOffset().get("lastFileRead"),
            is(format("%s/partition=0/test-topic+0+000000000%d.avro", S3_TEST_FOLDER_TOPIC_NAME,
                fileNum
            ))
        );
        assertThat(record.sourceOffset().get("fileOffset"), is(String.valueOf(msgNum + 1)));
      }
    }
  }

  @Test
  public void shouldStartFromTheFirstObjectWhenOffsetIsNotRecorded() throws IOException {
    final int count = 3;
    s3.createBucket(S3_TEST_BUCKET_NAME);

    Path s3File = writeByteArrayFile(count, 0);
    String s3ObjectKey =
        format("%s/partition=0/test-topic+0+0000000000.avro", S3_TEST_FOLDER_TOPIC_NAME);
    s3.putObject(S3_TEST_BUCKET_NAME, s3ObjectKey, s3File.toFile());

    HashMap<String, String> props = new HashMap<String, String>() {{
      put("format.class", BYTE_ARRAY_FORMAT_CLASS);
    }};

    S3SourceTask firstTask = setupTask(props);
    firstTask.doStart(createContextWithNullOffsetReader());
    List<SourceRecord> records = firstTask.poll();
    SourceRecord record = records.get(0);
    S3TestUtils.User user = fromByteArray(record);
    assertThat("should be first record", user.getAge(), is(0));
  }

  @Test
  public void shouldReadRecordsInObjectAndNoMore() throws IOException {
    final int numberRecords = 25; // less than batch size
    s3.createBucket(S3_TEST_BUCKET_NAME);

    Path s3File = writeByteArrayFile(numberRecords, 0);
    String s3ObjectKey =
        format("%s/partition=0/test-topic+0+0000000000.bin", S3_TEST_FOLDER_TOPIC_NAME);
    s3.putObject(S3_TEST_BUCKET_NAME, s3ObjectKey, s3File.toFile());

    HashMap<String, String> props = new HashMap<String, String>() {{
      put("format.class", BYTE_ARRAY_FORMAT_CLASS);
      put("record.batch.max.size", "300");
    }};

    S3SourceTask firstTask = setupTask(props);
    firstTask.doStart(createContextWithNullOffsetReader());
    List<SourceRecord> records = firstTask.poll();
    assertEquals(records.size(), numberRecords);
  }


  @Test
  public void noMoreFilesToProcessReturnEmptyListsOfRecords() throws IOException {
    final int numberRecords = 25; // less than batch size
    s3.createBucket(S3_TEST_BUCKET_NAME);

    Path s3File = writeByteArrayFile(numberRecords, 0);
    String s3ObjectKey =
        format("%s/partition=0/test-topic+0+0000000000.bin", S3_TEST_FOLDER_TOPIC_NAME);
    s3.putObject(S3_TEST_BUCKET_NAME, s3ObjectKey, s3File.toFile());

    HashMap<String, String> props = new HashMap<String, String>() {{
      put("format.class", BYTE_ARRAY_FORMAT_CLASS);
      put("record.batch.max.size", "300");
    }};

    S3SourceTask firstTask = setupTask(props);
    firstTask.doStart(createContextWithNullOffsetReader());
    List<SourceRecord> records = firstTask.poll();
    List<SourceRecord> nextRound = firstTask.poll();
    assertEquals(numberRecords, records.size());
    assertNull(nextRound);
  }


  @Test
  public void whenFilesAreNotEvenThenProcessNextFolder() throws IOException {
    final int numberRecords = 25; // less than batch size
    s3.createBucket(S3_TEST_BUCKET_NAME);

    Path s3File1 = writeByteArrayFile(numberRecords, 0);
    Path s3File2 = writeByteArrayFile(numberRecords, 0);
    Path s3File3 = writeByteArrayFile(numberRecords, 0);

    String s3ObjectKey1 =
        format("%s/partition=0/test-topic+0+0000000000.bin", S3_TEST_FOLDER_TOPIC_NAME);
    String s3ObjectKey2 =
        format("%s/partition=1/test-topic+0+0000000000.bin", S3_TEST_FOLDER_TOPIC_NAME);
    String s3ObjectKey3 =
        format("%s/partition=1/test-topic+0+0000000025.bin", S3_TEST_FOLDER_TOPIC_NAME);
    s3.putObject(S3_TEST_BUCKET_NAME, s3ObjectKey1, s3File1.toFile());
    s3.putObject(S3_TEST_BUCKET_NAME, s3ObjectKey2, s3File2.toFile());
    s3.putObject(S3_TEST_BUCKET_NAME, s3ObjectKey3, s3File3.toFile());

    HashMap<String, String> props = new HashMap<String, String>() {{
      put("format.class", BYTE_ARRAY_FORMAT_CLASS);
      put("record.batch.max.size", "300");

      //set 2 folders with 1 and 2 files
      put(
          "folders",
          S3_TEST_FOLDER_TOPIC_NAME + "/partition=0/," + S3_TEST_FOLDER_TOPIC_NAME + "/partition=1"
      );
    }};

    S3SourceTask firstTask = setupTask(props);
    firstTask.doStart(createContextWithNullOffsetReader());
    List<SourceRecord> pool1 = firstTask.poll();
    List<SourceRecord> pool2 = firstTask.poll();
    List<SourceRecord> pool3 = firstTask.poll();

    assertEquals(numberRecords, pool1.size());
    assertEquals(numberRecords, pool2.size());
    assertEquals(numberRecords, pool3.size());
    Map<String, ?> pool3RecordsSourcePartition = pool3.get(0).sourcePartition();
    assertTrue(pool3RecordsSourcePartition.containsKey("folder"));
    assertEquals("topics/test-topic/partition=1", pool3RecordsSourcePartition.get("folder"));
  }

  @Test
  public void shouldcontinueFromPreviousOffsetWhenRestartedWithByteRecords() throws IOException {
    final int count = 3;
    s3.createBucket(S3_TEST_BUCKET_NAME);

    Path s3File = writeByteArrayFile(count, 0);
    String s3ObjectKey =
        format("%s/partition=0/test-topic+0+0000000000.bin", S3_TEST_FOLDER_TOPIC_NAME);
    s3.putObject(S3_TEST_BUCKET_NAME, s3ObjectKey, s3File.toFile());

    HashMap<String, String> props = new HashMap<String, String>() {{
      put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
      put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
      put("format.class", BYTE_ARRAY_FORMAT_CLASS);
    }};

    S3SourceTask firstTask = setupTask(props);

    List<SourceRecord> records = firstTask.poll();
    SourceRecord record = records.get(0);
    S3TestUtils.User user = fromByteArray(record);
    assertThat("should be first record", user.getAge(), is(0));

    long offset = Long.valueOf(String.valueOf(record.sourceOffset().get("fileOffset")));

    firstTask.stop();

    S3SourceTask secondTask = setupTask(props);
    secondTask.doStart(createContext(S3_TEST_FOLDER_TOPIC_NAME, s3ObjectKey, offset));

    records = secondTask.poll();
    user = fromByteArray(records.get(0));
    assertThat("should be first record", user.getAge(), is(1));
  }

  @Test
  public void shouldcontinueFromPreviousOffsetWhenRestartedWithJsonRecords() throws IOException {
    final int count = 3;
    s3.createBucket(S3_TEST_BUCKET_NAME);

    Path s3File = writeJsonFile(count);
    String s3ObjectKey =
        format("%s/partition=0/test-topic+0+0000000000.json", S3_TEST_FOLDER_TOPIC_NAME);
    s3.putObject(S3_TEST_BUCKET_NAME, s3ObjectKey, s3File.toFile());

    HashMap<String, String> props = new HashMap<String, String>() {{
      put("format.class", JSON_FORMAT_CLASS);
    }};

    S3SourceTask firstTask = setupTask(props);

    List<SourceRecord> records = firstTask.poll();
    SourceRecord record = records.get(0);
    S3TestUtils.User user = fromJsonRecord(record);
    assertThat("should be first record", user.getAge(), is(0));

    long offset = Long.valueOf(String.valueOf(record.sourceOffset().get("fileOffset")));

    firstTask.stop();

    S3SourceTask secondTask = setupTask(props);
    secondTask.doStart(createContext(S3_TEST_FOLDER_TOPIC_NAME, s3ObjectKey, offset));

    records = secondTask.poll();
    user = fromJsonRecord(records.get(0));
    assertThat("should be first record", user.getAge(), is(1));
  }

  @Test
  public void shouldcontinueFromPreviousOffsetWhenRestartedWithAvroRecords() throws IOException {
    final int count = 3;
    s3.createBucket(S3_TEST_BUCKET_NAME);

    Path s3File = writeAvroFile(count);
    String s3ObjectKey =
        format("%s/partition=0/test-topic+0+0000000000.avro", S3_TEST_FOLDER_TOPIC_NAME);
    s3.putObject(S3_TEST_BUCKET_NAME, s3ObjectKey, s3File.toFile());

    S3SourceTask firstTask = setupTask();

    List<SourceRecord> records = firstTask.poll();
    SourceRecord record = records.get(0);
    S3TestUtils.User user = fromAvro(record);
    assertThat("should be first record", user.getAge(), is(0));

    long offset = Long.valueOf(String.valueOf(record.sourceOffset().get("fileOffset")));

    firstTask.stop();

    S3SourceTask secondTask = setupTask();
    secondTask.doStart(createContext(S3_TEST_FOLDER_TOPIC_NAME, s3ObjectKey, offset));

    records = secondTask.poll();
    user = fromAvro(records.get(0));
    assertThat("should be first record", user.getAge(), is(1));
  }

  @Test
  public void testS3ObjectIsCloseOnceEofDone() throws IOException {
    final S3SourceConnectorConfig config = mock(S3SourceConnectorConfig.class);
    final S3Storage storage = mock(S3Storage.class);
    final Partitioner partitioner = mock(Partitioner.class);
    final String nextFileName = "object_key";
    final S3ObjectSummary objectSummary = mock(S3ObjectSummary.class);
    final StorageObject s3Object = mock(StorageObject.class);
    final StorageObjectSourceReader reader = mock(StorageObjectSourceReader.class);
    final SourceRecordOffset sourceRecordOffset = mock(SourceRecordOffset.class);

    when(config.getList(S3SourceConnectorConfig.FOLDERS_CONFIG))
        .thenReturn(Collections.singletonList("folder"));
    when(config.getRecordBatchMaxSize()).thenReturn(1);
    when(config.getBucketName()).thenReturn("bucket");
    when(config.getPartitioner(storage)).thenReturn(partitioner);
    when(partitioner.getNextObjectName(any(), any())).thenReturn(nextFileName);
    when(objectSummary.getKey()).thenReturn("object_key");
    when(storage.open("object_key")).thenReturn(s3Object);
    when(storage.bucketExists()).thenReturn(true);
    when(s3Object.getKey()).thenReturn("object_key");
    when(reader.nextRecord(s3Object, "folder", 0L)).thenReturn(sourceRecordOffset);
    when(sourceRecordOffset.isEof()).thenReturn(true);

    task = new S3SourceTask(config, storage, reader);
    task.poll();

    verify(s3Object).close();
  }

  @Test
  public void version() {
    assertNotNull(task.version());
    assertFalse(task.version().equals("0.0.0.0"));
    assertTrue(task.version().matches("^(\\d+\\.)?(\\d+\\.)?(\\*|\\d+)(-\\w+)?$"));
  }

  @Test
  public void shouldHaveDefaultConstructor() {
    S3SourceTask s3SourceTask = new S3SourceTask();
    assertNotNull(s3SourceTask);
  }

  @Test
  public void shouldBeAbleToStartTheTaskWithoutError() {
    task.initialize(noContext());
    task.start(properties);
  }

  @Test
  public void shouldBeAbleToStartWithoutAnyPreviousOffset() {
    task.initialize(emptyContext());
    task.start(properties);
  }

  @Test(expected = ConnectException.class)
  public void shouldThrowExceptionIfBucketDoesNotExist() {
    S3Storage mockStorage = mock(S3Storage.class);
    when(mockStorage.bucketExists()).thenReturn(false);

    S3SourceTask task = setupTask(mockStorage);
    task.doStart(noContext());
  }

  private S3SourceTask setupTask() {
    return setupTask(Collections.emptyMap());
  }

  private S3SourceTask setupTask(Map<String, String> overrides) {
    final HashMap<String, String> propsWithOverrides = new HashMap<>(properties);
    propsWithOverrides.putAll(overrides);

    final S3SourceTask task = new S3SourceTask(propsWithOverrides, storage);

    return task;
  }

  private S3SourceTask setupTask(S3Storage storage) {
    final S3SourceTask task = new S3SourceTask(properties, storage);

    return task;
  }

  private SourceTaskContext noContext() {
    OffsetStorageReader mockOffsetStorageReader = mock(OffsetStorageReader.class);
    when(mockOffsetStorageReader
        .offset(Collections.singletonMap("folder", S3_TEST_FOLDER_TOPIC_NAME)))
        .thenReturn(null);

    SourceTaskContext mockSourceTaskContext = mock(SourceTaskContext.class);
    when(mockSourceTaskContext.offsetStorageReader()).thenReturn(mockOffsetStorageReader);
    return mockSourceTaskContext;
  }

  private SourceTaskContext emptyContext() {
    OffsetStorageReader mockOffsetStorageReader = mock(OffsetStorageReader.class);
    Map<String, Object> sourceOffset = Collections.emptyMap();
    when(mockOffsetStorageReader
        .offset(Collections.singletonMap("folder", S3_TEST_FOLDER_TOPIC_NAME)))
        .thenReturn(sourceOffset);

    SourceTaskContext mockSourceTaskContext = mock(SourceTaskContext.class);
    when(mockSourceTaskContext.offsetStorageReader()).thenReturn(mockOffsetStorageReader);
    return mockSourceTaskContext;
  }

  private SourceTaskContext createContext(String folder, String currentFile, long offset) {
    OffsetStorageReader mockOffsetStorageReader = mock(OffsetStorageReader.class);
    HashMap<String, Object> sourceOffset = new HashMap<>();
    sourceOffset.put("lastFileRead", currentFile);
    sourceOffset.put("fileOffset", offset);
    sourceOffset.put("eof", false);
    when(mockOffsetStorageReader.offset(Collections.singletonMap("folder", folder)))
        .thenReturn(sourceOffset);

    SourceTaskContext mockSourceTaskContext = mock(SourceTaskContext.class);
    when(mockSourceTaskContext.offsetStorageReader()).thenReturn(mockOffsetStorageReader);
    return mockSourceTaskContext;
  }

  private SourceTaskContext createContextWithNullOffsetReader() {
    OffsetStorageReader mockOffsetStorageReader = mock(OffsetStorageReader.class);

    when(mockOffsetStorageReader.offset(Collections.singletonMap("folder", any())))
        .thenReturn(null);

    SourceTaskContext mockSourceTaskContext = mock(SourceTaskContext.class);
    when(mockSourceTaskContext.offsetStorageReader()).thenReturn(mockOffsetStorageReader);
    return mockSourceTaskContext;
  }
}
