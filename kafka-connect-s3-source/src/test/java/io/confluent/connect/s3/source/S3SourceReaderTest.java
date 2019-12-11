/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.s3.source;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import io.confluent.connect.cloud.storage.source.StorageObject;
import io.confluent.connect.cloud.storage.source.util.SourceRecordOffset;
import io.confluent.connect.cloud.storage.source.util.StorageObjectSourceReader;
import io.confluent.connect.s3.format.bytearray.ByteArrayFormat;
import io.confluent.connect.s3.format.json.JsonFormat;

import static io.confluent.connect.s3.source.S3AvroTestUtils.createAvroS3Object;
import static io.confluent.connect.s3.source.S3AvroTestUtils.fromAvro;
import static io.confluent.connect.s3.source.S3ByteArrayTestUtils.bytesRecordSize;
import static io.confluent.connect.s3.source.S3ByteArrayTestUtils.createByteArrayS3Object;
import static io.confluent.connect.s3.source.S3ByteArrayTestUtils.fromByteArray;
import static io.confluent.connect.s3.source.S3JsonTestUtils.createJsonS3Object;
import static io.confluent.connect.s3.source.S3JsonTestUtils.fromJsonRecord;
import static io.confluent.connect.s3.source.S3TestUtils.User;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class S3SourceReaderTest extends S3SourceConnectorTestBase {

  public static final int OFFSET_ZERO = 0;

  @Test
  public void shouldConvertToAvroRecord() throws IOException {
    StorageObjectSourceReader s3SourceReader = new StorageObjectSourceReader(
        new S3SourceConnectorConfig(createProps()));

    String folder = "folder";
    StorageObject object = createAvroS3Object(1);

    SourceRecordOffset sourceRecordOffset = s3SourceReader.nextRecord(object, folder, OFFSET_ZERO);

    SourceRecord record = sourceRecordOffset.getRecord();
    assertThat(sourceRecordOffset.getOffset(), is(1L));
    assertThat(sourceRecordOffset.isEof(), is(true));

    User user = fromAvro(record);
    assertThat(user, is(new User("John", "Smith", 0)));
    assertThat(record.sourceOffset().get("lastFileRead"), is(object.getKey()));
    assertThat(record.sourcePartition().get("folder"), is(folder));
  }

  @Test
  public void shouldConvertToListOfAvroRecords() throws IOException {
    StorageObjectSourceReader s3SourceReader = new StorageObjectSourceReader(
        new S3SourceConnectorConfig(createProps()));

    int messages = 10;
    String folder = "folder";
    StorageObject object = createAvroS3Object(messages);

    long offset = 0;
    for (int i = 0; i < messages; i++) {
      SourceRecordOffset recordOffset = s3SourceReader.nextRecord(object, folder, offset);
      offset = recordOffset.getOffset();

      User user = fromAvro(recordOffset.getRecord());
      assertThat(user, is(new User("John", "Smith", i)));
      assertThat(recordOffset.getOffset(), is(i + 1L));
    }
  }

  @Test
  public void shouldConvertToJsonRecord() throws IOException {
    Map<String, String> props = createProps();
    props.put(S3SourceConnectorConfig.FORMAT_CLASS_CONFIG, JsonFormat.class.getName());
    StorageObjectSourceReader s3SourceReader = new StorageObjectSourceReader(
        new S3SourceConnectorConfig(props));

    String folder = "folder";
    StorageObject object = createJsonS3Object(1);

    SourceRecordOffset sourceRecordOffset = s3SourceReader.nextRecord(object, folder, OFFSET_ZERO);

    SourceRecord record = sourceRecordOffset.getRecord();
    User user = fromJsonRecord(record);
    assertThat(user, is(new User("John", "Smith", 0)));
    assertThat(record.sourceOffset().get("lastFileRead"), is(object.getKey()));
    assertThat(record.sourcePartition().get("folder"), is(folder));
  }

  @Test
  public void shouldConvertToListOfJsonRecords() throws IOException {
    Map<String, String> props = createProps();
    props.put(S3SourceConnectorConfig.FORMAT_CLASS_CONFIG, JsonFormat.class.getName());
    StorageObjectSourceReader s3SourceReader = new StorageObjectSourceReader(
        new S3SourceConnectorConfig(props));

    int messages = 10;
    String folder = "folder";
    StorageObject object = createJsonS3Object(messages);

    long offset = 0;
    for (int i = 0; i < messages; i++) {
      SourceRecordOffset recordOffset = s3SourceReader.nextRecord(object, folder, offset);
      offset = recordOffset.getOffset();

      User user = fromJsonRecord(recordOffset.getRecord());
      assertThat(user, is(new User("John", "Smith", i)));
    }
  }

  @Test
  public void shouldConvertToByteArrayRecord() throws IOException {
    Map<String, String> props = createProps();
    props.put(S3SourceConnectorConfig.FORMAT_CLASS_CONFIG, ByteArrayFormat.class.getName());
    StorageObjectSourceReader s3SourceReader = new StorageObjectSourceReader(
        new S3SourceConnectorConfig(props));

    String folder = "folder";
    StorageObject object = createByteArrayS3Object(1);

    SourceRecordOffset recordOffset = s3SourceReader.nextRecord(object, folder, OFFSET_ZERO);
    SourceRecord record = recordOffset.getRecord();

    User user = fromByteArray(record);
    assertThat(user, is(new User("John", "Smith", 0)));
    assertThat(record.sourceOffset().get("lastFileRead"), is(object.getKey()));
    assertThat(record.sourcePartition().get("folder"), is(folder));
    assertThat(recordOffset.getOffset(), is(bytesRecordSize(record)));
  }

  @Test
  public void shouldConvertToListOfByteArrayRecords() throws IOException {
    Map<String, String> props = createProps();
    props.put(S3SourceConnectorConfig.FORMAT_CLASS_CONFIG, ByteArrayFormat.class.getName());
    StorageObjectSourceReader s3SourceReader = new StorageObjectSourceReader(
        new S3SourceConnectorConfig(props));

    int messages = 10;
    String folder = "folder";
    StorageObject object = createByteArrayS3Object(messages);

    long offset = 0;
    for (int i = 0; i < messages; i++) {
      SourceRecordOffset recordOffset = s3SourceReader.nextRecord(object, folder, offset);
      offset = recordOffset.getOffset();

      User user = fromByteArray(recordOffset.getRecord());
      assertThat(user, is(new User("John", "Smith", i)));
      assertThat(
          recordOffset.getOffset(),
          is(bytesRecordSize(recordOffset.getRecord()) * (i + 1))
      );
    }
  }
}