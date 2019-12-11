/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.gcs.source;

import static io.confluent.connect.utils.licensing.LicenseConfigUtil.CONFLUENT_TOPIC_REPLICATION_FACTOR_CONFIG;
import static java.lang.String.format;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.confluent.connect.utils.licensing.LicenseConfigUtil;

public class GcsTestUtils {

  protected final String PATH_TO_CREDENTIALS_JSON = this.getClass().getClassLoader()
      .getResource("gcpcreds.json").getPath();
  protected final String PATH_TO_CREDENTIALS_PROPS = this.getClass().getClassLoader()
      .getResource("gcpcreds.properties").getPath();
  protected static final int TASKS_MAX = 1;
  protected static final int BATCH_SIZE = 1;
  
  protected static final String GCS_TEST_FOLDER_TOPIC_NAME = "topics/gcs_topic/partition=0/";
  
  protected static final String AVRO_FORMAT_CLASS =
      "io.confluent.connect.gcs.format.avro.AvroFormat";
  
  protected static final String BYTE_ARRAY_FORMAT_CLASS =
      "io.confluent.connect.gcs.format.bytearray.ByteArrayFormat";

  protected static final String JSON_FORMAT_CLASS =
      "io.confluent.connect.gcs.format.json.JsonFormat";

  protected static final String BUCKECT_PROPRTY = "gcs.bucket.name";
  protected static final String BUCKECT_NAME = "gcs_test_bucket";

  protected static final byte[] LINE_SEPARATOR = System.lineSeparator().getBytes();

  protected static final String DEFAULT_PARTITIONER = "io.confluent.connect.storage" +
      ".partitioner.DefaultPartitioner";

  protected static final String DAILY_PARTITIONER = "io.confluent.connect.storage" +
      ".partitioner.DailyPartitioner";

  protected static final String FIELD_PARTITIONER = "io.confluent.connect.storage" +
      ".partitioner.FieldPartitioner";

  protected static final String HOURLY_PARTITIONER = "io.confluent.connect.storage" +
      ".partitioner.HourlyPartitioner";

  protected static final String TIME_BASED_PARTITIONER = "io.confluent.connect.storage" +
      ".partitioner.TimeBasedPartitioner";

  protected static final String TOPIC_NAME = "gcs_topic";

  protected Map<String, String> getCommonConfig() {
    // setup up props for the source connector
    Map<String, String> props = new HashMap<>();
    props.put(CONNECTOR_CLASS_CONFIG, "io.confluent.connect.gcs.GcsSourceConnector");
    props.put("gcs.credentials.path", PATH_TO_CREDENTIALS_JSON);
    props.put(TASKS_MAX_CONFIG, Integer.toString(TASKS_MAX));

    // converters and formatters
    props.put("partitioner.class", DEFAULT_PARTITIONER);
    props.put("format.class", BYTE_ARRAY_FORMAT_CLASS);

    props.put(LicenseConfigUtil.CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(CONFLUENT_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
    props.put("record.batch.max.size", Integer.toString(BATCH_SIZE));
    props.put("gcs.poll.interval.ms", String.valueOf(TimeUnit.MINUTES.toMillis(1)));
    props.put(BUCKECT_PROPRTY, BUCKECT_NAME);

    return props;
  }

  @SuppressWarnings("deprecation")
  protected Storage uploadAvroDataToMockGcsBucket(String partitionPath,
                                                  String fileOffset) throws IOException {
    Storage storage = LocalStorageHelper.getOptions().getService();
    addNewAvroRecord(storage, partitionPath, 15, fileOffset);
    return storage;
  }

  protected void addNewAvroRecord(Storage storage, String partitionPath, int numberOfMessages,
                                  String fileOffset) throws IOException {
    // Create file with respective dummy data.
    Path fileName1 = GcsAvroTestUtils.writeDataToAvroFile(numberOfMessages, 0);
    final String gcsObjectKey1 =
        format("topics/%s/%s/%s+0+000000000%s.avro", TOPIC_NAME, partitionPath, TOPIC_NAME,
            fileOffset);

    BlobId blobId1 = BlobId.of(BUCKECT_NAME, gcsObjectKey1);
    BlobInfo blobInfo1 = BlobInfo.newBuilder(blobId1).setContentType("text/plain").build();
    storage.create(blobInfo1, new FileInputStream(fileName1.toString()));
  }

  @SuppressWarnings("deprecation")
  protected Storage createStorageAndUploadJsonRecords(int numberOfMessages) throws IOException {
    Path recordsFile = writeJsonFile(numberOfMessages);
    Storage storage = LocalStorageHelper.getOptions().getService();
    String objectKey = "topics/gcs_topic/partition=0/gcs_topic+0+0000000000.json";
    BlobId blobId1 = BlobId.of(BUCKECT_NAME, objectKey);
    BlobInfo blobInfo1 = BlobInfo.newBuilder(blobId1).setContentType("application/json").build();
    storage.create(blobInfo1, new FileInputStream(recordsFile.toString()));
    return storage;
  }

  protected Storage createStorageAndUploadByteRecords(int numberOfMessages) throws IOException {
    Path recordsFile = writeByteArrayFile(numberOfMessages, 0);
    Storage storage = LocalStorageHelper.getOptions().getService();
    String objectKey = "topics/gcs_topic/partition=0/gcs_topic+0+0000000001.bin";
    BlobId blobId1 = BlobId.of(BUCKECT_NAME, objectKey);
    BlobInfo blobInfo1 = BlobInfo.newBuilder(blobId1).setContentType("application/octet-stream").build();
    storage.create(blobInfo1, new FileInputStream(recordsFile.toString()));
    return storage;
  }

  protected void addNewByteRecords(Storage storage, int numberOfMessages, int fileOffset) throws IOException {
    Path recordsFile = writeByteArrayFile(numberOfMessages, 0);
    String objectKey = format("topics/gcs_topic/partition=0/gcs_topic+0+000000000%s.bin", fileOffset);
    BlobId blobId1 = BlobId.of(BUCKECT_NAME, objectKey);
    BlobInfo blobInfo1 = BlobInfo.newBuilder(blobId1).setContentType("application/octet-stream").build();
    storage.create(blobInfo1, new FileInputStream(recordsFile.toString()));
  }

  protected Path writeJsonFile(int messages) throws IOException {
    Path file = Files.createTempFile("users-", ".json");
    List<String> lines = new ArrayList<>();
    for (int i = 0; i < messages; i++) {
      lines.add(new String(createJsonRecordBytes(i)));
    }
    Files.write(file, lines);
    return file;
  }

  protected byte[] createJsonRecordBytes(int age) {
    Schema schema = userSchemaConnect();
    @SuppressWarnings("resource")
    JsonConverter jsonConverter = new JsonConverter();
    Map<String, Object> converterConfig = new HashMap<>();
    converterConfig.put("schemas.cache.size", String.valueOf(50));
    converterConfig.put("schemas.enable", "false");
    jsonConverter.configure(converterConfig, false);
    return jsonConverter.fromConnectData("", schema, userRecordConnect(schema, age));
  }

  protected Schema userSchemaConnect() {
    return org.apache.kafka.connect.data.SchemaBuilder.struct().name("User").version(1)
        .field("firstname", Schema.STRING_SCHEMA)
        .field("lastname", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA).build();
  }

  protected Struct userRecordConnect(Schema schema, int age) {
    return (new Struct(schema)).put("firstname", "John").put("lastname", "Smith").put("age", age);
  }

  protected Path writeByteArrayFile(int messages,  int startingIndex) throws IOException {
    Path file = Files.createTempFile("users-", ".bin");
    ByteArrayConverter converter = new ByteArrayConverter();

    for (int i = startingIndex; i < messages; i++) {
      byte[] messageBytes = converter.fromConnectData("", Schema.BYTES_SCHEMA, Base64.getEncoder()
          .encode(SerializationUtils.serialize(new GcsUserObjectTestUtils.User("John", "Smith", i))));
      Files.write(file, messageBytes, StandardOpenOption.APPEND);
      Files.write(file, LINE_SEPARATOR, StandardOpenOption.APPEND);
    }

    return file;
  }

  protected long bytesRecordSize(SourceRecord record) {
    return ((byte[]) record.value()).length + LINE_SEPARATOR.length;
  }
}
