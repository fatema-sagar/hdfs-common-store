/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.common;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.google.cloud.storage.StorageOptions;

import io.confluent.connect.gcs.sink.integration.RunSink;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Integration tests to run the GCS Sink Connector.
 */
public class SinkIT {

  private static final Logger log = LoggerFactory.getLogger(SinkIT.class);
  private static final String PATH_TO_CREDENTIALS = "scriptsIT/creds/gcpcreds.json";
  private static final String GCS_BUCKET = "gcs-sink-source-it-tests";
  private static final String KAFKA_TOPIC_JSON = "gcs-connect-topic-json";
  private static final String KAFKA_TOPIC_AVRO = "gcs-connect-topic-avro";
  private static final String KAFKA_TOPIC_BYTE = "gcs-connect-topic-byte";

  private static final String BUCKET_JSON = "gcs-sink-source-it-test-json";
  private static final String BUCKET_AVRO = "gcs-sink-source-it-test-avro";
  private static final String BUCKET_BYTE = "gcs-sink-source-it-test-byte";
  private static final String BUCKET_FIELD = "gcs-sink-source-it-test-field";
  private static final String BUCKET_TIME = "gcs-sink-source-it-test-time";

  private final int NO_RECORDS_TO_GENERATE = 3000;

  /**
   * Execute the various Sink connector scenarious.
   * For example, this runs the sink connectors for various formats and the various partitioners.
   *
   * @throws Exception the exception
   */
  @Test
  public void testSink() throws Exception {
    runSinkIts();
  }

  private void runSinkIts() throws Exception {
    // Build credentials.
    StorageOptions.Builder builder = StorageOptions.newBuilder();
    GoogleCredentials credentials = GoogleCredentials
        .fromStream(new FileInputStream(PATH_TO_CREDENTIALS))
        .createScoped(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
    builder.setCredentials(credentials);
    Storage storage = builder.build().getService();

    // Run tests with default partitioner.
    runDefaultPartitioner(storage);

    // Run tests with time based partitioner.
    runTimePartitioner(storage);

    // Run field partitioner
    runFieldParitioner(storage);

  }

  private void runTimePartitioner(Storage storage) throws Exception {
    Map<String, String> propsToOverride = new HashMap<>();
    propsToOverride.put("gcs.bucket.name", BUCKET_TIME);
    propsToOverride.put("format.class", "io.confluent.connect.gcs.format.avro.AvroFormat");
    propsToOverride.put("partitioner.class", "io.confluent.connect.storage.partitioner.TimeBasedPartitioner");
    propsToOverride.put("path.format", "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH/'mins'=mm");
    propsToOverride.put("partition.duration.ms", "10000");
    propsToOverride.put("locale", "en-US");
    propsToOverride.put("timezone", "UTC");
    propsToOverride.put("timestamp.extractor", "Wallclock");
    runSink(storage, KAFKA_TOPIC_AVRO, 2, BUCKET_TIME, NO_RECORDS_TO_GENERATE, propsToOverride);
  }

  private void runFieldParitioner(Storage storage) throws Exception {
    Map<String, String> propsToOverride = new HashMap<>();
    propsToOverride.put("gcs.bucket.name", BUCKET_FIELD);
    propsToOverride.put("format.class", "io.confluent.connect.gcs.format.avro.AvroFormat");
    propsToOverride.put("partitioner.class", "io.confluent.connect.storage.partitioner" +
        ".FieldPartitioner");
    propsToOverride.put("partition.field.name", "name");
    runSink(storage, KAFKA_TOPIC_AVRO, 2, BUCKET_FIELD, 2000, propsToOverride);
  }

  private void runDefaultPartitioner(Storage storage) throws Exception {
    Map<String, String> propsToOverride = new HashMap<>();

    // Default Paritioner JSON format
    propsToOverride.put("gcs.bucket.name", BUCKET_JSON);
    //SMT properties.
    propsToOverride.put("transforms", "InsertSource");
    propsToOverride.put("transforms.InsertSource.type", "org.apache.kafka.connect.transforms.InsertField$Value");
    propsToOverride.put("transforms.InsertSource.static.field", "data_source");
    propsToOverride.put("transforms.InsertSource.static.value", "test-file-source");
    runSink(storage, KAFKA_TOPIC_JSON, 2, BUCKET_JSON, NO_RECORDS_TO_GENERATE, Collections.EMPTY_MAP);


    propsToOverride = new HashMap<>();
    //SMT properties.
    propsToOverride.put("transforms", "InsertSource");
    propsToOverride.put("transforms.InsertSource.type", "org.apache.kafka.connect.transforms.InsertField$Value");
    propsToOverride.put("transforms.InsertSource.static.field", "data_source");
    propsToOverride.put("transforms.InsertSource.static.value", "test-file-source");

    // Default Paritioner AVRO format
    propsToOverride.put("format.class", "io.confluent.connect.gcs.format.avro.AvroFormat");
    propsToOverride.put("gcs.bucket.name", BUCKET_AVRO);
    runSink(storage, KAFKA_TOPIC_AVRO, 2, BUCKET_AVRO, NO_RECORDS_TO_GENERATE, propsToOverride);

    propsToOverride = new HashMap<>();
    // Default Paritioner BYTE ARRAY format
    propsToOverride.put("format.class", "io.confluent.connect.gcs.format.bytearray.ByteArrayFormat");
    propsToOverride.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
    propsToOverride.put("gcs.bucket.name", BUCKET_BYTE);
    runSink(storage, KAFKA_TOPIC_BYTE, 2, BUCKET_BYTE, NO_RECORDS_TO_GENERATE, propsToOverride);
  }

  private void runSink(Storage storage, String kafkaTopic, int numOfPartitions,
                              String bucket, int noOfRecordsToGenerate, Map<String, String> propOverrides) throws Exception {
    // Check bucket exists, if not create.
    createBucketIfNotExistsAndClearDirectories(storage, bucket);
    try(RunSink sink = new RunSink(kafkaTopic, PATH_TO_CREDENTIALS,
        numOfPartitions, bucket, noOfRecordsToGenerate, propOverrides)) {
      sink.start();
    }
  }

  private void deleteFiles(Storage storage, String gcsBucketName) {
    if (storage.get(gcsBucketName, Storage.BucketGetOption.fields()) != null) {
      log.info("Listing files under :  bucket : " + gcsBucketName);
      Iterable<Blob> blobs = storage.list(gcsBucketName,
          Storage.BlobListOption.prefix("")).iterateAll();
      for (Blob blob : blobs) {
        log.info("Deleting : " + blob.getName());
        blob.delete();
      }
    }
  }

  private void createBucketIfNotExistsAndClearDirectories(Storage storage, String gcsBucketName) {
    if (storage.get(gcsBucketName, Storage.BucketGetOption.fields()) == null) {
      log.info("Creating bucket : " + gcsBucketName);
      storage.create(BucketInfo.newBuilder(gcsBucketName)
          .setStorageClass(StorageClass.STANDARD)
          .setLocation("us-east1")
          .build());
    }
    // delete the files under the bucket.
    deleteFiles(storage, gcsBucketName);
  }
}
