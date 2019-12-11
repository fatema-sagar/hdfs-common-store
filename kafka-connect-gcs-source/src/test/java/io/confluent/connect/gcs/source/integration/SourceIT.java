/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.gcs.source.integration;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Integration tests which run against the buckets and files created from the GCS Sink.
 * See SinkIT class under sink-integration-tests module.
 *
 */
public class SourceIT {

  private static final Logger log = LoggerFactory.getLogger(SourceIT.class);

  private static final String PATH_TO_CREDENTIALS = "../sink-integration-tests/scriptsIT/creds" +
      "/gcpcreds.json";
  private static final String GCS_BUCKET = "gcs-sink-source-it-tests";
  private static final String KAFKA_TOPIC = "gcs-connect-topic";

  private static final String KAFKA_TOPIC_JSON = "gcs-connect-topic-json";
  private static final String KAFKA_TOPIC_AVRO = "gcs-connect-topic-avro";
  private static final String KAFKA_TOPIC_BYTE = "gcs-connect-topic-byte";

  private static final String BUCKET_JSON = "gcs-sink-source-it-test-json";
  private static final String BUCKET_AVRO = "gcs-sink-source-it-test-avro";
  private static final String BUCKET_BYTE = "gcs-sink-source-it-test-byte";
  private static final String BUCKET_FIELD = "gcs-sink-source-it-test-field";
  private static final String BUCKET_TIME = "gcs-sink-source-it-test-time";

  private final int RECORDS_TO_ASSERT = 3000;

  @Test
  public void runSinkSourceIts() throws Exception {
    runSourceITTests();
  }

  private void runSourceITTests() throws Exception {
    // Build credentials.
    StorageOptions.Builder builder = StorageOptions.newBuilder();
    GoogleCredentials credentials = GoogleCredentials
        .fromStream(new FileInputStream(PATH_TO_CREDENTIALS))
        .createScoped(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
    builder.setCredentials(credentials);
    Storage storage = builder.build().getService();

    runDefaultPartitioner(storage);
    runTimePartitioner(storage);
    runFieldPartitioner(storage);
  }

  private void runTimePartitioner(Storage storage) throws Exception {
    Map<String, String> propsToverride = new HashMap<>();
    propsToverride.put("gcs.credentials.path", PATH_TO_CREDENTIALS);
    propsToverride.put("format.class", "io.confluent.connect.gcs.format.avro.AvroFormat");
    propsToverride.put("gcs.bucket.name", BUCKET_TIME);
    propsToverride.put("partitioner.class", "io.confluent.connect.storage.partitioner.TimeBasedPartitioner");
    propsToverride.put("path.format", "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH/'mins'=mm");
    listFolders(storage, BUCKET_TIME, KAFKA_TOPIC_AVRO);
    runSource(storage, KAFKA_TOPIC_AVRO, 2, BUCKET_TIME, RECORDS_TO_ASSERT, propsToverride);
  }

  private void runFieldPartitioner(Storage storage) throws Exception {
    Map<String, String> propsToOverride = new HashMap<>();
    propsToOverride.put("gcs.credentials.path", PATH_TO_CREDENTIALS);
    propsToOverride.put("format.class", "io.confluent.connect.gcs.format.avro.AvroFormat");
    propsToOverride.put("gcs.bucket.name", BUCKET_FIELD);
    propsToOverride.put("partitioner.class", "io.confluent.connect.storage.partitioner.FieldPartitioner");
    propsToOverride.put("partition.field.name", "name");
    listFolders(storage, BUCKET_FIELD, KAFKA_TOPIC_AVRO);
    runSource(storage, KAFKA_TOPIC_AVRO, 2, BUCKET_FIELD, 2000, propsToOverride);
  }

  private void runDefaultPartitioner(Storage storage) throws Exception {
    Map<String, String> propsToOverride = new HashMap<>();
    propsToOverride.put("gcs.credentials.path", PATH_TO_CREDENTIALS);

    // Default - json
    propsToOverride.put("format.class", "io.confluent.connect.gcs.format.json.JsonFormat");
    propsToOverride.put("gcs.bucket.name", BUCKET_JSON);
    listFolders(storage, BUCKET_JSON, KAFKA_TOPIC_JSON);
    runSource(storage, KAFKA_TOPIC_JSON, 2, BUCKET_JSON, RECORDS_TO_ASSERT, propsToOverride);

    // Default - avro
    propsToOverride = new HashMap<>();
    propsToOverride.put("gcs.credentials.path", PATH_TO_CREDENTIALS);
    listFolders(storage, BUCKET_AVRO, KAFKA_TOPIC_AVRO);
    propsToOverride.put("gcs.bucket.name", BUCKET_AVRO);
    propsToOverride.put("format.class", "io.confluent.connect.gcs.format.avro.AvroFormat");
    runSource(storage, KAFKA_TOPIC_AVRO, 2, BUCKET_AVRO, RECORDS_TO_ASSERT, propsToOverride);

    // Default - byte
    propsToOverride = new HashMap<>();
    propsToOverride.put("gcs.credentials.path", PATH_TO_CREDENTIALS);
    listFolders(storage, BUCKET_BYTE, KAFKA_TOPIC_BYTE);
    propsToOverride.put("gcs.bucket.name", BUCKET_BYTE);
    propsToOverride.put("format.class", "io.confluent.connect.gcs.format.bytearray.ByteArrayFormat");
    runSource(storage, KAFKA_TOPIC_BYTE, 2, BUCKET_BYTE, RECORDS_TO_ASSERT, propsToOverride);
  }

  private void runSource(Storage storage, String kafkaTopic, int numOfPartitions,
                              String bucket, int noOfRecordsToGenerate, Map<String, String> propOverrides) throws Exception {

    try(RunGcsSource source = new RunGcsSource(storage, PATH_TO_CREDENTIALS, kafkaTopic,
        numOfPartitions, bucket, noOfRecordsToGenerate, propOverrides)) {
      source.start();
    }
  }

  private void listFolders(Storage storage, String gcsBucketName, String prefix) {
    if (storage.get(gcsBucketName, Storage.BucketGetOption.fields()) != null) {
      log.info("Listing files under :  bucket : " + gcsBucketName + " prefix: " + prefix );
      Iterable<Blob> blobs = storage.list(gcsBucketName,
          Storage.BlobListOption.prefix("topics/" + prefix)).iterateAll();
      for (Blob blob : blobs) {
        log.info(blob.getName());
      }
    }
  }
}
