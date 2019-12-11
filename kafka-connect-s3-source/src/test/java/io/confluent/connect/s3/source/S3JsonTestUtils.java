/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.s3.source;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

import io.confluent.connect.cloud.storage.source.StorageObject;

public class S3JsonTestUtils {

  public static S3TestUtils.User fromJsonRecord(SourceRecord record) throws IOException {
    return new S3TestUtils.User((Map<String, Object>) record.value());
  }

  public static StorageObject createJsonS3Object(int messages) throws IOException {
    Path jsonFile = writeJsonFile(messages);

    S3Object object = new S3Object();
    object.setObjectContent(Files.newInputStream(jsonFile));
    object.setKey("topics/test-topic/partition=0/test-topic+0+0000000000.json");
    ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(jsonFile.toFile().length());
    object.setObjectMetadata(objectMetadata);
    return new S3StorageObject(object);
  }

  public static Path writeJsonFile(int messages) throws IOException {
    Path file = Files.createTempFile("users-", ".json");
    List<String> lines = new ArrayList<>();
    for (int i = 0; i < messages; i++) {
      lines.add(new String(createJsonRecordBytes(i)));
    }
    Files.write(file, lines);
    return file;
  }

  private static byte[] createJsonRecordBytes(int age) {
    Schema schema = userSchemaConnect();
    @SuppressWarnings("resource")
    JsonConverter jsonConverter = new JsonConverter();
    Map<String, Object> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "false");
    converterConfig.put("schemas.cache.size", String.valueOf(50));
    jsonConverter.configure(converterConfig, false);
    return jsonConverter.fromConnectData("", schema, userRecordConnect(schema, age));
  }

  public static Schema userSchemaConnect() {
    return org.apache.kafka.connect.data.SchemaBuilder.struct().name("User").version(1)
        .field("firstname", Schema.STRING_SCHEMA)
        .field("lastname", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA).build();
  }

  public static Struct userRecordConnect(Schema schema, int age) {
    return (new Struct(schema)).put("firstname", "John").put("lastname", "Smith").put("age", age);
  }

}
