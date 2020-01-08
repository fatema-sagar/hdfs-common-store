/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.hdfs.source;

import io.confluent.connect.hdfs.source.HDTestUtils.User;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HDJsonTestUtils {

  public static User fromJsonRecord(SourceRecord record) throws IOException {
    return new User((Map<String, Object>) record.value());
  }

  public static Path writeJsonFile(int messages) throws IOException {
    Path file = Files.createTempFile("users-", "json");
    List<String> lines = new ArrayList<>();
    for (int i=0; i<messages; i++) {
      lines.add(new String(createJsonRecordBytes(i)));
    }
    Files.write(file, lines);
    return file;
  }

  private static byte[] createJsonRecordBytes(int age) {
    Schema schema = userSchemaConnect();
    JsonConverter jsonConverter = new JsonConverter();
    Map<String, Object> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "false");
    converterConfig.put("schemas.cache.size", String.valueOf(50));
    jsonConverter.configure(converterConfig, false);
    return jsonConverter.fromConnectData("", schema, userRecordConnect(schema, age));
  }

  private static Struct userRecordConnect(Schema schema, int age) {
    return (new Struct(schema)).put("firstname", "John")
        .put("lastname", "Smith")
        .put("age", age);
  }

  private static Schema userSchemaConnect() {
    return SchemaBuilder.struct().name("User").version(1)
        .field("firstname", Schema.STRING_SCHEMA)
        .field("lastname", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA).build();
  }
}
