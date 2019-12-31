/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.hdfs.source;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class HDAvroTestUtils {

  public static HDTestUtils.User fromAvro(SourceRecord record) {
    Struct struct = (Struct) record.value();
    return new HDTestUtils.User(struct);
  }

  public static Path writeAvroFile(int messages) throws IOException {
    Path avroFile = Files.createTempFile("users-", ".avro");
    Schema schema = userSchemaAvro();
    DataFileWriter dataFileWriter = userAvroFile(schema, avroFile);
    for (int i=0; i<messages; i++) {
      GenericRecord userRecord = userRecordAvro(schema, i);
      dataFileWriter.append(userRecord);
    }
    dataFileWriter.close();
    return avroFile;
  }

  private static GenericRecord userRecordAvro(Schema schema, int age) {
    GenericRecord user = new GenericData.Record(schema);
    user.put("firstname", "John");
    user.put("lastname", "Smith");
    user.put("age", age);
    user.put("averageMarks", 55.7f);
    user.put("booleanValue", false);
    user.put("doubleValue", 5.66);
    user.put("longValue", 654L);
    return user;
  }

  private static DataFileWriter userAvroFile(Schema schema, Path avroFile) throws IOException {
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>();
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(schema, avroFile.toFile());
    return dataFileWriter;
  }

  private static Schema userSchemaAvro() {
    return SchemaBuilder.record("User").fields()
        .name("firstname").type().stringType().noDefault()
        .name("lastname").type().stringType().noDefault()
        .name("age").type().intType().noDefault()
        .name("averageMarks").type().floatType().noDefault()
        .name("booleanValue").type().booleanType().noDefault()
        .name("doubleValue").type().doubleType().noDefault()
        .name("longValue").type().longType().noDefault()
        .endRecord();
  }
}
