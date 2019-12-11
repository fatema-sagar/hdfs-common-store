/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.gcs.source;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * Utility Class to generate .avro file with some dummy data.
 */
public class GcsAvroTestUtils {
  
  public static GcsUserObjectTestUtils.User fromAvro(SourceRecord record) {
    Struct struct = (Struct) record.value();
    return new GcsUserObjectTestUtils.User(struct);
  }

  public static Path writeDataToAvroFile(int numberOfRecords, int startingIndex) throws IOException {
    Path avroFile = Files.createTempFile("users-", ".avro");
    Schema schema = userSchemaAvro();
    
    DataFileWriter<GenericRecord> dataFileWriter = userAvroFile(schema, avroFile);

    for (int i = startingIndex; i < numberOfRecords; i++) {
      GenericRecord userRecord = userRecordAvro(schema, i);
      dataFileWriter.append(userRecord);
    }
    dataFileWriter.close();

    return avroFile;
  }
  
  private static Schema userSchemaAvro() {
    return SchemaBuilder.record("User").fields()
        .name("firstname").type().stringType().noDefault()
        .name("lastname").type().stringType().noDefault()
        .name("age").type().intType().noDefault()
        .endRecord();
  }
  
  private static DataFileWriter<GenericRecord> userAvroFile(Schema schema, Path file) throws IOException {
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(schema, file.toFile());
    return dataFileWriter;
  }
  
  private static GenericRecord userRecordAvro(Schema schema, int age) {
    GenericRecord user = new GenericData.Record(schema);
    user.put("firstname", "Virat");
    user.put("lastname", "Kohli");
    user.put("age", age);
    return user;
  }
}
