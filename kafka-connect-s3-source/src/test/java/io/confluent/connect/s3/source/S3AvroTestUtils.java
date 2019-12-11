/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.s3.source;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

import io.confluent.connect.cloud.storage.source.StorageObject;

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

public class S3AvroTestUtils {
  
  public static S3TestUtils.User fromAvro(SourceRecord record) {
    Struct struct = (Struct) record.value();
    return new S3TestUtils.User(struct);
  }

  public static StorageObject createAvroS3Object(int messages) throws IOException {
    Path avroFile = writeAvroFile(messages);

    S3Object object = new S3Object();
    object.setObjectContent(Files.newInputStream(avroFile));
    object.setKey("topics/test-topic/partition=0/test-topic+0+0000000000.avro");
    ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(avroFile.toFile().length());
    object.setObjectMetadata(objectMetadata);

    return new S3StorageObject(object);
  }

  public static Path writeAvroFile(int messages) throws IOException {
    Path avroFile = Files.createTempFile("users-", ".avro");
    Schema schema = userSchemaAvro();
    DataFileWriter dataFileWriter = userAvroFile(schema, avroFile);

    for (int i = 0; i < messages; i++) {
      GenericRecord userRecord = userRecordAvro(schema, i);
      dataFileWriter.append(userRecord);
    }
    dataFileWriter.close();

    return avroFile;
  }

  public static GenericRecord userRecordAvro(Schema schema, int age) {
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

  public static Schema userSchemaAvro() {
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

  public static DataFileWriter userAvroFile(Schema schema, Path file) throws IOException {
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(schema, file.toFile());
    return dataFileWriter;
  }

}
