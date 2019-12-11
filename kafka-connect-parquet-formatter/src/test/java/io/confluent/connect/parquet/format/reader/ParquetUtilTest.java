/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.parquet.format.reader;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ParquetUtilTest {

  public static Path parquetRecordWriter(int rec, String path) throws IOException {
    Schema schema = userSchemaAvro();

    List<GenericData.Record> recordList = new ArrayList<GenericData.Record>();
    for (int i = 0; i < rec; i++) {
      recordList.add(userRecordAvro(schema, i));
    }

    return writeToParquet(recordList, schema, path);
  }


  private static Path writeToParquet(List<GenericData.Record> recordList, Schema schema, String filePath) throws IOException {
    // Path to Parquet file
    Path path = new Path(filePath);
    ParquetWriter<GenericData.Record> writer = null;
    // Creating ParquetWriter using builder
      writer = AvroParquetWriter.
              <GenericData.Record>builder(path)
              .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
              .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
              .withSchema(schema)
              .withConf(new Configuration())
              .withCompressionCodec(CompressionCodecName.SNAPPY)
              .withValidation(false)
              .withDictionaryEncoding(false)
              .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
              .build();

      for (GenericData.Record record : recordList) {
        writer.write(record);
      }

      writer.close();
    return path;
  }

  private static Schema userSchemaAvro() {
    return SchemaBuilder.record("User").fields()
            .name("firstname").type().stringType().noDefault()
            .name("lastname").type().stringType().noDefault()
            .name("age").type().intType().noDefault()
            .endRecord();
  }

  private static GenericData.Record userRecordAvro(Schema schema, int age) {
    GenericData.Record user = new GenericData.Record(schema);
    user.put("firstname", "Virat");
    user.put("lastname", "Kohli");
    user.put("age", age);
    return user;
  }
}