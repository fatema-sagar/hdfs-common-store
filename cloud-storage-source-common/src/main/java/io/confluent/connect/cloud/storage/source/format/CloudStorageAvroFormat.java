/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.cloud.storage.source.format;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;
import io.confluent.connect.cloud.storage.source.StorageObjectFormat;
import io.confluent.connect.cloud.storage.source.util.ValueOffset;

/**
 * Class for AvroFormat. 
 * It is used while reading avro data.
 */
public class CloudStorageAvroFormat extends StorageObjectFormat {

  private static final Logger log = LoggerFactory.getLogger(CloudStorageAvroFormat.class);

  public static final String DEFAULT_AVRO_EXTENSION = "avro";
  private final AvroData avroData;
  private final String extension;
  private DataFileStream<GenericRecord> reader = null;
  private Schema schema;

  public CloudStorageAvroFormat(CloudStorageSourceConnectorCommonConfig config) {
    avroData = new AvroData(config.getSchemaCacheSize());
    extension = DEFAULT_AVRO_EXTENSION;
  }

  @Override
  public String getExtension() {
    return extension;
  }

  @Override
  public ValueOffset extractRecord(InputStream content, long objectSize, long offset) {

    try {
      if (offset == 0) {
        reader = new DataFileStream<>(content, new GenericDatumReader<>());
        schema = avroData.toConnectSchema(reader.getSchema());
      }

      log.trace("Attempting to extract next avro record after offset {}", offset);

      // Possible to use reader.next(GR reuse) to avoid constantly creating new objects which
      // will be GC
      GenericRecord values = reader.next();
      Struct valueStruct = new Struct(schema);

      /**
       * Value of String type field gets converted to UTF8 type in case of GenericRecord.
       * So here, we are converting utf8 to string.
       * 
       * TODO : But when in case of Complex Data Types of Avro like Arrays, Maps, 
       * Unions etc, if they contain Utf8(String) data, there it will break. 
       * Need to make a fix for that.
       */
      values.getSchema().getFields().forEach(val -> {
        Object valueObject = values.get(val.pos());
        if (valueObject instanceof Utf8) {
          valueObject = String.valueOf(valueObject);
        }
        valueStruct.put(val.name(), valueObject);
      });

      SchemaAndValue scheamAndValue = new SchemaAndValue(schema, valueStruct);

      return new ValueOffset(scheamAndValue, offset + 1,
          !reader.hasNext()
      );
    } catch (IOException e) {
      throw new ConnectException("Could not process next avro record", e);
    }
  }

  @Override
  public void skip(InputStream content, long offset) {
    log.debug("Skipping to avro record offset {}", offset);
    try {
      reader = new DataFileStream<>(content, new GenericDatumReader<>());
      schema = avroData.toConnectSchema(reader.getSchema());

      // TODO change to DataFileReader which allows seeking
      for (int i = 0; i < offset; i++) {
        reader.next();
      }
    } catch (IOException e) {
      throw new ConnectException("Could not process avro file before skipping", e);
    }
  }

}
