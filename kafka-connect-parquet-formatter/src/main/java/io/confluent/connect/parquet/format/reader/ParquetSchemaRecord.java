/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.parquet.format.reader;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

public class ParquetSchemaRecord {
  private GenericData.Record record;
  private Schema avroSchema;

  public ParquetSchemaRecord(GenericData.Record record , Schema avroSchema) {
    this.record = record;
    this.avroSchema = avroSchema;
  }
  
  /*
   It return the list of Simple group
  */
  public GenericData.Record getRecord() {
    return record;
  }

  public Schema getAvroSchema() {
    return avroSchema;
  }
}
