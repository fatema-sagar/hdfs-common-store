/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.parquet.format.reader;

import io.confluent.connect.parquet.format.exception.UnSupportedStreamTypeException;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class RecordExtractor {

  private static final Logger log = LoggerFactory.getLogger(RecordExtractor.class);

  /**
   * This Method is used to return all the Parquet records from given {@link InputStream}
   *
   * @param inputStream must be of type
   *                    {@link FSDataInputStream} or {@link FileInputStream}
   * @return {@link List} of type {@link SimpleGroup} containing all the records from given
   * {@link InputStream}
   * @throws IOException throws Exception.
   */
  public List<ParquetSchemaRecord> getParquetRecords(InputStream inputStream)
          throws IOException {
    List<ParquetSchemaRecord> grecords = new ArrayList<>();

    if (!(inputStream instanceof FileInputStream)) {
      if (!(inputStream instanceof FSDataInputStream)) {
        log.error("Unsupported type of InputStream {}", inputStream.getClass().getName());
        throw new UnSupportedStreamTypeException("InputStream expected of type "
                + FileInputStream.class.getName()
                + " or " + FSDataInputStream.class.getName() + " but found "
                + inputStream.getClass().getName());
      }
    }

    InputFile inputFile = new ParquetInputFile(inputStream);
    ParquetFileReader reader = ParquetFileReader.open(inputFile);
    MessageType schema = reader.getFooter().getFileMetaData().getSchema();
    PageReadStore pages;

    while ((pages = reader.readNextRowGroup()) != null) {
      long rows = pages.getRowCount();
      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
      RecordReader recordReader = columnIO.getRecordReader(pages,
              new GroupRecordConverter(schema));

      for (int i = 0; i < rows; i++) {
        SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
        GenericData.Record record = new GenericData
                .Record(new AvroSchemaConverter().convert(schema));
        /*
        * To avoid type incompatibility we have to create a GenericRecord.
        * There is no predefined way of generating GenericRecord from Parquet
        * So, here we are manually creating a generic record by putting all the
        * keys and values from provided SimpleGroup Object.
        */
        for (int j = 0; j < simpleGroup.getType().getFields().size(); j++) {
          Type type = simpleGroup.getType().getType(j);
          switch (type.asPrimitiveType().getPrimitiveTypeName().toString()) {
            case "FLOAT": record.put(j, simpleGroup.getFloat(j, 0));
              break;
            case "INT32": record.put(j, simpleGroup.getInteger(j, 0));
              break;
            case "INT64": record.put(j, simpleGroup.getLong(j, 0));
              break;
            case "INT96": record.put(j, simpleGroup.getLong(j, 0));
              break;
            case "DOUBLE": record.put(j, simpleGroup.getDouble(j, 0));
              break;
            case "BOOLEAN": record.put(j, simpleGroup.getBoolean(j, 0));
              break;
            case "BINARY": record.put(j, simpleGroup.getString(j, 0));
              break;
            default: record.put(j, simpleGroup.getString(j, 0));
          }
        }
        grecords.add(new ParquetSchemaRecord(record,
                 new AvroSchemaConverter().convert(schema)));
      }
    }
    log.info("Reader Closed");
    return grecords;
  }

  /**
   * This method skip the records to a particular offset
   */
  public void skip(InputStream inputStream, long offset)
          throws IOException {
    if (!(inputStream instanceof FileInputStream)) {
      if (!(inputStream instanceof FSDataInputStream)) {
        log.error("Unsupported type of InputStream {}", inputStream.getClass().getName());
        throw new UnSupportedStreamTypeException("InputStream expected of type "
                + FileInputStream.class.getName()
                + " or " + FSDataInputStream.class.getName() + " but found "
                + inputStream.getClass().getName());
      }
    }

    log.debug("Reading records from {}", inputStream.getClass().getName());
    InputFile inputFile = new ParquetInputFile(inputStream);
    ParquetFileReader reader = ParquetFileReader.open(inputFile);
    for (int i = 0 ; i < offset; i++) {
      reader.skipNextRowGroup();
    }
  }
}
