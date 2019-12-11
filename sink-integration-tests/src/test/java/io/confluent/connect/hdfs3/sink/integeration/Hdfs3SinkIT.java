/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.hdfs3.sink.integeration;

import io.confluent.connect.gcs.sink.integration.RunSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.OutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedInputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;

/**
 * Integration tests to run the HDFS3 Sink Connector.
 */
public class Hdfs3SinkIT {
  private static final Logger log = LoggerFactory.getLogger(Hdfs3SinkIT.class);

  protected  String HDFS_URL;
  private static final String KAFKA_TOPIC_JSON = "hdfs3-connect-topic-default-json";
  private static final String KAFKA_TOPIC_AVRO = "hdfs3-connect-topic-default-avro";
  private static final String KAFKA_TOPIC_STRING = "hdfs3-connect-topic-default-string";
  private static final String KAFKA_TOPIC_PARQUET = "hdfs3-connect-topic-default-parquet";
  private static final String AVRO_FORMAT_CLASS = "io.confluent.connect.hdfs3.avro.AvroFormat";
  private static final String DEFAULT_PARTITIONER = "io.confluent.connect.storage.partitioner.DefaultPartitioner";
  private static final String TOPIC_DIR = "topics";
  //For large data, Sink connector is not working
  private final int NO_RECORDS_TO_GENERATE = 60;
  private FileSystem fs;

  /**
   * Execute the various Sink connector scenarious.
   * For example, this runs the sink connectors for various formats and the various partitioners.
   *@throws Exception the exception
   */
  @Before
  public void setUp() throws Exception {
    HDFS_URL = getHdfsUrl();
    Configuration conf = new Configuration();
    fs = FileSystem.get(URI.create(HDFS_URL), conf);
    //verify hdfs cluster is up or not
    createFileInHadoop();
    listFolders("check");
    deleteFiles("check");
  }

  /*
    Function to run Sink TimeBasedPartitoner
   */
  @Test
  public void runTimePartitioner() throws Exception {
    Map<String, String> propsToOverride = getConfigForSink();
    propsToOverride.put("format.class", "io.confluent.connect.hdfs3.avro.AvroFormat");
    propsToOverride.put("partitioner.class", "io.confluent.connect.storage.partitioner.TimeBasedPartitioner");
    propsToOverride.put("path.format", "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH/'mins'=mm");
    propsToOverride.put("partition.duration.ms", "10000");
    propsToOverride.put("locale", "en-US");
    propsToOverride.put("timezone", "UTC");
    propsToOverride.put("timestamp.extractor", "Wallclock");
    propsToOverride.put("topics.dir", "topics-time");
    propsToOverride.put("logs.dir", "logs-time");
    runSink(KAFKA_TOPIC_AVRO, 2, NO_RECORDS_TO_GENERATE, propsToOverride);
  }

  /*
    Function to run Sink FieldPartitoner
   */
  @Test
  public void runFieldParitioner() throws Exception {
    Map<String, String> propsToOverride = getConfigForSink();
    propsToOverride.put("format.class", "io.confluent.connect.hdfs3.avro.AvroFormat");
    propsToOverride.put("partitioner.class", "io.confluent.connect.storage.partitioner"+
            ".FieldPartitioner");
    propsToOverride.put("partition.field.name", "id,name");
    propsToOverride.put("topics.dir", "topics-field");
    propsToOverride.put("logs.dir", "logs-field");
    runSink(KAFKA_TOPIC_AVRO, 2, NO_RECORDS_TO_GENERATE, propsToOverride);
  }

  /*
    Function to run Sink HourlyPartitoner
   */
  @Test
  public void runHourlyParitioner() throws Exception {
    Map<String, String> propsToOverride = getConfigForSink();
    propsToOverride.put("format.class", "io.confluent.connect.hdfs3.avro.AvroFormat");
    propsToOverride.put("partitioner.class", "io.confluent.connect.storage.partitioner"+
            ".HourlyPartitioner");
    propsToOverride.put("locale", "en-US");
    propsToOverride.put("timezone", "UTC");
    propsToOverride.put("timestamp.extractor", "Wallclock");
    propsToOverride.put("topics.dir", "topics-hourly");
    propsToOverride.put("logs.dir", "logs-hourly");
    runSink(KAFKA_TOPIC_AVRO, 2, NO_RECORDS_TO_GENERATE, propsToOverride);
  }

  /*
    Function to run Sink DailyPartitoner
   */
  @Test
  public void runDailyParitioner() throws Exception {
    Map<String, String> propsToOverride = getConfigForSink();
    propsToOverride.put("format.class", "io.confluent.connect.hdfs3.avro.AvroFormat");
    propsToOverride.put("partitioner.class", "io.confluent.connect.storage.partitioner"+
            ".DailyPartitioner");
    propsToOverride.put("topics.dir", "topics-daily");
    propsToOverride.put("locale", "en-US");
    propsToOverride.put("timezone", "UTC");
    propsToOverride.put("timestamp.extractor", "Wallclock");
    propsToOverride.put("logs.dir", "logs-daily");
    runSink(KAFKA_TOPIC_AVRO, 2, NO_RECORDS_TO_GENERATE, propsToOverride);
  }

  /*
    Function to run all Formatters of Sink DefaultPartitoner
   */
  @Test
  public void runDefaultPartitioner() throws Exception {
    Map<String, String> propsToOverride = getConfigForSink();

    //SMT properties.
    propsToOverride.put("transforms", "InsertSource");
    propsToOverride.put("transforms.InsertSource.type", "org.apache.kafka.connect.transforms.InsertField$Value");
    propsToOverride.put("transforms.InsertSource.static.field", "data_source");
    propsToOverride.put("transforms.InsertSource.static.value", "test-file-source");
    //Json Format Properties
    propsToOverride.put("topics.dir", "topics-json");
    propsToOverride.put("logs.dir", "logs-json");
    propsToOverride.put("format.class", "io.confluent.connect.hdfs3.json.JsonFormat");
    runSink(KAFKA_TOPIC_JSON, 2, NO_RECORDS_TO_GENERATE, propsToOverride);


    propsToOverride = getConfigForSink();
    //SMT properties.
    propsToOverride.put("transforms", "InsertSource");
    propsToOverride.put("transforms.InsertSource.type", "org.apache.kafka.connect.transforms.InsertField$Value");
    propsToOverride.put("transforms.InsertSource.static.field", "data_source");
    propsToOverride.put("transforms.InsertSource.static.value", "test-file-source");

    ////Avro Format Properties
    propsToOverride.put("format.class", "io.confluent.connect.hdfs3.avro.AvroFormat");
    propsToOverride.put("topics.dir", "topics-avro");
    propsToOverride.put("logs.dir", "logs-avro");
    runSink(KAFKA_TOPIC_AVRO, 2, NO_RECORDS_TO_GENERATE, propsToOverride);

    //String Format Properties
    propsToOverride = getConfigForSink();
    propsToOverride.put("format.class", "io.confluent.connect.hdfs3.string.StringFormat");
    propsToOverride.put("topics.dir", "topics-string");
    propsToOverride.put("logs.dir", "logs-string");
    propsToOverride.put("value.converter","org.apache.kafka.connect.storage.StringConverter");
    runSink(KAFKA_TOPIC_STRING, 2, NO_RECORDS_TO_GENERATE, propsToOverride);

    propsToOverride = getConfigForSink();
    //SMT properties.
    propsToOverride.put("transforms", "InsertSource");
    propsToOverride.put("transforms.InsertSource.type", "org.apache.kafka.connect.transforms.InsertField$Value");
    propsToOverride.put("transforms.InsertSource.static.field", "data_source");
    propsToOverride.put("transforms.InsertSource.static.value", "test-file-source");
    //Parquet Format Properties
    propsToOverride.put("format.class", "io.confluent.connect.hdfs3.parquet.ParquetFormat");
    propsToOverride.put("topics.dir", "topics-parquet");
    propsToOverride.put("logs.dir", "logs-parquet");
    runSink(KAFKA_TOPIC_PARQUET, 2, NO_RECORDS_TO_GENERATE, propsToOverride);


  }

  private void runSink(String kafkaTopic, int numOfPartitions, int noOfRecordsToGenerate, Map<String, String> propOverrides) throws Exception {
    deleteFiles(propOverrides.get("topics.dir"));
    deleteFiles(propOverrides.get("logs.dir"));
    try(RunSink sink = new RunSink(kafkaTopic,"",
            numOfPartitions,"",noOfRecordsToGenerate, propOverrides)) {
      sink.start();
    }
  }

  private  void deleteFiles(String topicDir) throws IOException {
    if (fs.exists(new Path("/" + topicDir))) {
      //delete all file under topicDir
      fs.delete(new Path("/" + topicDir),true);
    }
  }
  protected Map<String, String> getConfigForSink() {
    Map<String, String> props = new HashMap<>();
    props.put("connector.class", "io.confluent.connect.hdfs3.Hdfs3SinkConnector");
    props.put("flush.size", "3");
    props.put(TASKS_MAX_CONFIG, "1");
    //change this url according to your set up
    props.put("record.batch.max.size","5");
    props.put("storage.class", "io.confluent.connect.hdfs3.storage.HdfsStorage");
    props.put("format.class", AVRO_FORMAT_CLASS);
    props.put("partitioner.class", DEFAULT_PARTITIONER);
    props.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
    props.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    props.put("key.converter.schemas.enable", "true");
    props.put("value.converter.schemas.enable", "true");
    props.put("hdfs.url",getHdfsUrl());
    props.put("topics.dir","topics");
    props.put("logs.dir","logs");
    return props;
  }

  public String getHdfsUrl() {
    try (InputStream input = new FileInputStream("bin/scriptsIT/properties/hdfs3.properties")) {
      Properties prop = new Properties();
      prop.load(input);
      return prop.getProperty("hdfs.url");
    } catch (IOException ex) {
      throw new ConnectException("Failed to get url",ex);
    }
  }

  private void createFileInHadoop() throws IOException {
    File file = File.createTempFile("temp", null);
    InputStream in1 = new BufferedInputStream(new FileInputStream(file));
    OutputStream out1 = fs.create(new org.apache.hadoop.fs.Path(HDFS_URL + "/check"));
    IOUtils.copyBytes(in1, out1, 4096, true);
    file.deleteOnExit();
  }

  private void listFolders(String folder) throws IOException {
    if(fs.exists(new Path(HDFS_URL + "/" +folder))) {
      RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(
              new Path(HDFS_URL + "/" +folder), true);
      while (fileStatusListIterator.hasNext()) {
        log.info(fileStatusListIterator.next().getPath().toString());
      }
    }
  }
}
