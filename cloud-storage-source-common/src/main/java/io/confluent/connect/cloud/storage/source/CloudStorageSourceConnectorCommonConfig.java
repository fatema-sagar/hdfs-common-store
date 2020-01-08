/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.cloud.storage.source;

import io.confluent.connect.utils.licensing.LicenseConfigUtil;
import io.confluent.connect.utils.recommenders.Recommenders;
import io.confluent.connect.utils.validators.Validators;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.confluent.connect.utils.validators.Validators.instanceOf;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

/**
 * This Class has common configurations related to Cloud source storage connector.
 * Source connector configuration class of connector like S3, GCS, Azure blob source has to extend 
 * this class and add extra specific configurations accordingly.
 */
public abstract class CloudStorageSourceConnectorCommonConfig extends AbstractConfig {

  public static final String STORE_URL_CONFIG = "store.url";
  public static final String STORE_URL_DOC = "Store's connection URL, if applicable.";
  public static final String STORE_URL_DEFAULT = null;
  public static final String STORE_URL_DISPLAY = "Store URL";
  
  public static final String FORMAT_BYTEARRAY_EXTENSION_CONFIG = "format.bytearray.extension";
  public static final String FORMAT_BYTEARRAY_EXTENSION_DEFAULT = ".bin";
  public static final String FORMAT_BYTEARRAY_EXTENSION_DOC = String
      .format(
          "Output file extension for Byte Array Format. Defaults to ``%s``.",
          FORMAT_BYTEARRAY_EXTENSION_DEFAULT
      );
  public static final String FORMAT_BYTEARRAY_EXTENSION_DISPLAY = "Output file extension for "
      + "ByteArrayFormat";

  public static final String FORMAT_BYTEARRAY_LINE_SEPARATOR_CONFIG = "format.bytearray.separator";
  public static final String FORMAT_BYTEARRAY_LINE_SEPARATOR_DOC =
      "String inserted between records for ByteArrayFormat. "
          + "Defaults to ``System.lineSeparator()`` "
          + "and may contain escape sequences like ``\\n``. "
          + "An input record that contains the line separator looks like "
          + "multiple records in the storage object output.";
  public static final String FORMAT_BYTEARRAY_LINE_SEPARATOR_DEFAULT = System.lineSeparator();
  public static final String FORMAT_BYTEARRAY_LINE_SEPARATOR_DISPLAY = "Line separator "
      + "ByteArrayFormat";

  public static final String FORMAT_CLASS_CONFIG = "format.class";
  public static final String FORMAT_CLASS_DOC =
      "Class responsible for converting storage objects to source records.";
  public static final String FORMAT_CLASS_DISPLAY = "Format class";

  public static final String TOPICS_DIR_CONFIG = "topics.dir";
  public static final String TOPICS_DIR_DEFAULT = "topics";
  public static final String TOPICS_DIR_DOC =
      "Top level directory where data was stored to be re-ingested by Kafka.";
  private static final String TOPICS_DIR_DISPLAY = "Topics Directory";

  public static final String DIRECTORY_DELIM_CONFIG = "directory.delim";
  public static final String DIRECTORY_DELIM_DEFAULT = "/";
  public static final String DIRECTORY_DELIM_DOC = "Directory delimiter pattern.";
  private static final String DIRECTORY_DELIM_DISPLAY = "Directory delimiter character";

  public static final String PARTITION_FIELD_NAME_CONFIG = "partition.field.name";
  public static final String PARTITION_FIELD_NAME_DOC =
      "The name of the partitioning field when FieldPartitioner is used.";
  public static final String PARTITION_FIELD_NAME_DEFAULT = "";
  private static final String PARTITION_FIELD_NAME_DISPLAY = "Partition Field Names";

  public static final String PATH_FORMAT_CONFIG = "path.format";
  public static final String PATH_FORMAT_DEFAULT = "";
  public static final String PATH_FORMAT_DOC =
      "This configuration that was used to set the format of the data directories when "
          + "partitioning with a TimeBasedPartitioner. For example, if you set ``path.format`` to "
          + "``'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH``, then a valid data directories would "
          + "be: ``/year=2015/month=12/day=07/hour=15/``.";
  private static final String PATH_FORMAT_DISPLAY = "Partition Path Format";

  public static final String SCHEMA_CACHE_SIZE_CONFIG = "schema.cache.size";
  public static final String SCHEMA_CACHE_SIZE_DOC =
      "The size of the schema cache used in the Avro converter.";
  public static final int SCHEMA_CACHE_SIZE_DEFAULT = 50;
  public static final String SCHEMA_CACHE_SIZE_DISPLAY = "Schema Cache Size";

  public static final String RECORD_BATCH_MAX_SIZE_CONFIG = "record.batch.max.size";
  public static final String RECORD_BATCH_MAX_SIZE_DOC =
      "The maximum amount of records to return each time storage is polled.";
  public static final int RECORD_BATCH_MAX_SIZE_DEFAULT = 200;
  public static final String RECORD_BATCH_MAX_SIZE_DISPLAY = "Record batch max size";
  
  public static final String FOLDERS_CONFIG = "folders";

  public static final String FILE_NAME_REGEX_PATTERN = "filename.regex";
  private static final String FILE_NAME_REGEX_PATTERN_DEFAULT = "(.+)\\+(\\d+)\\+.+$";

  public CloudStorageSourceConnectorCommonConfig(Map<String, String> props) {
    super(config(), props);
  }
  
  protected CloudStorageSourceConnectorCommonConfig(ConfigDef configDef,
      Map<String, String> props) {
    super(configDef, props);
  }

  /**
   * This is a method implementation which every cloud storage should implement.
   * This method should return the poll interval for the specific connector.
   * @return the interval duration for the connector.
   */
  protected abstract Long getPollInterval();

  public static ConfigDef config() {
    
    final String groupStorage = "Storage";
    final String groupPartitioner = "Partitioner";
    final String groupConnector = "Connector";
    int orderInGroup = 0;
    ConfigDef configDef = new ConfigDef();

    configDef
        .define(
            STORE_URL_CONFIG,
            ConfigDef.Type.STRING,
            STORE_URL_DEFAULT,
            ConfigDef.Importance.HIGH,
            STORE_URL_DOC,
            groupStorage,
            ++orderInGroup,
            ConfigDef.Width.LONG,
            STORE_URL_DISPLAY);
    
    configDef
        .define(
            FORMAT_BYTEARRAY_EXTENSION_CONFIG,
            ConfigDef.Type.STRING,
            FORMAT_BYTEARRAY_EXTENSION_DEFAULT,
            ConfigDef.Importance.LOW,
            FORMAT_BYTEARRAY_EXTENSION_DOC,
            groupStorage,
            ++orderInGroup,
            ConfigDef.Width.LONG,
            FORMAT_BYTEARRAY_EXTENSION_DISPLAY);

    configDef
        .define(
            FORMAT_BYTEARRAY_LINE_SEPARATOR_CONFIG,
            ConfigDef.Type.STRING,
            // Because ConfigKey automatically trims strings, we cannot set
            // the default here and instead inject null;
            // the default is applied in getFormatByteArrayLineSeparator().
            null,
            ConfigDef.Importance.LOW,
            FORMAT_BYTEARRAY_LINE_SEPARATOR_DOC,
            groupStorage,
            ++orderInGroup,
            ConfigDef.Width.LONG,
            FORMAT_BYTEARRAY_LINE_SEPARATOR_DISPLAY);

    configDef
        .define(
            FORMAT_CLASS_CONFIG,
            ConfigDef.Type.CLASS,
            ConfigDef.NO_DEFAULT_VALUE,
            Validators.anyOf(
                instanceOf(StorageObjectFormat.class)
            ),
            ConfigDef.Importance.HIGH,
            FORMAT_CLASS_DOC,
            groupConnector,
            ++orderInGroup,
            ConfigDef.Width.LONG,
            FORMAT_CLASS_DISPLAY,
            Recommenders
                .anyOf(
                        StorageObjectFormat.class));

    configDef
        .define(
            SCHEMA_CACHE_SIZE_CONFIG,
            ConfigDef.Type.INT,
            SCHEMA_CACHE_SIZE_DEFAULT,
            atLeast(1),
            ConfigDef.Importance.LOW,
            SCHEMA_CACHE_SIZE_DOC,
            groupConnector,
            ++orderInGroup,
            ConfigDef.Width.SHORT,
            SCHEMA_CACHE_SIZE_DISPLAY);

    configDef
        .define(
            RECORD_BATCH_MAX_SIZE_CONFIG,
            ConfigDef.Type.INT,
            RECORD_BATCH_MAX_SIZE_DEFAULT,
            atLeast(1),
            ConfigDef.Importance.MEDIUM,
            RECORD_BATCH_MAX_SIZE_DOC,
            groupConnector,
            ++orderInGroup,
            ConfigDef.Width.SHORT,
            RECORD_BATCH_MAX_SIZE_DISPLAY);

    configDef
        .define(
            TOPICS_DIR_CONFIG,
            ConfigDef.Type.STRING,
            TOPICS_DIR_DEFAULT,
            ConfigDef.Importance.HIGH,
            TOPICS_DIR_DOC,
            groupStorage,
            ++orderInGroup,
            ConfigDef.Width.SHORT,
            TOPICS_DIR_DISPLAY);

    configDef
        .define(
            DIRECTORY_DELIM_CONFIG,
            ConfigDef.Type.STRING,
            DIRECTORY_DELIM_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            DIRECTORY_DELIM_DOC,
            groupStorage,
            ++orderInGroup,
            ConfigDef.Width.SHORT,
            DIRECTORY_DELIM_DISPLAY);

    configDef
        .define(
            PARTITION_FIELD_NAME_CONFIG,
            ConfigDef.Type.LIST,
            PARTITION_FIELD_NAME_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            PARTITION_FIELD_NAME_DOC,
            groupPartitioner,
            ++orderInGroup,
            ConfigDef.Width.LONG,
            PARTITION_FIELD_NAME_DISPLAY);

    configDef
        .define(
            PATH_FORMAT_CONFIG,
            ConfigDef.Type.STRING,
            PATH_FORMAT_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            PATH_FORMAT_DOC,
            groupPartitioner,
            ++orderInGroup,
            ConfigDef.Width.LONG,
            PATH_FORMAT_DISPLAY);
    
    /**
     *  This is not visible to user, only for internal use
     *  It holds list of folders for this task to watch for changes.
     */
    configDef
        .defineInternal(
            FOLDERS_CONFIG,
            ConfigDef.Type.LIST,
            Collections.EMPTY_LIST,
            ConfigDef.Importance.HIGH);

    /**
     *  This is not visible to user, only for internal use
     *  It holds file name/storage object name regex pattern.
     *  This is used in StorageObjectKeyParts.java class
     */
    configDef
        .defineInternal(
            FILE_NAME_REGEX_PATTERN,
            ConfigDef.Type.STRING,
            FILE_NAME_REGEX_PATTERN_DEFAULT,
            ConfigDef.Importance.MEDIUM);

    return LicenseConfigUtil.addToConfigDef(configDef);
  }

  public int getSchemaCacheSize() {
    return getInt(SCHEMA_CACHE_SIZE_CONFIG);
  }

  public Class<?> getStorageObjectFormatClass() {
    return getClass(FORMAT_CLASS_CONFIG);
  }

  public String getStorageDelimiter() {
    return getString(DIRECTORY_DELIM_CONFIG);
  }

  public String getTopicsFolder() {
    return getString(TOPICS_DIR_CONFIG) + getString(DIRECTORY_DELIM_CONFIG);
  }

  public List<String> getFieldNameList() {
    return getList(PARTITION_FIELD_NAME_CONFIG);
  }

  public String getByteArrayExtension() {
    return getString(FORMAT_BYTEARRAY_EXTENSION_CONFIG);
  }

  public int getRecordBatchMaxSize() {
    return getInt(RECORD_BATCH_MAX_SIZE_CONFIG);
  }

  public String getFormatByteArrayLineSeparator() {
    // White space is significant for line separators, but ConfigKey trims it out,
    // so we need to check the originals rather than using the normal machinery.
    if (originalsStrings().containsKey(FORMAT_BYTEARRAY_LINE_SEPARATOR_CONFIG)) {
      return originalsStrings().get(FORMAT_BYTEARRAY_LINE_SEPARATOR_CONFIG);
    }
    return FORMAT_BYTEARRAY_LINE_SEPARATOR_DEFAULT;
  }
  
  public StorageObjectFormat getInstantiatedStorageObjectFormat() {
    try {
      @SuppressWarnings("unchecked") Class<StorageObjectFormat> formatClass =
          (Class<StorageObjectFormat>) this.getStorageObjectFormatClass();
      return formatClass
          .getConstructor(CloudStorageSourceConnectorCommonConfig.class)
          .newInstance(this);
    } catch (NoSuchMethodException
        | InstantiationException
        | IllegalAccessException
        | InvocationTargetException e) {
      throw new ConnectException("Failed to instantiate StorageObjectFormat class ");
    }
  }

  public static void main(String[] args) {
    System.out.println(config().toEnrichedRst());
  }

}

