/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.azure.blob.storage;

import static io.confluent.connect.storage.common.util.StringUtils.isNotBlank;
import static io.confluent.connect.utils.validators.Validators.anyOf;
import static io.confluent.connect.utils.validators.Validators.oneOfLowercase;
import static io.confluent.connect.utils.validators.Validators.oneOfUppercase;
import static io.confluent.connect.utils.validators.Validators.validUri;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.confluent.connect.storage.partitioner.DailyPartitioner;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.FieldPartitioner;
import io.confluent.connect.storage.partitioner.HourlyPartitioner;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import io.confluent.connect.storage.partitioner.Partitioner;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.errors.ConnectException;

import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;

import io.confluent.connect.cloud.storage.source.CloudSourceStorage;
import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;
import io.confluent.connect.utils.Strings;
import io.confluent.connect.utils.recommenders.Recommenders;

/**
 * Configuration class for {@link AzureBlobStorageSourceConnector}.
 */
public class AzureBlobStorageSourceConnectorConfig extends CloudStorageSourceConnectorCommonConfig {

  public static final String AZ_POLL_INTERVAL_MS_CONFIG = "azblob.poll.interval.ms";
  public static final Long AZ_POLL_INTERVAL_MS_DEFAULT = TimeUnit.MINUTES.toMillis(1);
  public static final String AZ_POLL_INTERVAL_MS_DOC =
      "Frequency in milliseconds to poll for new or removed folders. This may result in updated "
          + "task configurations starting to poll for data in added folders or stopping polling "
          + "for data in removed folders.";
  public static final String AZ_POLL_INTERVAL_MS_DISPLAY =
      "Metadata Change Monitoring Interval (ms)";

  public static final String AZ_ACCOUNT_NAME_CONFIG = "azblob.account.name";
  public static final String AZ_ACCOUNT_NAME_DOC = "The account name: "
      + "Must be between 3-23 alphanumeric characters.";
  public static final String AZ_ACCOUNT_NAME_DISPLAY = "Account Name";
  
  public static final String AZ_ACCOUNT_SAS_TOKEN_CONFIG = "azblob.sas.token";
  public static final String AZ_ACCOUNT_SAS_TOKEN_DOC = "A shared access signature (SAS) is a URI "
      + "that grants restricted access rights to Azure Storage resources. You can provide a shared "
      + "access signature to clients who should not be trusted with your storage account key but "
      + "whom you wish to delegate access to certain storage account resources. By distributing a "
      + "shared access signature URI to these clients, you grant them access to a resource for a "
      + "specified period of time. An account-level SAS can delegate access to multiple storage "
      + "services (i.e. blob, file, queue, table). Note that stored access policies are currently "
      + "not supported for an account-level SAS.";
  public static final String AZ_ACCOUNT_SAS_TOKEN_DISPLAY = "Account Shared Access Signature";

  public static final String AZ_ACCOUNT_KEY_CONFIG = "azblob.account.key";
  public static final String AZ_ACCOUNT_KEY_DOC = "The Azure Storage account key: "
      + "Must not be null or empty, if you want to acces azure account using account key "
      + "instead of ``" + AZ_ACCOUNT_SAS_TOKEN_CONFIG 
      + "``.";
  public static final String AZ_ACCOUNT_KEY_DISPLAY = "Account Key";

  public static final String AZ_STORAGE_CONTAINER_NAME_CONFIG = "azblob.container.name";
  public static final String AZ_STORAGE_CONTAINER_NAME_DEFAULT = "default";
  public static final String AZ_STORAGE_CONTAINER_NAME_DOC = "The container name. "
      + "Must be between 3-63 alphanumeric and '-' characters.";
  public static final String AZ_STORAGE_CONTAINER_NAME_DISPLAY = "Container Name";
  
  public static final String AZ_PROXY_URL_CONFIG = "azblob.proxy.url";
  public static final String AZ_PROXY_URL_DEFAULT = "";
  public static final String AZ_PROXY_URL_DOC = "Azure Proxy settings encoded in URL syntax. "
      + "This property is meant to be used only if you need to access Azure through a proxy.";
  public static final String AZ_PROXY_URL_DISPLAY = "Azure Proxy Settings";

  public static final String AZ_PROXY_USER_CONFIG = "azblob.proxy.user";
  public static final String AZ_PROXY_USER_DEFAULT = null;
  public static final String AZ_PROXY_USER_DOC = "Azure Proxy User. "
      + "This property is meant to be used only if you "
      + "need to access Azure through a proxy. Using ``" + AZ_PROXY_USER_CONFIG
      + "`` instead of embedding the username and password in ``" + AZ_PROXY_URL_CONFIG
      + "`` allows the password to be hidden in the logs.";
  public static final String AZ_PROXY_USER_DISPLAY = "Azure Proxy User";

  public static final String AZ_PROXY_PASS_CONFIG = "azblob.proxy.password";
  public static final Password AZ_PROXY_PASS_DEFAULT = new Password(null);
  public static final String AZ_PROXY_PASS_DOC = "Azure Proxy Password. "
      + "This property is meant to be used only if you "
      + "need to access Azure through a proxy. Using ``" + AZ_PROXY_PASS_CONFIG
      + "`` instead of embedding the username and password in ``" + AZ_PROXY_URL_CONFIG
      + "`` allows the password to be hidden in the logs.";
  public static final String AZ_PROXY_PASS_DISPLAY = "Azure Proxy Password";

  public static final String AZ_RETRY_RETRIES_CONFIG = "azblob.retry.retries";
  public static final int AZ_RETRY_RETRIES_DEFAULT = 3;
  public static final String AZ_RETRY_RETRIES_DOC = "Specifies the maximum number of "
      + "retries attempts an operation will be tried before producing an error. "
      + "A value of 1 means 1 try and 1 retries. The actual number "
      + "of retry attempts is determined by the Azure client based on multiple factors, "
      + "including, but not limited to - the value of this parameter, type of exception "
      + "occurred, throttling settings of the underlying Azure client, etc.";
  public static final String AZ_RETRY_RETRIES_DISPLAY = "Azure Blob Storage Max Retries";

  public static final String AZ_RETRY_BACKOFF_MS_CONFIG = "azblob.retry.backoff.ms";
  public static final int AZ_RETRY_BACKOFF_MS_DEFAULT = 4000;
  public static final String AZ_RETRY_BACKOFF_MS_DOC = "Specifies the amount of delay to "
      + "use before retrying an operation in milliseconds. "
      + "The delay increases (exponentially or linearly) with each retry up to a "
      + "maximum specified by MaxRetryDelay";
  public static final String AZ_RETRY_BACKOFF_MS_DISPLAY = "Retry Backoff (ms)";
  
  public static final String AZ_RETRY_MAX_BACKOFF_MS_CONFIG = "azblob.retry.max.backoff.ms";
  public static final int AZ_RETRY_MAX_BACKOFF_MS_DEFAULT = 120000;
  public static final String AZ_RETRY_MAX_BACKOFF_MS_DOC = "Specifies the maximum delay in "
      + "milliseconds allowed before retrying an operation.";
  public static final String AZ_RETRY_MAX_BACKOFF_MS_DISPLAY = "Max Retry Backoff (ms)";
  
  public static final String AZ_RETRY_TYPE_CONFIG = "azblob.retry.type";
  public static final String AZ_RETRY_TYPE_DEFAULT = RetryPolicyType.EXPONENTIAL.name();
  public static final String AZ_RETRY_TYPE_DOC = "The policy specifying the type of "
      + "retry pattern to use. Should be either "
      + RetryPolicyType.EXPONENTIAL.name() + " or " + RetryPolicyType.FIXED.name()
      + ". An EXPONENTIAL policy will start with a delay of the value of ``"
      + AZ_RETRY_BACKOFF_MS_CONFIG
      + "`` in (ms) then double for each retry up to a max in time ("
      + AZ_RETRY_MAX_BACKOFF_MS_CONFIG + ") or total retries (" + AZ_RETRY_RETRIES_CONFIG
      + "). A FIXED policy will always just use the value of " 
      + AZ_RETRY_BACKOFF_MS_CONFIG
      + " (ms) as the delay up to a total number of tries equal to the value of "
      + AZ_RETRY_RETRIES_CONFIG + ".";
  public static final String AZ_RETRY_TYPE_DISPLAY = "Azure Blob Storage Type of Retry";

  public static final String AZ_COMPRESSION_TYPE_CONFIG = "az.compression.type";
  public static final String AZ_COMPRESSION_TYPE_DEFAULT = "none";

  public static final String SCHEMA_COMPATIBILITY_CONFIG = "schema.compatibility";
  public static final String SCHEMA_COMPATIBILITY_DEFAULT = "NONE";

  public static final String AZ_CONNECTION_TRY_TIMEOUT_MS_CONFIG = "azblob.connection.timeout.ms";
  public static final int AZ_CONNECTION_TRY_TIMEOUT_MS_DEFAULT =
      ((int) TimeUnit.SECONDS.toMillis(30));
  public static final String AZ_CONNECTION_TRY_TIMEOUT_MS_DOC = "Indicates the maximum time "
      + "allowed for any single try of an HTTP request. NOTE: When "
      + "transferring large amounts of data, the default value will probably not be "
      + "sufficient. You should override this value based on the bandwidth available to "
      + "the host machine and proximity to the Storage service. A good starting point "
      + "may be something like (60 seconds per MB of anticipated-payload-size).";
  public static final String AZ_CONNECTION_TRY_TIMEOUT_MS_DISPLAY = "Retry Try Timeout (ms)";

  public static final String AZ_RETRY_SECONDARY_HOST_CONFIG = "azblob.retry.secondary.host";
  public static final String AZ_RETRY_SECONDARY_HOST_DEFAULT = null;
  public static final String AZ_RETRY_SECONDARY_HOST_DOC = "If a secondary Host is specified, "
      + "retries will be tried against this host. NOTE: "
      + "Before setting this field, make sure you understand the issues around "
      + "reading stale and potentially-inconsistent data at https://docs.microsoft"
      + ".com/en-us/azure/storage/common/storage-designing-ha-apps-with-ragrs";
  public static final String AZ_RETRY_SECONDARY_HOST_DISPLAY = "Azure Retry Secondary Host";
  
  public static final String PARTITIONER_CLASS_CONFIG = "partitioner.class";
  public static final Class<DefaultPartitioner> PARTIONER_CLASS_DEFAULT = DefaultPartitioner.class;
  public static final String PARTITIONER_CLASS_DOC =
      "The partitioner to use when reading data to the store.";
  private static final String PARTITIONER_CLASS_DISPLAY = "Partitioner Class";

  public AzureBlobStorageSourceConnectorConfig(Map<String, String> originals) {
    super(config(), originals);
  }
  
  protected AzureBlobStorageSourceConnectorConfig(ConfigDef configDef, Map<String, String> props) {
    super(configDef, props);
  }

  @Override
  protected Long getPollInterval() {
    return getLong(AZ_POLL_INTERVAL_MS_CONFIG);
  }

  public static ConfigDef config() {
    ConfigDef configDef = CloudStorageSourceConnectorCommonConfig.config();

    int orderInGroup = 0;
    final String groupAzure = "Azure";
    final String groupPartitioner = "Partitioner";
    final String groupConnector = "Connector";

    configDef
        .define(
            AZ_POLL_INTERVAL_MS_CONFIG, 
            ConfigDef.Type.LONG, 
            AZ_POLL_INTERVAL_MS_DEFAULT,
            ConfigDef.Importance.MEDIUM, 
            AZ_POLL_INTERVAL_MS_DOC, 
            groupConnector,
            ++orderInGroup, 
            ConfigDef.Width.MEDIUM, 
            AZ_POLL_INTERVAL_MS_DISPLAY)
    
        .define(
            AZ_ACCOUNT_NAME_CONFIG, 
            Type.STRING, 
            ConfigDef.NO_DEFAULT_VALUE,
            new AzureNamingConventionValidator("Account Name", 3, 23), 
            Importance.HIGH,
            AZ_ACCOUNT_NAME_DOC, 
            groupAzure,
            ++ orderInGroup, 
            Width.LONG,
            AZ_ACCOUNT_NAME_DISPLAY)
    
        .define(
            AZ_ACCOUNT_KEY_CONFIG, 
            Type.PASSWORD, 
            "",
            new PasswordValidator("Account Key"), 
            Importance.HIGH, 
            AZ_ACCOUNT_KEY_DOC,
            groupAzure, 
            ++ orderInGroup, 
            Width.LONG, 
            AZ_ACCOUNT_KEY_DISPLAY)
        
        .define(
            AZ_ACCOUNT_SAS_TOKEN_CONFIG, 
            Type.STRING, 
            "",
            Importance.HIGH, 
            AZ_ACCOUNT_SAS_TOKEN_DOC,
            groupAzure, 
            ++ orderInGroup, 
            Width.LONG, 
            AZ_ACCOUNT_SAS_TOKEN_DISPLAY)
    
        .define(
            AZ_STORAGE_CONTAINER_NAME_CONFIG, 
            Type.STRING, 
            AZ_STORAGE_CONTAINER_NAME_DEFAULT,
            new AzureNamingConventionValidator("Container Name", 3, 63, '-'),
            Importance.MEDIUM,
            AZ_STORAGE_CONTAINER_NAME_DOC, 
            groupAzure,
            ++ orderInGroup, 
            Width.LONG,
            AZ_STORAGE_CONTAINER_NAME_DISPLAY)
        
        .define(
            AZ_PROXY_PASS_CONFIG, 
            Type.PASSWORD, 
            AZ_PROXY_PASS_DEFAULT, 
            Importance.LOW,
            AZ_PROXY_PASS_DOC, 
            groupAzure, 
            ++ orderInGroup,
            Width.LONG,
            AZ_PROXY_PASS_DISPLAY)
        
        .define(
            AZ_RETRY_RETRIES_CONFIG, 
            Type.INT,
            AZ_RETRY_RETRIES_DEFAULT, 
            atLeast(1),
            Importance.MEDIUM,
            AZ_RETRY_RETRIES_DOC, 
            groupAzure,
            ++ orderInGroup, 
            Width.LONG, 
            AZ_RETRY_RETRIES_DISPLAY)
        
        .define(
            AZ_RETRY_BACKOFF_MS_CONFIG, 
            Type.LONG, 
            AZ_RETRY_BACKOFF_MS_DEFAULT, 
            atLeast(0L),
            Importance.LOW,
            AZ_RETRY_BACKOFF_MS_DOC, 
            groupAzure, 
            ++ orderInGroup, 
            Width.SHORT,
            AZ_RETRY_BACKOFF_MS_DISPLAY)
        
        .define(
            AZ_RETRY_MAX_BACKOFF_MS_CONFIG,
            Type.LONG, 
            AZ_RETRY_MAX_BACKOFF_MS_DEFAULT,
            atLeast(1L),
            Importance.LOW,
            AZ_RETRY_MAX_BACKOFF_MS_DOC,
            groupAzure, 
            ++ orderInGroup, 
            Width.SHORT, 
            AZ_RETRY_MAX_BACKOFF_MS_DISPLAY)
        
        .define(
            AZ_RETRY_TYPE_CONFIG,
            Type.STRING,
            AZ_RETRY_TYPE_DEFAULT,
            anyOf(
                oneOfUppercase(RetryPolicyType.class),
                oneOfLowercase(RetryPolicyType.class),
                new NullValidator()),
            Importance.LOW,
            AZ_RETRY_TYPE_DOC, 
            groupAzure, 
            ++ orderInGroup,
            Width.LONG,
            AZ_RETRY_TYPE_DISPLAY)

        .define(
            AZ_RETRY_SECONDARY_HOST_CONFIG, 
            Type.STRING, 
            AZ_RETRY_SECONDARY_HOST_DEFAULT,
            anyOf(
                validUri("http", "https"), 
                emptyStringValidator()), 
            Importance.LOW,
            AZ_RETRY_SECONDARY_HOST_DOC, 
            groupAzure,
            ++ orderInGroup, 
            Width.LONG, 
            AZ_RETRY_SECONDARY_HOST_DISPLAY)
        
        .define(
            AZ_PROXY_URL_CONFIG,
            Type.STRING,
            AZ_PROXY_URL_DEFAULT,
            anyOf(
                validUri("http", "https"),
                emptyStringValidator()),
            Importance.LOW,
            AZ_PROXY_URL_DOC,
            groupAzure,
            ++ orderInGroup, 
            Width.LONG,
            AZ_PROXY_URL_DISPLAY)

        .define(
            AZ_PROXY_USER_CONFIG, 
            Type.STRING, 
            AZ_PROXY_USER_DEFAULT, 
            Importance.LOW,
            AZ_PROXY_USER_DOC,
            groupAzure, 
            ++ orderInGroup,
            Width.LONG, 
            AZ_PROXY_USER_DISPLAY)

        .define(
            AZ_CONNECTION_TRY_TIMEOUT_MS_CONFIG, 
            Type.LONG,
            AZ_CONNECTION_TRY_TIMEOUT_MS_DEFAULT, 
            between(1L, 1000L * Integer.MAX_VALUE),
            Importance.MEDIUM,
            AZ_CONNECTION_TRY_TIMEOUT_MS_DOC, 
            groupAzure,
            ++ orderInGroup, 
            Width.SHORT, 
            AZ_CONNECTION_TRY_TIMEOUT_MS_DISPLAY)
        
        .define(
            PARTITIONER_CLASS_CONFIG,
            ConfigDef.Type.CLASS,
            PARTIONER_CLASS_DEFAULT,
            ConfigDef.Importance.HIGH,
            PARTITIONER_CLASS_DOC,
            groupPartitioner,
            ++orderInGroup,
            ConfigDef.Width.LONG,
            PARTITIONER_CLASS_DISPLAY,
            Recommenders
                .anyOf(
                    DefaultPartitioner.class,
                    FieldPartitioner.class,
                    TimeBasedPartitioner.class,
                    DailyPartitioner.class,
                    HourlyPartitioner.class));

    return configDef;
  }

  public String getStorageDelimiter() {
    return getString(DIRECTORY_DELIM_CONFIG);
  }

  public String getTopicsFolder() {
    return getString(TOPICS_DIR_CONFIG) 
        + getString(DIRECTORY_DELIM_CONFIG);
  }

  public int getSchemaCacheSize() {
    return getInt(SCHEMA_CACHE_SIZE_CONFIG);
  }

  public Class<?> getAzureBlobObjectFormatClass() {
    return getClass(FORMAT_CLASS_CONFIG);
  }

  public int getRecordBatchMaxSize() {
    return getInt(RECORD_BATCH_MAX_SIZE_CONFIG);
  }

  public Partitioner getPartitioner(CloudSourceStorage azureBlobStorage) {
    try {
      @SuppressWarnings("unchecked")
      Class<Partitioner> partitionerClass = (Class<Partitioner>) this.getClass(
          PARTITIONER_CLASS_CONFIG);
      return partitionerClass.getConstructor(
          AzureBlobStorageSourceConnectorConfig.class, AzureBlobSourceStorage.class)
          .newInstance(this, azureBlobStorage);
    } catch (Exception e) {
      throw new ConnectException("Failed to instantiate Partitioner class ", e);
    }
  }
  
  public Password getAccountKey() {
    return getPassword(AZ_ACCOUNT_KEY_CONFIG);
  }

  public String getAccountName() {
    return getString(AZ_ACCOUNT_NAME_CONFIG);
  }
  
  public String getAccountSasToaken() {
    return getString(AZ_ACCOUNT_SAS_TOKEN_CONFIG);
  }

  public String getContainerName() {
    return getString(AZ_STORAGE_CONTAINER_NAME_CONFIG);
  }
  
  /**
   * Get the AzureBlobStorage account url
   * 
   * @return String having url of the AzureBlobStorage account
   */
  public String getUrl() {
    final String url = getString(CloudStorageSourceConnectorCommonConfig.STORE_URL_CONFIG);
    if (isNotBlank(url)) {
      return url;
    }
    return String.format(Locale.ROOT, "https://%s.blob.core.windows.net", getString(AZ_ACCOUNT_NAME_CONFIG));

  }
  
  /**
   * Get the RequestRetryOptions 
   * 
   * @return RequestRetryOptions object
   */
  public RequestRetryOptions getRequestRetryOptions() {
    return new RequestRetryOptions(RetryPolicyType.valueOf(getString(AZ_RETRY_TYPE_CONFIG)),
        getInt(AZ_RETRY_RETRIES_CONFIG) + 1,
        Math.toIntExact(getLong(AZ_CONNECTION_TRY_TIMEOUT_MS_CONFIG) / 1000),
        getLong(AZ_RETRY_BACKOFF_MS_CONFIG), getLong(AZ_RETRY_MAX_BACKOFF_MS_CONFIG),
        getString(AZ_RETRY_SECONDARY_HOST_CONFIG));
  }
  
  private static EmptyStringValidator emptyStringValidator() {
    return new EmptyStringValidator();
  }

  /**
   * EmptyStringValidator class for validating String
   */
  private static class EmptyStringValidator implements ConfigDef.Validator {
    @Override
    public void ensureValid(String name, Object value) {
      if (isNotBlank((String) value)) {
        throw new ConfigException(name + " must be blank");
      }
    }

    public String toString() {
      return "should be and empty string";
    }
  }
  
  /**
   * AzureNamingConventionValidator class for validating the azure blob account name 
   * and Container Name values
   */
  private static class AzureNamingConventionValidator implements ConfigDef.Validator {

    private final String propertyName;
    private final int min;
    private final int max;
    private char[] allowed;

    public AzureNamingConventionValidator(String propertyName, int min, int max, char... allowed) {
      this.propertyName = propertyName;
      this.min = min;
      this.max = max;
      this.allowed = allowed;
    }

    @Override
    public void ensureValid(String name, Object value) {
      if (value == null) {
        throw new ConfigException(name, value, propertyName + " must be non-null");
      }

      String property = (String) value;
      if (property.length() < min) {
        throw new ConfigException(property, value,
            propertyName + " must be at least: " + min + " characters");
      }

      if (property.length() > max) {
        throw new ConfigException(property, value,
            propertyName + " must be no more: " + max + " characters");
      }

      String filteredProperty = allowed.length == 0 ? property
          : property.replaceAll("[" + String.valueOf(allowed) + "]", "");
      if (!Strings.isAlphanumeric(filteredProperty)) {
        throw new ConfigException(property, value,
            propertyName + " must be only alphanumeric and [" + String.valueOf(allowed)
                + "] characters");
      }

      if (!Strings.isAllLowerCase(filteredProperty.replaceAll("\\d", ""))) {
        throw new ConfigException(property, value,
            propertyName + " must be only lowercase characters");
      }
    }

    public String toString() {
      return "[" + min + ",...," + max + "]";
    }
  }
  
  /**
   * PasswordValidator class for validating property value
   */
  private static class PasswordValidator implements ConfigDef.Validator {

    private final String propertyName;

    public PasswordValidator(String propertyName) {
      this.propertyName = propertyName;
    }

    @Override
    public void ensureValid(String name, Object value) {
      Password password = (Password) value;

      if (password == null) {
        throw new ConfigException(propertyName + " must be non-null and non-blank");
      }
    }

    public String toString() {
      return "password must be non-blank";
    }

  }
  
  /**
   * NullValidator class for validating object
   */
  public static class NullValidator implements ConfigDef.Validator {

    @Override
    public void ensureValid(String name, Object object) {
      if (object != null) {
        throw new ConfigException(name, object, "Should be null");
      }
    }

    @Override
    public String toString() {
      return "[null]";
    }

  }

  public static void main(String[] args) {
    System.out.println(config().toEnrichedRst());
  }
  
}