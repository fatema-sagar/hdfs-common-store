/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.azure.blob.storage;

import static io.confluent.connect.storage.common.util.StringUtils.isNotBlank;
import static io.confluent.license.util.StringUtils.isBlank;

import java.net.MalformedURLException;
import java.security.InvalidKeyException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.ContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.models.StorageException;
import com.azure.storage.common.credentials.SASTokenCredential;
import com.azure.storage.common.credentials.SharedKeyCredential;

import io.confluent.connect.cloud.storage.source.CloudSourceStorage;
import io.confluent.connect.cloud.storage.source.StorageObject;

/**
 * It implements CloudSourceStorage interface.
 * This has implementation of methods needed to interact with 
 * AzureBlobStorage container and object.
 */
public class AzureBlobSourceStorage implements CloudSourceStorage {
  
  private static final Logger log = LoggerFactory.getLogger(AzureBlobSourceStorage.class);

  private final AzureBlobStorageSourceConnectorConfig conf;
  private final ContainerClient containerClient;
  private final AzureBlobSourceStorageHelper storageHelper;

  /**
   * Construct an AzureBlobStorage class given a configuration and an Azure Storage
   * account + container.
   *
   * @param conf the AzureBlobStorage configuration.
   */
  public AzureBlobSourceStorage(AzureBlobStorageSourceConnectorConfig conf)
      throws StorageException, InvalidKeyException, MalformedURLException {
    this.conf = conf;

    /*
     * Create a BlobServiceClient object
     * 
     */
    BlobServiceClient storageClient = null;
    
    if (conf.getAccountSasToaken() != null && !"".equals(conf.getAccountSasToaken())) {
      /*
       * The Storage account's shared access signature (SAS) from configuration
       * create a credential object. This is used to access your account.
       */
      final SASTokenCredential credential = SASTokenCredential
          .fromSASTokenString(conf.getAccountSasToaken());
      /*
       * BlobServiceClient object wraps the service endpoint, 
       * saskeycredential and a request pipeline.
       */
      storageClient = new BlobServiceClientBuilder()
          .endpoint(conf.getUrl()).credential(credential).buildClient();
    } else {
      /*
       * The account's name and key from configuration to create a 
       * credential object. This is used to access your account.
       */
      final SharedKeyCredential credential = new SharedKeyCredential(conf.getAccountName(),
          conf.getAccountKey().value());
      /*
       * BlobServiceClient object wraps the service endpoint, 
       * sharedkeycredential and a request pipeline.
       */
      storageClient = new BlobServiceClientBuilder()
          .endpoint(conf.getUrl()).credential(credential).buildClient();
    }

    /*
     * Create a client that references the given container in your Azure Storage account. 
     * This returns a ContainerClient object that wraps the container's endpoint,
     * credential and a request pipeline (inherited from storageClient).
     * Note that container names require lower case.
     */
    containerClient = storageClient
        .getContainerClient(conf.getContainerName());
    
    this.storageHelper = new AzureBlobSourceStorageHelper(containerClient);

  }

  // Visible For Testing
  public AzureBlobSourceStorage(
      AzureBlobStorageSourceConnectorConfig conf, ContainerClient containerClient) {
    this.conf = conf;
    this.containerClient = containerClient;
    this.storageHelper = new AzureBlobSourceStorageHelper(containerClient);
  }
  
  // Visible For Testing
  public AzureBlobSourceStorage(
      AzureBlobStorageSourceConnectorConfig conf, 
      ContainerClient containerClient, 
      AzureBlobSourceStorageHelper storageHelper) {
   
    this.conf = conf;
    this.containerClient = containerClient;
    this.storageHelper = storageHelper;
  }
  
  /**
   * Getting the container URL
   *
   * @return ContainerURL Object
   */
  public ContainerClient getAzureContinerClient() {
    return containerClient;
  }
  
  /**
   * Overriding the parent method for creating the StorageObject From given key
   *
   * @param key the name of the blob item from which the storage object need to be created
   * @return StorageObject created from blob item
   */
  @Override
  public StorageObject getStorageObject(String key) {
    log.trace("Getting StorageObject from blob item with"
        + " name {} ", key);
    Iterator<BlobItem> blobItems = containerClient.listBlobsFlat().iterator();
    while (blobItems.hasNext()) {
      BlobItem blobItem = blobItems.next();
      if (blobItem.name().equalsIgnoreCase(key)) {
        try {
          return new AzureBlobStorageObject(blobItem, 
              new AzureBlobSourceStorage(conf));
        } catch (Exception e) {
          throw new ConnectException(
              MessageFormat.format("Error getting storage object for {}", key), e);
        }
      }
    }
    log.debug("No blob found for the specified key{}", key);
    return null;
  }

  /**
   * As of now the implementation of this method is not added.
   * Azure Blob Storage source connect is not using it anywhere.
   * In future, if this method is required then implementation can be added here.
   */
  @Override
  public List<StorageObject> getListOfStorageObjects(String path) {
    throw new UnsupportedOperationException(
        "Implementation not added in Azure Blob Storage class.");
  }

  /**
   * Overriding the parent method for checking if the given blob name exists or not 
   *
   * @param name of the blob item to check
   * @return boolean status if blob exists or not
   */
  @Override
  public boolean exists(String name) {
    try {
      return isNotBlank(name) && containerClient.getBlobClient(name).exists();
    } catch (Exception e) {
      throw new ConnectException(
          MessageFormat.format("Error checking existance of {}", name),e);
    }
  }

  /**
   * This is currently not supported as we don't need delete operation for 
   * the azure blob Storage connector but as we are overriding the 
   * CloudSourceStorage class so have to implement the method
   *
   * @return String the container url in string format
   */
  @Override
  public void delete(String name) {
    throw new UnsupportedOperationException(
        "This is not currently supported in Azure Blob Storage class");
  }

  /**
   * Get the container URL in String format
   *
   * @return String the container url in string format
   */
  public String url() {
    return containerClient.getContainerUrl().toString();
  }

  /**
   * Get the Connector configuration
   *
   * @return AzureBlobStorageSourceConnectorConfig containing connector related configurations
   */
  public AzureBlobStorageSourceConnectorConfig getConfiguration() {
    return conf;
  }
  
  /**
   * Overriding the parent method for checking if container exists or not in AzureBlobStorage
   *
   * @return boolean status if container exists or not
   */
  @Override
  public boolean bucketExists() {
    return containerClient.exists();
  }

  /**
   * Returns partitions(folders in Azure Blob Container) upto level-2 depth 
   * for the given path and delimiter for path.
   * 
   * <p>For example, a blob container having folder such as /kafka-topic/partition=0/ 
   * and /kafka-topic/partition=1/, the method should return the path of the partitions,
   * "/kafka-topic/partition=0/" and "/kafka-topic/partition=1/" from which blobs will be read.
   * 
   * <p>Here we are considering only directories which we can get to know by the blob property 
   * "isPrefix=true". 
   * 
   * @param path under which folders needs to list
   * @param delimiter specifying the boundary between separate
   * @return the distinct folder names in a Set
   */
  public Set<String> listFolders(String path, String delimiter) {
    log.trace("Listing all folders on azure Blob Storage with"
        + " path {} and delimiter {}", path, delimiter);

    Set<String> partitions = new HashSet<>();
    try {

      Iterator<BlobItem> blobItems = storageHelper.getBlobList(delimiter, path);
      while (blobItems.hasNext()) {
        BlobItem blobItem = blobItems.next();
        if (blobItem.isPrefix()) {
          Iterator<BlobItem> blobItemsPrefix = storageHelper.getBlobList(delimiter, blobItem);
          while (blobItemsPrefix.hasNext()) {
            BlobItem itempre = blobItemsPrefix.next();
            if (itempre.isPrefix()) {
              partitions.add(itempre.name()); 
            }
          }
        }
      }
    } catch (Exception e) {
      throw new ConnectException("Error while getting list of folders from "
          + "Azure Blob Storage Container", e);
    }
    return partitions;
  }

  /**
   * List the folders present just under the given path which are separated by a delimiter
   *
   * @param path under which folders needs to list
   * @param delimiter specifying the boundary between separate
   * @return folders name in a list
   */
  public List<String> listBaseFolders(String path, String delimiter) {
    log.trace("Listing folders on azure Blob Storage with"
        + " path {} and delimiter {}", path, delimiter);
    
    List<String> partitions = new ArrayList<String>();
    try {
      
      Iterator<BlobItem> blobItems = storageHelper.getBlobList(delimiter, path);
      
      while (blobItems.hasNext()) {
        BlobItem blobItem = blobItems.next();
        if (blobItem.isPrefix()) {
          partitions.add(blobItem.name());
        }
      }
    } catch (Exception e) {
      throw new ConnectException("Error while getting Base folders from "
          + "Azure Blob Storage Container", e);
    }
    return partitions;
  }

  /**
   * Get the file present under the given path after the given file name having
   * file type as given fileSuffix
   *
   * @param path under which folders needs to list
   * @param previousObject the previous object after which the file need to return
   * @param fileSuffix the extension specifying the type of file need to search
   * @return status id container created or not
   */
  public String getNextFileName(
      String path, String previousObject, String fileSuffix) {
    log.debug("Listing objects on azure with path "
        + "{} starting after file {}", path, previousObject);
    
    ListBlobsOptions options = new ListBlobsOptions().prefix(path);

    Iterator<BlobItem> blobItems = storageHelper.getFlatBlobList(options);
    boolean returnNextBlogflag = isBlank(previousObject) ? true : false;

    while (blobItems.hasNext()) {
      BlobItem blob = blobItems.next();
      if (blob.name().endsWith(fileSuffix)) {
        if (returnNextBlogflag) {
          return blob.name();
        }
        if (blob.name().equals(previousObject)) {
          returnNextBlogflag = true;
        } else {
          continue;
        }
      }
    }
    return "";
  }

  /**
   * Get the BlobItem based on the given key
   *
   * @param key the name of the bolbItem need to return
   * @return BlobItem for the given key
   */
  public BlobItem getFile(String key) {
    Iterator<BlobItem> blobItems = containerClient.listBlobsFlat().iterator();
    while (blobItems.hasNext()) {
      BlobItem blobItem = blobItems.next();
      if (blobItem.name().equalsIgnoreCase(key)) {
        return blobItem;
      }
    }
    return null;
  }
  
  /**
   * Get the List of Blobs under given path using streams
   * Here we are using containerClient.listBlobsFlat() which provide streaming 
   * capability and also it is lazy loaded(as referred from azure documentation) 
   * instead of marker as in latest version(12.0 preview) it is not present.
   *
   * @param path under which blob list need to find
   * 
   * @return List of blob item from the container extracted
   */
  public List<BlobItem> getBlobsListStreamResponse(String path) {
    
    return storageHelper.getListOfBlobResponse(path);
  }

  /**
   * Get the Storage Object from the given blobItem name as key
   *
   * @param key the name of the bolbItem from which the Storage object will be created 
   * @return StorageObject created from blobItem name
   */
  @Override
  public StorageObject open(String key) {
    return getStorageObject(key);
  }

}