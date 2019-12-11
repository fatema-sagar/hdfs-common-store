/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.gcs.source;

import static io.confluent.license.util.StringUtils.isBlank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.api.gax.paging.Page;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.auth.oauth2.GoogleCredentials;

import io.confluent.connect.cloud.storage.source.CloudSourceStorage;
import io.confluent.connect.cloud.storage.source.StorageObject;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

/**
 * It implements CloudSourceStorage interface.
 * This has implementation of methods needed to interact with 
 * Gcs bucket and Blob object.
 */
public class GcsSourceStorage implements CloudSourceStorage {
  
  private static final Logger log = LoggerFactory.getLogger(GcsSourceStorage.class);
  
  private final String url;
  private final String bucketName;
  private final Storage gcs;

  /**
   * Construct an Gcs storage class given a configuration and Gcs address.
   *
   * @param conf the Gcs configuration.
   * @param url the Gcs address.
   */
  public GcsSourceStorage(GcsSourceConnectorConfig conf, String url) throws IOException {
    this.url = url;
    this.bucketName = conf.getBucketName();
    this.gcs = newGcsClient(conf);
  }
  
  //Visible for testing.
  GcsSourceStorage(String url, String bucketName, Storage gcs) {
    this.url = url;
    this.bucketName = bucketName;
    this.gcs = gcs;
  }

  @Override
  public StorageObject getStorageObject(String key) {
    return new GcsStorageObject(gcs.get(bucketName, key));
  }

  /**
   * As of now the implementation of this method is not added.
   * Gcs source connect is not using it anywhere.
   * In future, if this method is required then implementation can be added here.
   */
  @Override
  public List<StorageObject> getListOfStorageObjects(String path) {
    throw new UnsupportedOperationException(
        "Implementation not added in Gcs Storage class.");
  }

  @Override
  public boolean exists(String blobName) {
    try {
      return !isBlank(blobName)
          && gcs.get(bucketName, blobName, Storage.BlobGetOption.fields()) != null;
    } catch (StorageException ex) {
      throw new ConnectException("Error while checking if GCS blob exists "
          + "for blob name : " + blobName, ex);
    }
  }

  @Override
  public boolean bucketExists() {
    try {
      return !isBlank(bucketName)
          && gcs.get(bucketName, Storage.BucketGetOption.fields()) != null;
    } catch (StorageException ex) {
      throw new ConnectException("Error while checking if GCS bucket exists "
          + "for bucket name : " + bucketName, ex);
    }
  }

  @Override
  public void delete(String name) {
    if (bucketName.equals(name)) {
      return;
    } else {
      gcs.delete(bucketName, name);
    }
  }

  @Override
  public StorageObject open(String path) {
    return new GcsStorageObject(getFile(path));
  }

  private RetrySettings getRetrySettings(GcsSourceConnectorConfig config) {
    int maxRetryAttempts = config.getInt(GcsSourceConnectorConfig.GCS_PART_RETRIES_CONFIG);
    long maxRetryBackoff = GcsSourceConnectorConfig.GCS_RETRY_MAX_BACKOFF_TIME_MS;
    long initialRetryBackoff = config.getLong(GcsSourceConnectorConfig.GCS_RETRY_BACKOFF_CONFIG);
    return RetrySettings.newBuilder()
        .setTotalTimeout(Duration.ofMillis(maxRetryBackoff * maxRetryAttempts))
        .setInitialRetryDelay(Duration.ofMillis(initialRetryBackoff))
        .setMaxRetryDelay(Duration.ofMillis(maxRetryBackoff))
        .setMaxAttempts(maxRetryAttempts)
        .build();
  }

  private Storage newGcsClient(GcsSourceConnectorConfig config) {
    StorageOptions.Builder builder = StorageOptions.newBuilder();
    builder.setRetrySettings(getRetrySettings(config));
    try {
      String pathToCredentials = config.credentialsPath();
      log.info("Starting new GCS Client with credentials path: {}", pathToCredentials);
      GoogleCredentials credentials = config.getCredentials();
      if (credentials != null) {
        builder.setCredentials(credentials);
      }
    } catch (Exception e) {
      throw new ConnectException("Failed to get valid GoogleCredentials.", e);
    }
    return builder.build().getService();
  }

  /**
   * get Blob Object for given key from GCS bucket.
   * @param key : filePath of GCS object.
   * @return : Blob object (GCS object)
   */
  public Blob getFile(String key) {
    Page<Blob> blobs = list(key);
    for (Blob blob : blobs.getValues()) {
      return blob;
    }
    return null;
  }

  /**
   * get list of GCS blobs from a given path (key)
   * @param key : key or path of the blob object.
   * @return : lists of blobs.
   */
  public List<String> getListOfBlobs(String key, String delimiter) {
    Iterator<Blob> iter = list(key).iterateAll().iterator();
    List<String> blobList = new ArrayList<String>();
    while (iter.hasNext()) {
      String path = iter.next().getName();
      if (path.endsWith(delimiter)) {
        blobList.add(path);
      }
    }
    return blobList;

  }
  
  public Page<Blob> list(String keyPath) {
    try {
      return gcs.list(
          bucketName,
          Storage.BlobListOption.currentDirectory(),
          Storage.BlobListOption.prefix(keyPath)
      );
    } catch (StorageException ex) {
      throw new ConnectException("Error while fetching Blob object from "
          + "GCS bucket for path : " + keyPath, ex);
    }
  }

  public Page<Blob> list(String path, String nextPageToken) {
    try {
      return gcs.list(
          bucketName,
          Storage.BlobListOption.prefix(path),
          Storage.BlobListOption.pageToken(nextPageToken)
      );
    } catch (StorageException ex) {
      throw new ConnectException("Error while fetching Blob object from "
          + "GCS bucket for path : " + path + " and page token : " + nextPageToken , ex);
    }
  }

  public Blob getNextObject(String folder, String previousObject, String extension) {
    log.trace("Get next object from topic {} using previous object {} and extension {}", folder,
        previousObject, extension
    );
    
    boolean returnNextBlogflag = isBlank(previousObject) ? true : false;

    Page<Blob> data = list(folder);

    for (Blob blob : data.getValues()) {
      if (returnNextBlogflag) {
        return blob;
      }

      if (!blob.getName().equals(previousObject)) {
        continue;
      } else {
        returnNextBlogflag = true;
      }
    }

    return null;
  }
  
  public String url() {
    return url;
  }
  
}
