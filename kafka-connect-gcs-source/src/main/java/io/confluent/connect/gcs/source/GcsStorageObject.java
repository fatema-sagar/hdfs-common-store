/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.gcs.source;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;

import com.google.cloud.storage.Blob;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.cloud.storage.source.StorageObject;

/**
 * It implements StorageObject interface.
 * This has implementation of methods needed to interact with a
 * GCS Blob object.
 */
public class GcsStorageObject implements StorageObject {

  private static final Logger log = LoggerFactory.getLogger(GcsStorageObject.class);
  private static final long LENGTH_ZERO = 0L;

  private final Blob delegate;
  private InputStream stream;

  public GcsStorageObject(Blob gcsObject) {
    this.delegate = gcsObject;

    /**
     * Create a stream of the object reader for processing.
     */
    try {
      this.stream = Channels.newInputStream(gcsObject.reader());
    } catch (Exception e) {
      throw new ConnectException("Error configuring the Gcs Object Wrapper.", e);
    }
  }
  
  @Override
  public String getKey() {
    return !emptyGcsBlobObject() ? delegate.getName() : null;
  }

  @Override
  public InputStream getObjectContent() {
    // Return the initialized stream.
    return this.stream;
  }

  @Override
  public long getContentLength() {
    return !emptyGcsBlobObject() ? delegate.getSize() : 0;
  }

  @Override
  public Object getObjectMetadata() {
    return !emptyGcsBlobObject() ? delegate.getSize() : 0;
  }

  @Override
  public void close() {
    try {
      if (stream != null) {
        stream.close();
      }
    } catch (IOException e) {
      log.error("Error closing the input stream for blob object {} from bucket {}:", 
          delegate.getName(), delegate.getBucket(), e);
      // Do nothing.
    }
  }

  private boolean emptyGcsBlobObject() {
    return delegate == null || delegate.getSize() == LENGTH_ZERO;
  }

}
