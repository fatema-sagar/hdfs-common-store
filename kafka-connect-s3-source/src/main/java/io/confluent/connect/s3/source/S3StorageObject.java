/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.s3.source;

import static java.util.Objects.isNull;

import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.S3Object;

import io.confluent.connect.cloud.storage.source.StorageObject;

/**
 * Wrapper class for S3 object.
 */
public class S3StorageObject implements StorageObject {
  
  private static final Logger log = LoggerFactory.getLogger(S3StorageObject.class);
  
  private static final long LENGTH_ZERO = 0L;

  private S3Object delegate;
  
  public S3StorageObject(S3Object s3Object) {
    this.delegate = s3Object;
  }
  
  @Override
  public String getKey() {
    if (!emptyS3Object()) {
      return delegate.getKey();
    }
    return "";
  }

  @Override
  public InputStream getObjectContent() {
    if (!emptyS3Object()) {
      return delegate.getObjectContent();
    }
    return null;
  }

  @Override
  public long getContentLength() {
    if (!emptyS3Object()) {
      return delegate.getObjectMetadata().getContentLength();
    }
    return 0;
  }

  @Override
  public Object getObjectMetadata() {
    if (!emptyS3Object()) {
      return delegate.getObjectMetadata().getContentLength();
    }
    return null;
  }
  
  @Override
  public void close() {
    try {
      if (delegate != null) {
        delegate.close();
      }
    } catch (Exception e) {
      log.error("Error closing the object {} from bucket {}:", 
          delegate.getKey(), delegate.getBucketName(), e);
      // Do nothing.
    }
  }
  
  private boolean emptyS3Object() {
    return delegate == null || isNull(delegate.getObjectMetadata())
        || delegate.getObjectMetadata().getContentLength() == LENGTH_ZERO;
  }

}
