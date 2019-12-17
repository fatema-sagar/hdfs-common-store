/*
 * Copyright [2019 - 2019] Confluent Inc.
 */


package io.confluent.connect.hdfs.source;

import io.confluent.connect.cloud.storage.source.StorageObject;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class HDStorageObject implements StorageObject {

    protected FileSystem fileSystem;
    protected String key;
    private static final Logger log = LoggerFactory.getLogger(HDStorageObject.class);
    private static final long LENGTH_ZERO = 0L;
    private HDSourceConnectorConfig config;

    public HDStorageObject(FileSystem fileSystem, String key) {
        this.fileSystem = fileSystem;
        this.key = key;
    }

    @Override
    public String getKey() {
        try {
            if (fileSystem.exists(new Path(key)))
            return key;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }

    @Override
    public InputStream getObjectContent() {
        try {
            if (fileSystem.exists(new Path(key))) {
                FSDataInputStream inputStream = fileSystem.open(new Path(key));
                return inputStream;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public long getContentLength() {
        try {
            if (fileSystem.exists(new Path(key))) {
                return fileSystem.getLength(new Path(key));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public Object getObjectMetadata() {
        try {
            if (fileSystem.exists(new Path(key))) {
                return fileSystem.getLength(new Path(key));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() {
        try {
            if (fileSystem != null) {
                fileSystem.close();
            }
        } catch (IOException io) {
            log.error("Error closing the filesystem {}: "+key);
        }
    }
}
