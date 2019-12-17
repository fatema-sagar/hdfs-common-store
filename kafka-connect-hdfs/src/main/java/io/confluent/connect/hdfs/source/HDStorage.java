/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.hdfs.source;

import io.confluent.connect.cloud.storage.source.CloudSourceStorage;
import io.confluent.connect.cloud.storage.source.StorageObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * This has implementation of methods needed to interact with
 * Hadoop File System.
 */
public class HDStorage implements CloudSourceStorage {

    private static final Logger log = LoggerFactory.getLogger(HDStorage.class);

    private static final String VERSION_FORMAT = "APN/1.0 Confluent/1.0 KafkaHDFSConnector/%s";
    private final String url;
    private String key;
    private final HDSourceConnectorConfig config;
    private FileSystem fileSystem;
    private Set<String> topicPartiton = new HashSet<>();
    /**
     *
     * @param url HDFS address
     * @param config HDFS Configuration
     */
    public HDStorage(HDSourceConnectorConfig config, String url) {
        this.url = url;
        this.config = config;
    }

    public String getUrl() { return url; }

    public HDSourceConnectorConfig getConfig() { return config; }

    /**
     * @throws IOException
     */
    public Set<String> readFiles() throws IOException {
        FileSystem fs = FileSystem.get(URI.create(config.getHdfsUrl()), new Configuration());
        FileStatus[] fileStatus = fs.listStatus(new Path(config.getHdfsUrl()));
        for(FileStatus status : fileStatus){
            if (status.getPath().toString().contains(config.getTopicsFolder())) {
                readFilesFromFolder(status.getPath().toString());
                break;
            }
        }
        return topicPartiton;
    }

    private void readFilesFromFolder(String path) throws IOException {
        FileSystem fileSystem = FileSystem.get(URI.create(path), new Configuration());
        FileStatus[] status = fileSystem.listStatus(new Path(path));
        for (FileStatus fileStatus : status) {
            if ( !fileStatus.getPath().toString().contains("/+tmp")) {
                countPartitions(fileStatus);
            }
        }
    }

    private void countPartitions(FileStatus fileStatus) throws IOException {
        if (fileStatus.isDirectory()) {
            FileSystem fs = FileSystem.get(URI.create(fileStatus.getPath().toString()), new Configuration());
            FileStatus[] fst = fs.listStatus(new Path(fileStatus.getPath().toString()));
            for (FileStatus status : fst) {
                topicPartiton.add(status.getPath().toString());
            }
        }
    }

    @Override
    public StorageObject getStorageObject(String key) {
        this.key = key;
        return new HDStorageObject(fileSystem, key);
    }

    @Override
    public List<StorageObject> getListOfStorageObjects(String path) {
        throw new UnsupportedOperationException(
                "Implementation not added in HDFS Storage class."
        );
    }

    @Override
    public boolean exists(String name) {
        try {
            if (fileSystem.exists(new Path(key))) {
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean bucketExists() {
        try {
            if (fileSystem.exists(new Path(key))) {
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public void delete(String name) {
        if (fileSystem.equals(name)) {
            return;
        } else {
            try {
                fileSystem.deleteSnapshot(new Path(key), name);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public StorageObject open(String path) {
        log.trace("Opening file stream to filesystem {} and path {}");
        return new HDStorageObject(fileSystem, key);
    }

    public void close(){
        try {
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getNextFileName(String path, String startAfterThisFile, String fileSuffix) {
        log.trace("Listing objects on hdfs with path {} starting after file {} and having suffix {}",
                path, startAfterThisFile, fileSuffix
        );
        String filename = "";
        Iterator itr = topicPartiton.iterator();

        while (itr.hasNext()){
            if(itr.equals(startAfterThisFile)) {
                if (itr.next().toString().contains(fileSuffix)) {
                    filename = itr.next().toString();
                }
            }
        }
        return filename;
    }
}
