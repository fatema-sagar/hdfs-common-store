/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.cloud.storage.source;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.cloud.storage.source.util.FileOffset;
import io.confluent.connect.cloud.storage.source.util.SourceRecordOffset;
import io.confluent.connect.cloud.storage.source.util.StorageObjectSourceReader;
import io.confluent.connect.utils.Version;

import static io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig.FOLDERS_CONFIG;
import static io.confluent.connect.cloud.storage.source.util.FileOffset.emptyOffset;
import static java.util.Objects.isNull;

/**
 * Abstract Cloud Storage source connector class.
 * Source task class of cloud storage source connector like S3, GCS, Azure blob has 
 * to extend this class.
 */
public abstract class AbstractCloudStorageSourceTask extends SourceTask {
  
  private static final Logger log = LoggerFactory.getLogger(AbstractCloudStorageSourceTask.class);
  
  private static final String FILE_OFFSET = "fileOffset";
  private static final String EOF = "eof";
  private static final String LAST_FILE_READ = "lastFileRead";

  // Visible for testing
  protected CloudStorageSourceConnectorCommonConfig config;
  protected CloudSourceStorage storage;
  protected Partitioner partitioner;
  protected StorageObjectSourceReader sourceReader;
  protected int maxBatchSize = 1;
  protected List<String> folders;
  
  private String extension;
  private int activeFolderIndex = 0;
  private Map<String, FileOffset> fileOffset = new HashMap<>();
  private StorageObject currentObject;

  public AbstractCloudStorageSourceTask() {}
  
  protected abstract CloudStorageSourceConnectorCommonConfig createConfig(Map<String, 
      String> props);
  
  protected abstract CloudSourceStorage createStorage();
  
  protected abstract Partitioner getPartitioner(CloudSourceStorage storage);
  
  protected CloudStorageSourceConnectorCommonConfig config() {
    return config;
  }
  
  //Visible for testing
  protected AbstractCloudStorageSourceTask(
      Map<String, String> props, 
      CloudSourceStorage storage, 
      StorageObjectSourceReader reader) {
    this.config = createConfig(props);
    folders = config.getList(FOLDERS_CONFIG);
    this.storage = storage;
    extension = config.getInstantiatedStorageObjectFormat().getExtension();
    sourceReader = reader;
    partitioner = getPartitioner(storage);
    maxBatchSize = config.getRecordBatchMaxSize();
  }

  @Override
  public void start(Map<String, String> props) {
    this.config = createConfig(props);
    folders = config.getList(FOLDERS_CONFIG);
    this.storage = createStorage();
    sourceReader = new StorageObjectSourceReader(config);
    this.partitioner = getPartitioner(storage);
    this.maxBatchSize = config.getRecordBatchMaxSize();
    doStart(context);

    log.info("Started source connector task with assigned folders {} using partitioner {}",
        folders, partitioner.getClass().getName()
    );
  }

  // visible for testing
  public void doStart(SourceTaskContext context) {
    if (!storage.bucketExists()) {
      throw new ConnectException("No bucket Exists");
    }

    folders.forEach(folder -> {
      final OffsetStorageReader offsetStorageReader = context.offsetStorageReader();
      final Map<String, Object> offset =
          offsetStorageReader.offset(StorageObjectSourceReader.buildSourcePartition(folder));

      if (!isNull(offset)) {
        Object maybeFileName = offset.get(LAST_FILE_READ);

        if (!isNull(maybeFileName)) {
          String filename = String.valueOf(maybeFileName);
          long objOffset = Long.parseLong(String.valueOf(offset.get(FILE_OFFSET)));
          boolean eof = Boolean.parseBoolean(String.valueOf(offset.get(EOF)));

          log.debug("Restoring offset for file {} with offset {}", filename, objOffset);
          fileOffset.put(folder, new FileOffset(filename, objOffset, eof));
        }
      }
    });
  }
 
  @Override
  public String version() {
    return Version.forClass(this.getClass());
  }

  @Override
  public List<SourceRecord> poll() {

    //TODO: this should be configurable
    final long waitTimeAfterScan = TimeUnit.SECONDS.toMillis(5);

    log.trace("Beginning poll for new objects.");
    String folder = folders.get(activeFolderIndex);

    // If no active cloud objects were found in the active folder, check the remaining folders
    // for objects with records.
    if (currentObject == null) {
      FileOffset checkFileOffset;
      Optional<StorageObject> candidateObject;

      int foldersInspected = 0;
      do {
        activeFolderIndex = (activeFolderIndex + foldersInspected) % folders.size();
        folder = folders.get(activeFolderIndex);
        checkFileOffset = fileOffset.getOrDefault(folder, emptyOffset());
        log.trace("Checking for files in folder {} with offset {}", folder, checkFileOffset);
        candidateObject = moveToNextObjectToProcess(folder, checkFileOffset);
        foldersInspected++;
      } while (!candidateObject.isPresent() && foldersInspected < folders.size());

      if (candidateObject.isPresent()) {
        currentObject = candidateObject.get();
        log.debug("Will process file {} from offset {}", currentObject.getKey(),
            checkFileOffset.offset()
        );
      } else {
        log.info("No new files ready after scan task assigned folders");
        try {
          Thread.sleep(waitTimeAfterScan);
        } catch (InterruptedException e) {
          // no action
        }
        return null;
      }
    }

    long offset = fileOffset.get(folder).offset();
    boolean eof = false;
    final String currentFileName = currentObject.getKey();
    log.debug("Begin extraction from {}", currentFileName);

    final List<SourceRecord> records = new ArrayList<>();
    for (int i = 0; i < maxBatchSize; i++) {
      final SourceRecordOffset sourceRecordOffset =
          sourceReader.nextRecord(currentObject, folder, offset);
      offset = sourceRecordOffset.getOffset();

      log.trace("Extracted record from {} and updated offset to {}",
          currentFileName, offset
      );
      records.add(sourceRecordOffset.getRecord());

      // If file has now been fully processed we must reset the current object and rotate the
      // active folder before the next poll
      if (sourceRecordOffset.isEof()) {
        log.debug("Finished processing file {}", currentFileName);
        currentObject.close();
        currentObject = null;
        eof = true;
        activeFolderIndex = ++activeFolderIndex % folders.size();
        break;
      }

    }

    //replace is used as fileOffset is initialized by moveToNextObjectToProcess for this folder.
    fileOffset.replace(folder, new FileOffset(currentFileName, offset, eof));
    log.debug("Extracted {} records", records.size());
    return records;
  }

  private Optional<StorageObject> moveToNextObjectToProcess(
      String folder,
      FileOffset currentFileOffset
  ) {

    if (currentFileOffset.filename() != null && !currentFileOffset.eof()) {
      // Have not finished processing file. Must retrieve and then skip to saved offset
      log.info("Continue processing file after restart {}", currentFileOffset.filename());

      final StorageObject storageObject = storage.getStorageObject(currentFileOffset.filename());
      sourceReader.skip(storageObject, currentFileOffset.offset());
      fileOffset.put(folder, new FileOffset(currentFileOffset));
      return Optional.of(storageObject);
    } else {
      // Either no previous file or previous file is finished. Must retrieve next file
      final String nextobjectName =
          partitioner.getNextObjectName(folder, currentFileOffset.filename());

      if (nextobjectName.isEmpty()) {
        return Optional.empty();
      }

      final String key = nextobjectName ;

      fileOffset.put(folder, new FileOffset(key, 0L, false));
      log.debug("Obtained new file to process  {}", key);
      return Optional.of(storage.open(key));
    }
  }

  @Override
  public void stop() {
    log.info("Stopping source connector task with assigned folders {}", folders);
  }

}
