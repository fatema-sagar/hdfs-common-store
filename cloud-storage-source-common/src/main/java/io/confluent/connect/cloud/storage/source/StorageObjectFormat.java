/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.cloud.storage.source;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.cloud.storage.source.util.Segment;
import io.confluent.connect.cloud.storage.source.util.SourceRecordOffset;
import io.confluent.connect.cloud.storage.source.util.StorageObjectKeyParts;
import io.confluent.connect.cloud.storage.source.util.ValueOffset;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;

import static io.confluent.connect.cloud.storage.source.util.StorageObjectSourceReader.buildSourceOffset;
import static io.confluent.connect.cloud.storage.source.util.StorageObjectSourceReader.buildSourcePartition;
import static java.lang.String.format;
import static java.util.Objects.isNull;

/**
 * A storage object format class.
 * This has logics to process storageObject into SourceRecord object.
 */
public abstract class StorageObjectFormat {

  private static final Logger log = LoggerFactory.getLogger(StorageObjectFormat.class);

  private static final long LENGTH_ZERO = 0L;

  public SourceRecordOffset nextRecord(StorageObject storageObject, String folder, 
      String directoryDelimiter, long offset, String fileNameRegexPattern) {

    if (emptyObject(storageObject)) {
      //TODO Allow skip objects under failure.
      log.error("Error processing object {} couldn't determine the size of storageObject.", 
          storageObject);
      throw new ConnectException(
          format("Invalid object %s without metadata or content length", storageObject.getKey()));
    }
    final Optional<StorageObjectKeyParts> maybeKeyParts = StorageObjectKeyParts
        .fromKey(directoryDelimiter, storageObject.getKey(), fileNameRegexPattern);
    final long objectSize = storageObject.getContentLength();
    final ValueOffset valueOffset = extractRecord(storageObject.getObjectContent(), 
        objectSize, offset);
    final SchemaAndValue schemaAndValue = valueOffset.getRecord();

    if (maybeKeyParts.isPresent()) {
      final StorageObjectKeyParts keyParts = maybeKeyParts.get();
      final SourceRecord sourceRecord = new SourceRecord(buildSourcePartition(folder),
          buildSourceOffset(storageObject.getKey(), valueOffset.getOffset(), valueOffset.isEof()),
          keyParts.getTopic(), keyParts.getPartition(), schemaAndValue.schema(),
          schemaAndValue.value()
      );
      return new SourceRecordOffset(sourceRecord, valueOffset.getOffset(), valueOffset.isEof());
    }

    throw new ConnectException(
        format("Storage Object key not in expected format: '%s'", storageObject.getKey()));
  }

  protected Segment readUntilSeparator(
      InputStream content, String lineSeparator, long objectSize, long offset
  ) {
    try {
      final byte[] separator = lineSeparator.getBytes(StandardCharsets.UTF_8);
      final ByteArrayOutputStream stream = new ByteArrayOutputStream();
      while (!endsWith(stream.toByteArray(), separator)) {
        final int read = content.read();
        if (read > -1) {
          stream.write(read);
        } else {
          log.error(
              "Malformed file, content in the file without a final bytes separator {}",
              lineSeparator
          );
          //TODO Allow skip objects under failure.
          throw new ConnectException(
              format(
                  "Malformed file, content in the file without final separator '%s'",
                  lineSeparator
              ));
        }
      }

      final boolean last = offset + stream.size() == objectSize;
      final byte[] value =
          Arrays.copyOfRange(stream.toByteArray(), 0, stream.size() - lineSeparator.length());

      log.trace("Read {} bytes object stream", value.length);
      return new Segment(value, stream.size(), last);
    } catch (IOException e) {
      throw new ConnectException("Count not read storage object stream", e);
    }
  }

  private boolean endsWith(byte[] buffer, byte[] pattern) {
    if (buffer.length < pattern.length) {
      return false;
    }

    final int start = buffer.length - pattern.length;
    final byte[] end = Arrays.copyOfRange(buffer, start, buffer.length);

    return Arrays.equals(end, pattern);
  }

  public abstract ValueOffset extractRecord(
      InputStream content, long offset, long objectSize
  );

  public void skip(InputStream content, long offset) {
    log.debug("Skipping object stream to offset {}", offset);
    try {
      final long skipped = content.skip(offset);
      if (skipped != offset) {
        throw new ConnectException(format(
            "Could not skip correct number of bytes. Attempted to skip %d Actually skipped %d",
            offset, skipped
        ));
      }
    } catch (IOException e) {
      throw new ConnectException("Could not skip to desired offset in storage object", e);
    }
  }

  public abstract String getExtension();

  private boolean emptyObject(StorageObject object) {
    return isNull(object.getObjectMetadata())
        || object.getContentLength() == LENGTH_ZERO;
  }
}
