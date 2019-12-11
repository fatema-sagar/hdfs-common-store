/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.cloud.storage.source;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.OngoingStubbing;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.cloud.storage.source.StorageObjectFormat;
import io.confluent.connect.cloud.storage.source.format.CloudStorageByteArrayFormat;
import io.confluent.connect.cloud.storage.source.util.Segment;

import static java.lang.String.format;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class StorageObjectFormatTest {

  private static final String lineSeparator = System.lineSeparator();
  private static final long OFFSET_ZERO = 0L;
  private StorageObjectFormat storageObjectFormat;

  private InputStream inputStream;

  @Before
  public void setup() throws Exception {
    storageObjectFormat = new CloudStorageByteArrayFormat(new TestSourceConnectorConfig(createProps()));
    inputStream = mock(InputStream.class);
  }

  @Test
  public void shouldNotIncludeSeparatorInResult() throws IOException {
    byte[] content = format("my content%s", lineSeparator).getBytes();
    setupObjectStream(content);

    Segment segment = storageObjectFormat.readUntilSeparator(
        inputStream,
        lineSeparator,
        content.length,
        OFFSET_ZERO
    );
    assertThat(segment.getValue(), is("my content".getBytes()));
  }

  @Test(expected = ConnectException.class)
  public void shouldThrowExceptionIfNoSeparatorIsFound() throws IOException {
    byte[] content = "my no delimiter content".getBytes();
    setupObjectStream(content);

    storageObjectFormat
        .readUntilSeparator(inputStream, lineSeparator, content.length, OFFSET_ZERO);
  }

  @Test
  public void shouldReadMultipleTimesUntilSpecifiedSeparator() throws IOException {
    byte[] content = format("first part%ssecond part%s", lineSeparator, lineSeparator).getBytes();
    setupObjectStream(content);

    Segment segment = storageObjectFormat.readUntilSeparator(
        inputStream,
        lineSeparator,
        content.length,
        OFFSET_ZERO
    );
    assertThat(segment.getValue(), is("first part".getBytes()));

    segment = storageObjectFormat.readUntilSeparator(
        inputStream,
        lineSeparator,
        content.length,
        OFFSET_ZERO
    );
    assertThat(segment.getValue(), is("second part".getBytes()));
  }

  private void setupObjectStream(byte[] content) throws IOException {
    OngoingStubbing<Integer> readStub = when(inputStream.read());
    for (byte b : content) {
      readStub = readStub.thenReturn((int) b);
    }
    readStub.thenReturn(-1);
  }
  
  protected Map<String, String> createProps() {
    Map<String, String> props = new HashMap<>();

    props.put("confluent.topic.bootstrap.servers", "localhost:9092");
    props.put("confluent.topic.replication.factor", "1");

    props.put("record.batch.max.size", "1");
    props.put("format.class", "io.confluent.connect.cloud.storage.source.format.CloudStorageAvroFormat");

    return props;
  }


  /**
   * This a mock config implementation which implements CloudStorageSourceConnectorCommonConfig.
   */
  public class TestSourceConnectorConfig extends CloudStorageSourceConnectorCommonConfig{

    public TestSourceConnectorConfig(Map<String, String> props) {
      super(config(), props);
    }

    @Override
    protected Long getPollInterval() {
      return 10000L;
    }
  }

}