/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.azure.blob.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.ContainerClient;
import com.azure.storage.blob.models.StorageException;

@RunWith(MockitoJUnitRunner.class)
public class AzureBlobSourceStorageTest {

  @Test
  public void existsReturnsFalseIfNameIsEmpty() {
    BlobClient mockBlobClient = Mockito.mock(BlobClient.class);
    ContainerClient mockContainerClient = Mockito.mock(ContainerClient.class);
    AzureBlobSourceStorage storage = new AzureBlobSourceStorage(null, mockContainerClient);
    boolean exists = storage.exists("");
    assertFalse(exists);
    exists = storage.exists(" ");
    assertFalse(exists);
  }

  
  @Test public void existsReturnsHappyPath() { 
    BlobClient mockBlobClient = Mockito.mock(BlobClient.class);
    ContainerClient mockContainerClient = Mockito.mock(ContainerClient.class);
    Mockito.when(mockContainerClient.getBlobClient(anyString())).thenReturn(mockBlobClient); 
    Mockito.when(mockBlobClient.exists()).thenReturn(true);
    AzureBlobSourceStorage storage = new AzureBlobSourceStorage(null, mockContainerClient); 
    boolean exists = storage.exists("dummy_blob"); 
    assertTrue(exists); 
  }
   

  @Test
  public void existsReturnsFalseBecauseBlobDoesntExist() {
    BlobClient mockBlobClient = Mockito.mock(BlobClient.class);
    StorageException storageException = Mockito.mock(StorageException.class);
    ContainerClient mockContainerClient = Mockito.mock(ContainerClient.class);
    Mockito.when(mockContainerClient.getBlobClient(anyString())).thenReturn(mockBlobClient);
    AzureBlobSourceStorage storage = new AzureBlobSourceStorage(null, mockContainerClient);
    boolean exists = storage.exists("dummy_blob");
    assertFalse(exists);
  }

  
  @Test(expected = ConnectException.class) 
  public void listFoldersReturnsException() { 
    BlobClient mockBlobClient = Mockito.mock(BlobClient.class);
    ContainerClient mockContainerClient = Mockito.mock(ContainerClient.class);
    AzureBlobSourceStorage storage = new AzureBlobSourceStorage(null, mockContainerClient);
    storage.listFolders("topics", "/"); 
  }
   

  @Test(expected = UnsupportedOperationException.class)
  public void deleteThrowsUnsupported() {
    AzureBlobSourceStorage azureBlobStorage = new AzureBlobSourceStorage(null, null);
    azureBlobStorage.delete("");
  }

  @Test
  public void urlDoesntTriggerExceptionAndReturnsWhatsPassedIn() throws MalformedURLException {
    ContainerClient mockContainerClient = Mockito.mock(ContainerClient.class);
    Mockito.when(mockContainerClient.getContainerUrl()).thenReturn(new URL("http://www.example.com"));
    AzureBlobSourceStorage storage = new AzureBlobSourceStorage(null, mockContainerClient);
    String url = storage.url();
    assertEquals("http://www.example.com", url);
  }

  @Test
  public void confReturnsPassedInConf() {
    AzureBlobStorageSourceConnectorConfig mockConfig = Mockito.mock(AzureBlobStorageSourceConnectorConfig.class);
    AzureBlobSourceStorage storage = new AzureBlobSourceStorage(mockConfig, null);
    assertSame(mockConfig, storage.getConfiguration());
  }

  
  @Test 
  public void bucketExistsReturnsTrue() throws URISyntaxException {
    ContainerClient mockContainerClient = Mockito.mock(ContainerClient.class);
    Mockito.when(mockContainerClient.exists()).thenReturn(true); 
    AzureBlobSourceStorage storage = new AzureBlobSourceStorage(null, mockContainerClient);
    assertTrue(storage.bucketExists()); 
  }
   
  @Test
  public void bucketExistsReturnsFalse() throws URISyntaxException {
    ContainerClient mockContainerClient = Mockito.mock(ContainerClient.class);
    Mockito.when(mockContainerClient.exists()).thenReturn(false);
    AzureBlobSourceStorage storage = new AzureBlobSourceStorage(null, mockContainerClient);
    assertFalse(storage.bucketExists());
  }
  
  @Test(expected = ConnectException.class) 
  public void listBaseFoldersThrowsRuntime() throws URISyntaxException { 
    ContainerClient mockContainerClient = Mockito.mock(ContainerClient.class);
    AzureBlobSourceStorage storage = new AzureBlobSourceStorage(null, mockContainerClient);
    storage.listBaseFolders("topics", "/");
 }
   
}