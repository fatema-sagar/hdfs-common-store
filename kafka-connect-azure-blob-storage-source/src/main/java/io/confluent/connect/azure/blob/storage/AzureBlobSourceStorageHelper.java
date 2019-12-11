/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.azure.blob.storage;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.storage.blob.ContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;

/**
 * This is a helper class for AzureBlobSourceStorage class which 
 * is used as abstraction for mocking the azure blob methods such 
 * as listBlobsHierarchy, listBlobsFlat as we are not able to mock 
 * them directly. 
 */
public class AzureBlobSourceStorageHelper {
  
  private static final Logger log = LoggerFactory.getLogger(AzureBlobSourceStorageHelper.class);
  
  private final ContainerClient containerClient;
  
  public AzureBlobSourceStorageHelper(ContainerClient containerClient) {
    this.containerClient = containerClient;
  }
  
  public Iterator<BlobItem> getBlobList(String delimiter, String path) {
    log.trace("Listing Blobs on azure Blob Storage with"
        + " path {} and delimiter {}", path, delimiter);
    ListBlobsOptions options = new ListBlobsOptions().prefix(path);
    return containerClient
        .listBlobsHierarchy(delimiter, options, null).iterator();
  }

  public Iterator<BlobItem> getBlobList(String delimiter, BlobItem blobItem) {
    log.trace("Listing Blobs on azure Blob Storage with"
        + " path {} and delimiter {}", blobItem.name(), delimiter);
    return containerClient
        .listBlobsHierarchy(delimiter, new ListBlobsOptions().prefix(blobItem.name()), null)
        .iterator();
  }
  
  public Iterator<BlobItem> getFlatBlobList(ListBlobsOptions options) {
    return containerClient.listBlobsFlat(options, null).iterator();
  }
  
  public List<BlobItem> getListOfBlobResponse(String path) {
    log.trace("Listing Blobs on azure Blob Storage with"
        + " path {}", path);
    List<BlobItem> blobsList = new ArrayList<>();
    containerClient.listBlobsFlat(new ListBlobsOptions().prefix(path), null).stream().forEach(
        response -> {
          blobsList.add(response);
        }
    );
    return blobsList;
  }

}
