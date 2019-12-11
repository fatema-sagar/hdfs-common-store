/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.azure.blob.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

import java.net.Authenticator;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.PasswordAuthentication;
import java.net.Proxy;
import java.net.URL;
import java.net.UnknownHostException;

import org.junit.Test;
import org.mockito.Mockito;


public class BlobStorageProxyTest {

  @Test
  public void configureProxyShouldProduceNullProxyWhenNotConfigure() {
    AzureBlobStorageSourceConnectorConfig mockConfig =
            Mockito.mock(AzureBlobStorageSourceConnectorConfig.class);

    when(mockConfig.getString(AzureBlobStorageSourceConnectorConfig.AZ_PROXY_URL_CONFIG)).thenReturn("");

    assertNull(BlobStorageProxy.configureProxy(mockConfig));
  }

  @Test
  public void configureProxyShouldReturnProxyWhenConfigured() {
    AzureBlobStorageSourceConnectorConfig mockConfig =
              Mockito.mock(AzureBlobStorageSourceConnectorConfig.class);

    when(mockConfig.getString(AzureBlobStorageSourceConnectorConfig.AZ_PROXY_URL_CONFIG))
            .thenReturn("http://localhost:8443");

    Proxy proxy = BlobStorageProxy.configureProxy(mockConfig);
    assertNotNull(proxy);
    assertEquals(proxy.address(), new InetSocketAddress("localhost", 8443));
  }

  @Test
  public void configureProxyShouldReturnProxyDefaultPortWhenConfigured() {
    AzureBlobStorageSourceConnectorConfig mockConfig =
              Mockito.mock(AzureBlobStorageSourceConnectorConfig.class);
    when(mockConfig.getString(AzureBlobStorageSourceConnectorConfig.AZ_PROXY_URL_CONFIG))
            .thenReturn("http://localhost");

    Proxy proxy = BlobStorageProxy.configureProxy(mockConfig);
    assertEquals(proxy.address(), new InetSocketAddress("localhost", 8080));
  }

  @Test
  public void configureProxyShouldConfigureAPasswordAuthenticatorForProxy() throws UnknownHostException,
          MalformedURLException {

    AzureBlobStorageSourceConnectorConfig mockConfig =
              Mockito.mock(AzureBlobStorageSourceConnectorConfig.class);
    AzureBlobSourceStorage classUnderTest = new AzureBlobSourceStorage(mockConfig,null);
    when(mockConfig.getString(AzureBlobStorageSourceConnectorConfig.AZ_PROXY_URL_CONFIG))
            .thenReturn("http://localhost:8080");
    when(mockConfig.getString(AzureBlobStorageSourceConnectorConfig.AZ_PROXY_USER_CONFIG))
            .thenReturn("username");
    when(mockConfig.getString(AzureBlobStorageSourceConnectorConfig.AZ_PROXY_PASS_CONFIG))
            .thenReturn("password");

    BlobStorageProxy.configureProxy(mockConfig);

    PasswordAuthentication passwordAuthentication = Authenticator.requestPasswordAuthentication("localhost:8080",
            Inet4Address.getByName("127.0.0.1"), 8080, "http", "null",
            "http", new URL("http://microsoft.net"), Authenticator.RequestorType.PROXY);

    assertNotNull(passwordAuthentication);
    assertEquals(passwordAuthentication.getUserName(), "username");
    assertEquals(String.valueOf(passwordAuthentication.getPassword()), "password");
  }

}