/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.azure.blob.storage;

import static io.confluent.connect.storage.common.util.StringUtils.isNotBlank;

import java.net.Authenticator;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.MessageFormat;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlobStorageProxy {
  private static final int DEFAULT_PROXY_PORT = 8080;
  private static final Logger log = LoggerFactory.getLogger(BlobStorageProxy.class);

  /**
   * Configure proxy, and set a process authenticator for proxy password challenge
   *
   * @param conf Connector configuration
   * @return proxy if configure of null if not
   */
  public static Proxy configureProxy(AzureBlobStorageSourceConnectorConfig conf) {
    final String proxyUrl =
        conf.getString(AzureBlobStorageSourceConnectorConfig.AZ_PROXY_URL_CONFIG);
    Proxy proxy = null;
    if (isNotBlank(proxyUrl)) {
      try {
        final URI proxyUri = new URI(proxyUrl);
        final String host = proxyUri.getHost();
        final Integer port = proxyUri.getPort() != - 1 ? proxyUri.getPort() : DEFAULT_PROXY_PORT;

        proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(host, port));
        log.debug("Proxy configured with host {} and port {}", host, port);

        // Azure client do not expose password authentication
        // set a default authenticator for process to response with proxy configuration
        final String proxyUsername =
            conf.getString(AzureBlobStorageSourceConnectorConfig.AZ_PROXY_USER_CONFIG);
        final String proxyPassword =
            conf.getString(AzureBlobStorageSourceConnectorConfig.AZ_PROXY_PASS_CONFIG);
        if (isNotBlank(proxyUsername) && isNotBlank(proxyPassword)) {
          log.debug("The process configure an authenticator for proxy with username {}",
              proxyUsername);

          Authenticator.setDefault(new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
              if (getRequestorType() == RequestorType.PROXY) {
                return new PasswordAuthentication(proxyUsername, proxyPassword.toCharArray());
              }
              return super.getPasswordAuthentication();
            }
          });
        }

      } catch (URISyntaxException e) {
        log.error("Invalid URI {} at proxy configuration", proxyUrl);
        throw new ConnectException(
            MessageFormat.format("Invalid URI {} at proxy configuration", proxyUrl), e);
      }
    }
    return proxy;
  }
  
}
