/*
 *  Copyright (c) 2023, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.wso2.carbon.connector.azure.storage.connection;

import com.azure.core.http.HttpClient;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.connector.azure.storage.exceptions.InvalidConfigurationException;
import org.wso2.carbon.connector.azure.storage.util.AzureConstants;
import org.wso2.carbon.connector.azure.storage.util.AzureUtil;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.connection.Connection;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;

public class AzureStorageConnectionHandler implements Connection {

    private static final Log LOG = LogFactory.getLog(AzureStorageConnectionHandler.class);
    private static final String CONNECTION_PROVIDER_PREFIX = "azure-storage-";
    private static final Duration MIN_EVICTION_INTERVAL = Duration.ofSeconds(1);

    // Read without locking; written only while holding the lock on this handler.
    private volatile ConnectionConfiguration connectionConfig;
    private volatile BlobServiceClient blobServiceClient;
    private volatile boolean closed;
    // Null when the shared Reactor Netty pool is used. Guarded by the lock on this handler.
    private ConnectionProvider connectionProvider;

    public AzureStorageConnectionHandler(ConnectionConfiguration fsConfig) {

        this.connectionConfig = fsConfig;
    }

    /**
     * @return an instance of BlobServiceClient
     */
    public BlobServiceClient getBlobServiceClient() throws ConnectException {

        BlobServiceClient client = blobServiceClient;
        if (client == null || closed) {
            synchronized (this) {
                checkNotClosed();
                if (blobServiceClient == null) {
                    createNewBlobServiceClientInstance(this.connectionConfig);
                }
                client = blobServiceClient;
            }
        }
        return client;
    }

    public ConnectionConfiguration getConnectionConfig() {

        return connectionConfig;
    }

    public synchronized void setConnectionConfig(ConnectionConfiguration connectionConfig) throws ConnectException {

        checkNotClosed();
        if (blobServiceClient != null && connectionConfig.equals(this.connectionConfig)) {
            // Another thread has already applied this configuration.
            return;
        }
        ConnectionProvider replacedConnectionProvider = this.connectionProvider;
        createNewBlobServiceClientInstance(connectionConfig);
        disposeConnectionProvider(replacedConnectionProvider, poolName(connectionConfig));
    }

    /**
     * Disposes the dedicated connection pool, if any. Later calls are rejected; an operation that already obtained
     * the client may still complete.
     */
    public synchronized void close() {

        closed = true;
        disposeConnectionProvider(connectionProvider, poolName(connectionConfig));
        connectionProvider = null;
        blobServiceClient = null;
    }

    // For tests.
    synchronized ConnectionProvider getConnectionProvider() {

        return connectionProvider;
    }

    private void checkNotClosed() throws ConnectException {

        if (closed) {
            throw new ConnectException("The Azure Storage connection '" + connectionConfig.getConnectionName()
                    + "' is closed.");
        }
    }

    private static void disposeConnectionProvider(ConnectionProvider provider, String poolName) {

        if (provider != null) {
            provider.dispose();
            LOG.info("Disposed connection pool '" + poolName + "'.");
        }
    }

    // Not using ConnectionProvider.name(), which is not available in older Reactor Netty 1.0.x versions.
    private static String poolName(ConnectionConfiguration config) {

        return CONNECTION_PROVIDER_PREFIX + config.getConnectionName();
    }

    /**
     * Creates a client for the given configuration and, only if that succeeds, makes the client, its connection pool
     * and the configuration current. Must be called while holding the lock on this handler.
     */
    private void createNewBlobServiceClientInstance(ConnectionConfiguration config)
            throws InvalidConfigurationException {

        String clientId = config.getClientID();
        String clientSecret = config.getClientSecret();
        String tenantId = config.getTenantID();
        String accountName = config.getAccountName();
        String accountKey = config.getAccountKey();
        String endpointProtocol = config.getEndpointProtocol();

        boolean useClientCredentials = StringUtils.isNotEmpty(clientId) && StringUtils.isNotEmpty(clientSecret)
                && StringUtils.isNotEmpty(tenantId) && StringUtils.isNotEmpty(accountName);
        boolean useAccountKey = StringUtils.isNotEmpty(accountName) && StringUtils.isNotEmpty(accountKey);
        if (!useClientCredentials && !useAccountKey) {
            throw new InvalidConfigurationException("Missing authentication parameters.");
        }

        ConnectionProvider newConnectionProvider = createConnectionProvider(config);
        boolean created = false;
        try {
            HttpClient httpClient = newConnectionProvider == null
                    ? new NettyAsyncHttpClientBuilder().build()
                    : new NettyAsyncHttpClientBuilder().connectionProvider(newConnectionProvider).build();
            BlobServiceClient newBlobServiceClient;
            if (useClientCredentials) {
                ClientSecretCredential credential = new ClientSecretCredentialBuilder()
                        .httpClient(httpClient)
                        .clientId(clientId)
                        .clientSecret(clientSecret)
                        .tenantId(tenantId)
                        .build();
                newBlobServiceClient = new BlobServiceClientBuilder()
                        .httpClient(httpClient)
                        .credential(credential)
                        .endpoint(AzureConstants.HTTPS_PROTOCOL + accountName + AzureConstants.BLOB_ENDPOINT_SUFFIX)
                        .buildClient();
            } else {
                newBlobServiceClient = new BlobServiceClientBuilder()
                        .httpClient(httpClient)
                        .connectionString(AzureUtil.getStorageConnectionString(accountName, accountKey,
                                endpointProtocol))
                        .buildClient();
            }
            // Publish the client before the configuration, so that a thread that sees the new configuration also gets
            // the new client.
            this.connectionProvider = newConnectionProvider;
            this.blobServiceClient = newBlobServiceClient;
            this.connectionConfig = config;
            created = true;
        } finally {
            if (!created) {
                disposeConnectionProvider(newConnectionProvider, poolName(config));
            }
        }
    }

    /**
     * Creates a dedicated connection pool if a pool parameter is set. Otherwise returns null, and the shared Reactor
     * Netty pool is used. Package-private for tests.
     */
    ConnectionProvider createConnectionProvider(ConnectionConfiguration config) {

        if (!config.isConnectionPoolConfigured()) {
            return null;
        }
        String poolName = poolName(config);
        ConnectionProvider.Builder builder = ConnectionProvider.builder(poolName);
        Duration maxIdleTime = config.getMaxIdleTime();
        Duration maxLifeTime = config.getMaxLifeTime();
        if (maxIdleTime != null) {
            builder.maxIdleTime(maxIdleTime);
        }
        if (maxLifeTime != null) {
            builder.maxLifeTime(maxLifeTime);
        }
        // Close expired connections proactively instead of only when they are next acquired.
        Duration evictionInterval = shorterOf(maxIdleTime, maxLifeTime);
        if (evictionInterval.compareTo(MIN_EVICTION_INTERVAL) < 0) {
            evictionInterval = MIN_EVICTION_INTERVAL;
        }
        builder.evictInBackground(evictionInterval);
        ConnectionProvider provider = builder.build();
        LOG.info("Created connection pool '" + poolName + "' with maxIdleTime=" + toMillis(maxIdleTime)
                + ", maxLifeTime=" + toMillis(maxLifeTime) + ", evictionInterval=" + toMillis(evictionInterval) + ".");
        return provider;
    }

    private static String toMillis(Duration duration) {

        return duration == null ? "not set" : duration.toMillis() + "ms";
    }

    private static Duration shorterOf(Duration first, Duration second) {

        if (first == null) {
            return second;
        }
        if (second == null) {
            return first;
        }
        return first.compareTo(second) <= 0 ? first : second;
    }
}
