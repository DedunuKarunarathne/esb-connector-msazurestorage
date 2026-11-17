/*
 *  Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
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

import com.azure.storage.blob.BlobServiceClient;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.wso2.carbon.connector.azure.storage.exceptions.InvalidConfigurationException;
import org.wso2.carbon.connector.core.ConnectException;
import reactor.netty.resources.ConnectionProvider;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Tests the lifecycle of the client and its dedicated connection pool in {@link AzureStorageConnectionHandler}.
 * No network access is needed: creating a client does not connect to Azure.
 */
public class AzureStorageConnectionHandlerTest {

    private static final int THREADS = 50;

    @Test
    public void testDedicatedPoolOnlyWhenPoolParameterIsSet() throws Exception {

        TrackingHandler noParameters = new TrackingHandler(config(null, null, "http"));
        Assert.assertNotNull(noParameters.getBlobServiceClient());
        Assert.assertNull(noParameters.getConnectionProvider(), "The shared pool must be used");

        TrackingHandler idleOnly = new TrackingHandler(config(10000L, null, "http"));
        idleOnly.getBlobServiceClient();
        Assert.assertSame(idleOnly.getConnectionProvider(), idleOnly.createdProviders().get(0));

        TrackingHandler lifeOnly = new TrackingHandler(config(null, 10000L, "http"));
        lifeOnly.getBlobServiceClient();
        Assert.assertSame(lifeOnly.getConnectionProvider(), lifeOnly.createdProviders().get(0));
    }

    @Test
    public void testConcurrentFirstUseCreatesOneClientAndOnePool() throws Exception {

        TrackingHandler handler = new TrackingHandler(config(10000L, null, "http"));
        List<BlobServiceClient> clients = runConcurrently(handler::getBlobServiceClient);

        Assert.assertEquals(distinct(clients).size(), 1, "All threads must get the same client");
        Assert.assertEquals(handler.createdProviders().size(), 1, "Exactly one pool must be created");
        Assert.assertSame(handler.getConnectionProvider(), handler.createdProviders().get(0));
    }

    @Test
    public void testConcurrentConfigChangeCreatesOneNewPool() throws Exception {

        TrackingHandler handler = new TrackingHandler(config(10000L, null, "http"));
        BlobServiceClient oldClient = handler.getBlobServiceClient();
        ConnectionProvider oldProvider = handler.getConnectionProvider();
        ConnectionConfiguration newConfig = config(15000L, null, "http");

        // Same check-then-set sequence as AzureConfig.connect(), followed by an operation getting the client.
        List<BlobServiceClient> clients = runConcurrently(() -> {
            if (!handler.getConnectionConfig().equals(newConfig)) {
                handler.setConnectionConfig(newConfig);
            }
            return handler.getBlobServiceClient();
        });

        Assert.assertEquals(handler.createdProviders().size(), 2, "Exactly one new pool must be created");
        ConnectionProvider newProvider = handler.createdProviders().get(1);
        Assert.assertSame(handler.getConnectionProvider(), newProvider);
        Assert.assertTrue(handler.isDisposed(oldProvider), "The replaced pool must be disposed");
        Assert.assertFalse(handler.isDisposed(newProvider), "The current pool must not be disposed");
        Assert.assertEquals(distinct(clients).size(), 1, "All threads must get the new client");
        Assert.assertNotSame(clients.get(0), oldClient);
        Assert.assertEquals(handler.getConnectionConfig(), newConfig);
    }

    @Test
    public void testFailedConfigChangeKeepsCurrentStateAndDisposesNewPool() throws Exception {

        ConnectionConfiguration oldConfig = config(10000L, null, "http");
        TrackingHandler handler = new TrackingHandler(oldConfig);
        BlobServiceClient oldClient = handler.getBlobServiceClient();
        ConnectionProvider oldProvider = handler.getConnectionProvider();

        // An invalid protocol makes the SDK reject the endpoint after the new pool has been created.
        try {
            handler.setConnectionConfig(config(15000L, null, "invalid protocol"));
            Assert.fail("Expected the client creation to fail");
        } catch (RuntimeException expected) {
            // expected: the exact exception type is defined by the Azure SDK
        }

        Assert.assertEquals(handler.createdProviders().size(), 2);
        Assert.assertTrue(handler.isDisposed(handler.createdProviders().get(1)), "The unused pool must be disposed");
        Assert.assertFalse(handler.isDisposed(oldProvider), "The current pool must stay in use");
        Assert.assertSame(handler.getConnectionProvider(), oldProvider);
        Assert.assertSame(handler.getBlobServiceClient(), oldClient);
        Assert.assertEquals(handler.getConnectionConfig(), oldConfig, "The failed configuration must not be stored");
    }

    @Test
    public void testConfigChangeWithMissingCredentialsKeepsCurrentState() throws Exception {

        ConnectionConfiguration oldConfig = config(10000L, null, "http");
        TrackingHandler handler = new TrackingHandler(oldConfig);
        BlobServiceClient oldClient = handler.getBlobServiceClient();

        ConnectionConfiguration missingKey = config(10000L, null, "http");
        missingKey.setAccountKey(null);
        try {
            handler.setConnectionConfig(missingKey);
            Assert.fail("Expected InvalidConfigurationException");
        } catch (InvalidConfigurationException expected) {
            // expected
        }

        Assert.assertEquals(handler.createdProviders().size(), 1, "No pool must be created");
        Assert.assertSame(handler.getBlobServiceClient(), oldClient);
        Assert.assertEquals(handler.getConnectionConfig(), oldConfig);
    }

    @Test
    public void testCloseDisposesPoolAndRejectsFurtherUse() throws Exception {

        TrackingHandler handler = new TrackingHandler(config(10000L, null, "http"));
        handler.getBlobServiceClient();
        ConnectionProvider provider = handler.getConnectionProvider();

        handler.close();

        Assert.assertTrue(handler.isDisposed(provider));
        Assert.assertNull(handler.getConnectionProvider());
        try {
            handler.getBlobServiceClient();
            Assert.fail("Expected ConnectException after close");
        } catch (ConnectException expected) {
            // expected
        }
        try {
            handler.setConnectionConfig(config(15000L, null, "http"));
            Assert.fail("Expected ConnectException after close");
        } catch (ConnectException expected) {
            // expected
        }
        Assert.assertEquals(handler.createdProviders().size(), 1, "A closed handler must not create a new pool");
    }

    @Test
    public void testCloseDuringConcurrentFirstUseLeavesNoLivePool() throws Exception {

        for (int iteration = 0; iteration < 20; iteration++) {
            TrackingHandler handler = new TrackingHandler(config(10000L, null, "http"));
            ExecutorService executor = Executors.newFixedThreadPool(THREADS + 1);
            try {
                CountDownLatch start = new CountDownLatch(1);
                List<Future<Object>> results = new ArrayList<>();
                for (int i = 0; i < THREADS; i++) {
                    results.add(executor.submit(() -> {
                        start.await();
                        try {
                            return handler.getBlobServiceClient();
                        } catch (ConnectException e) {
                            return e;
                        }
                    }));
                }
                Future<?> closer = executor.submit(() -> {
                    start.await();
                    handler.close();
                    return null;
                });
                start.countDown();
                closer.get(60, TimeUnit.SECONDS);

                for (Future<Object> result : results) {
                    // Any exception other than a clean rejection fails the test here.
                    Object outcome = result.get(60, TimeUnit.SECONDS);
                    Assert.assertTrue(outcome instanceof BlobServiceClient || outcome instanceof ConnectException,
                            "Unexpected outcome: " + outcome);
                }
                Assert.assertTrue(handler.createdProviders().size() <= 1, "At most one pool may be created");
                for (ConnectionProvider provider : handler.createdProviders()) {
                    Assert.assertTrue(handler.isDisposed(provider), "No pool may stay live after close");
                }
                Assert.assertNull(handler.getConnectionProvider());
            } finally {
                executor.shutdownNow();
            }
        }
    }

    private static <T> List<T> runConcurrently(Callable<T> task) throws Exception {

        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<T>> futures = new ArrayList<>();
            for (int i = 0; i < THREADS; i++) {
                futures.add(executor.submit(() -> {
                    start.await();
                    return task.call();
                }));
            }
            start.countDown();
            List<T> results = new ArrayList<>();
            for (Future<T> future : futures) {
                results.add(future.get(60, TimeUnit.SECONDS));
            }
            return results;
        } finally {
            executor.shutdownNow();
        }
    }

    private static <T> Set<T> distinct(List<T> items) {

        Set<T> set = Collections.newSetFromMap(new IdentityHashMap<>());
        set.addAll(items);
        return set;
    }

    private static ConnectionConfiguration config(Long maxIdleMillis, Long maxLifeMillis, String protocol)
            throws Exception {

        ConnectionConfiguration config = new ConnectionConfiguration();
        config.setConnectionName("TEST_CONNECTION");
        config.setAccountName("devstoreaccount1");
        config.setAccountKey("ZHVtbXlrZXlkdW1teWtleWR1bW15a2V5ZHVtbXlrZXk=");
        config.setEndpointProtocol(protocol);
        config.setMaxIdleTime(maxIdleMillis == null ? null : Duration.ofMillis(maxIdleMillis));
        config.setMaxLifeTime(maxLifeMillis == null ? null : Duration.ofMillis(maxLifeMillis));
        return config;
    }

    /**
     * Records every dedicated pool the handler creates and whether it has been disposed.
     */
    private static class TrackingHandler extends AzureStorageConnectionHandler {

        private final List<ConnectionProvider> created = Collections.synchronizedList(new ArrayList<>());
        private final Set<ConnectionProvider> disposed =
                Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));

        TrackingHandler(ConnectionConfiguration config) {

            super(config);
        }

        @Override
        ConnectionProvider createConnectionProvider(ConnectionConfiguration config) {

            ConnectionProvider real = super.createConnectionProvider(config);
            if (real == null) {
                return null;
            }
            ConnectionProvider[] self = new ConnectionProvider[1];
            self[0] = (ConnectionProvider) Proxy.newProxyInstance(ConnectionProvider.class.getClassLoader(),
                    new Class<?>[]{ConnectionProvider.class}, (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "equals":
                                return proxy == args[0];
                            case "hashCode":
                                return System.identityHashCode(proxy);
                            case "dispose":
                            case "disposeLater":
                                disposed.add(self[0]);
                                break;
                            default:
                                break;
                        }
                        try {
                            return method.invoke(real, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    });
            created.add(self[0]);
            return self[0];
        }

        List<ConnectionProvider> createdProviders() {

            synchronized (created) {
                return new ArrayList<>(created);
            }
        }

        boolean isDisposed(ConnectionProvider provider) {

            return disposed.contains(provider);
        }
    }
}
