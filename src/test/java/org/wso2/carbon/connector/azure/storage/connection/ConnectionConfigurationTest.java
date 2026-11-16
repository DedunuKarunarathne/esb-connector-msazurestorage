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

import org.testng.Assert;
import org.testng.annotations.Test;

import java.time.Duration;

/**
 * Tests the connection pool parameters of {@link ConnectionConfiguration}.
 */
public class ConnectionConfigurationTest {

    @Test
    public void testEqualsAndHashCodeIncludePoolParameters() throws Exception {

        ConnectionConfiguration base = config(null, null);
        Assert.assertEquals(config(null, null), base);
        Assert.assertEquals(config(null, null).hashCode(), base.hashCode());

        Assert.assertEquals(config(10000L, 20000L), config(10000L, 20000L));
        Assert.assertEquals(config(10000L, 20000L).hashCode(), config(10000L, 20000L).hashCode());

        Assert.assertFalse(config(10000L, null).equals(base));
        Assert.assertFalse(config(null, 10000L).equals(base));
        Assert.assertFalse(config(10000L, null).equals(config(15000L, null)));
        Assert.assertFalse(config(null, 10000L).equals(config(null, 15000L)));
        Assert.assertFalse(config(10000L, null).equals(config(null, 10000L)));
    }

    @Test
    public void testConnectionPoolConfigured() throws Exception {

        Assert.assertFalse(config(null, null).isConnectionPoolConfigured());
        Assert.assertTrue(config(10000L, null).isConnectionPoolConfigured());
        Assert.assertTrue(config(null, 10000L).isConnectionPoolConfigured());
        Assert.assertTrue(config(10000L, 20000L).isConnectionPoolConfigured());
    }

    private static ConnectionConfiguration config(Long maxIdleMillis, Long maxLifeMillis) throws Exception {

        ConnectionConfiguration config = new ConnectionConfiguration();
        config.setConnectionName("TEST_CONNECTION");
        config.setAccountName("devstoreaccount1");
        config.setAccountKey("ZHVtbXlrZXk=");
        config.setEndpointProtocol("http");
        config.setMaxIdleTime(maxIdleMillis == null ? null : Duration.ofMillis(maxIdleMillis));
        config.setMaxLifeTime(maxLifeMillis == null ? null : Duration.ofMillis(maxLifeMillis));
        return config;
    }
}
