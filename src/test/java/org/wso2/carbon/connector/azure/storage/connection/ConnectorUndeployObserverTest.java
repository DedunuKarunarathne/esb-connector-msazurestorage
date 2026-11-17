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

import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.config.SynapseObserver;
import org.apache.synapse.libraries.model.Library;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import javax.xml.namespace.QName;

/**
 * Tests that {@link ConnectorUndeployObserver} closes the connections only when this connector is undeployed.
 */
public class ConnectorUndeployObserverTest {

    @Test
    public void testConnectionsAreClosedWhenThisConnectorIsUndeployed() {

        RecordingSynapseConfiguration synapseConfiguration = new RecordingSynapseConfiguration();
        CountingObserver observer = new CountingObserver(synapseConfiguration);

        observer.synapseLibraryRemoved(library(ConnectorUndeployObserver.CONNECTOR_LIBRARY_NAME,
                ConnectorUndeployObserver.CONNECTOR_LIBRARY_PACKAGE));

        Assert.assertEquals(observer.closeCount, 1);
        Assert.assertEquals(synapseConfiguration.unregistered.size(), 1);
        Assert.assertSame(synapseConfiguration.unregistered.get(0), observer);
    }

    @Test
    public void testOtherLibrariesAreIgnored() {

        RecordingSynapseConfiguration synapseConfiguration = new RecordingSynapseConfiguration();
        CountingObserver observer = new CountingObserver(synapseConfiguration);

        observer.synapseLibraryRemoved(library("file", ConnectorUndeployObserver.CONNECTOR_LIBRARY_PACKAGE));
        observer.synapseLibraryRemoved(library(ConnectorUndeployObserver.CONNECTOR_LIBRARY_NAME, "org.example"));
        observer.synapseLibraryRemoved(null);

        Assert.assertEquals(observer.closeCount, 0);
        Assert.assertTrue(synapseConfiguration.unregistered.isEmpty());
    }

    @Test
    public void testObserversOfThisConnectorAreEqual() {

        SynapseConfiguration synapseConfiguration = new RecordingSynapseConfiguration();
        ConnectorUndeployObserver first = new ConnectorUndeployObserver(synapseConfiguration);
        ConnectorUndeployObserver second = new ConnectorUndeployObserver(synapseConfiguration);

        // Synapse registers an observer only if no equal observer is registered.
        Assert.assertEquals(first, second);
        Assert.assertEquals(first.hashCode(), second.hashCode());
    }

    private static Library library(String name, String packageName) {

        return (Library) Proxy.newProxyInstance(Library.class.getClassLoader(), new Class<?>[]{Library.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "getQName":
                            return new QName(packageName, name);
                        case "getPackage":
                            return packageName;
                        case "getFileName":
                            return "/tmp/" + name + "-connector.zip";
                        case "equals":
                            return proxy == args[0];
                        case "hashCode":
                            return System.identityHashCode(proxy);
                        default:
                            return null;
                    }
                });
    }

    private static class CountingObserver extends ConnectorUndeployObserver {

        private int closeCount;

        CountingObserver(SynapseConfiguration synapseConfiguration) {

            super(synapseConfiguration);
        }

        @Override
        void closeConnections() {

            closeCount++;
        }
    }

    /**
     * Provides unregisterObserver, as available in the Synapse runtime of supported MI versions.
     */
    public static class RecordingSynapseConfiguration extends SynapseConfiguration {

        private final List<SynapseObserver> unregistered = new ArrayList<>();

        public void unregisterObserver(SynapseObserver observer) {

            unregistered.add(observer);
        }
    }
}
