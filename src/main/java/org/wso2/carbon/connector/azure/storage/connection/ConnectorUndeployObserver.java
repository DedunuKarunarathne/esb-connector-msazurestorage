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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.config.AbstractSynapseObserver;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.config.SynapseObserver;
import org.apache.synapse.libraries.model.Library;
import org.wso2.carbon.connector.azure.storage.util.AzureConstants;
import org.wso2.carbon.connector.core.connection.ConnectionHandler;

/**
 * Closes the connections of this connector when the connector library is undeployed, as Synapse does not call
 * destroy() on the connector's class mediators then.
 * <p>
 * {@code synapseLibraryRemoved} and {@code SynapseConfiguration.unregisterObserver} exist in the Synapse runtime of
 * MI 4.0.0 and later, but not in the synapse-core version this connector is compiled against. Hence the method has no
 * {@code @Override}, and the observer unregisters itself using reflection.
 */
public class ConnectorUndeployObserver extends AbstractSynapseObserver {

    private static final Log LOG = LogFactory.getLog(ConnectorUndeployObserver.class);
    static final String CONNECTOR_LIBRARY_NAME = "msazurestorage";
    static final String CONNECTOR_LIBRARY_PACKAGE = "org.wso2.carbon.connector";

    private final SynapseConfiguration synapseConfiguration;

    public ConnectorUndeployObserver(SynapseConfiguration synapseConfiguration) {

        this.synapseConfiguration = synapseConfiguration;
    }

    public void synapseLibraryRemoved(Library library) {

        if (library != null && library.getQName() != null
                && CONNECTOR_LIBRARY_NAME.equals(library.getQName().getLocalPart())
                && CONNECTOR_LIBRARY_PACKAGE.equals(library.getPackage())) {
            LOG.info("Azure Storage connector is being undeployed. Closing its connections.");
            closeConnections();
            unregister();
        }
    }

    // Package-private for tests.
    void closeConnections() {

        ConnectionHandler.getConnectionHandler().shutdownConnections(AzureConstants.CONNECTOR_NAME);
    }

    private void unregister() {

        // Synapse removes the observer only after notifying all observers.
        try {
            synapseConfiguration.getClass().getMethod("unregisterObserver", SynapseObserver.class)
                    .invoke(synapseConfiguration, this);
        } catch (ReflectiveOperationException e) {
            LOG.warn("Could not unregister the undeploy observer of the Azure Storage connector.", e);
        }
    }

    // Synapse does not register an observer equal to an already registered one, so one observer is registered.
    @Override
    public boolean equals(Object o) {

        return o != null && getClass() == o.getClass();
    }

    @Override
    public int hashCode() {

        return getClass().getName().hashCode();
    }
}
