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

package org.wso2.carbon.connector.azure.storage;

import org.apache.commons.lang.StringUtils;
import org.apache.synapse.ManagedLifecycle;
import org.apache.synapse.MessageContext;
import org.apache.synapse.core.SynapseEnvironment;
import org.wso2.carbon.connector.azure.storage.connection.AzureStorageConnectionHandler;
import org.wso2.carbon.connector.azure.storage.connection.ConnectionConfiguration;
import org.wso2.carbon.connector.azure.storage.exceptions.InvalidConfigurationException;
import org.wso2.carbon.connector.azure.storage.util.AzureConstants;
import org.wso2.carbon.connector.azure.storage.util.AzureUtil;
import org.wso2.carbon.connector.azure.storage.util.Error;
import org.wso2.carbon.connector.core.AbstractConnector;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.connection.ConnectionHandler;
import org.wso2.carbon.connector.core.util.ConnectorUtils;

import java.time.Duration;

/**
 * This class is for reading connector init params and initializing AzureStorageConnectionHandler.
 */
public class AzureConfig extends AbstractConnector implements ManagedLifecycle {

    @Override
    public void init(SynapseEnvironment synapseEnvironment) {
        // Do nothing on deployment - configs unknown by that time
    }

    @Override
    public void destroy() {

        ConnectionHandler.getConnectionHandler().
                shutdownConnections(AzureConstants.CONNECTOR_NAME);
    }

    @Override
    public void connect(MessageContext messageContext) throws ConnectException {

        String connectorName = AzureConstants.CONNECTOR_NAME;
        String connectionName = "";
        try {
            ConnectionConfiguration configuration = getConnectionConfigFromContext(messageContext);
            connectionName = configuration.getConnectionName();

            ConnectionHandler handler = ConnectionHandler.getConnectionHandler();
            if (handler.checkIfConnectionExists(connectorName, connectionName)) {
                AzureStorageConnectionHandler connectionHandler = (AzureStorageConnectionHandler) handler
                        .getConnection(connectorName, connectionName);
                if (!connectionHandler.getConnectionConfig().equals(configuration)) {
                    connectionHandler.setConnectionConfig(configuration);
                }
            } else {
                AzureStorageConnectionHandler azureStorageConnectionHandler = new AzureStorageConnectionHandler(configuration);
                handler.createConnection(AzureConstants.CONNECTOR_NAME, connectionName, azureStorageConnectionHandler);
            }
        } catch (InvalidConfigurationException e) {
            AzureUtil.setErrorPropertiesToMessage(messageContext, Error.INVALID_CONFIGURATION, e.getMessage());
            handleException("[" + connectionName + "] Failed to initiate Azure Storage connector configuration.", e,
                    messageContext);
        }
    }

    private ConnectionConfiguration getConnectionConfigFromContext(MessageContext msgContext) throws InvalidConfigurationException {

        String connectionName = (String) ConnectorUtils.
                lookupTemplateParamater(msgContext, AzureConstants.CONNECTION_NAME);
        String endpointProtocol = (String) ConnectorUtils.
                lookupTemplateParamater(msgContext, AzureConstants.PROTOCOL);
        String accountName = (String) ConnectorUtils.
                lookupTemplateParamater(msgContext, AzureConstants.ACCOUNT_NAME);
        String accountKey = (String) ConnectorUtils.
                lookupTemplateParamater(msgContext, AzureConstants.ACCOUNT_KEY);
        String clientId = (String) ConnectorUtils.lookupTemplateParamater(msgContext, AzureConstants.CLIENT_ID);
        String clientSecret = (String) ConnectorUtils.lookupTemplateParamater(msgContext, AzureConstants.CLIENT_SECRET);
        String tenantId = (String) ConnectorUtils.lookupTemplateParamater(msgContext, AzureConstants.TENANT_ID);
        String maxIdleTime = (String) ConnectorUtils.lookupTemplateParamater(msgContext, AzureConstants.MAX_IDLE_TIME);
        String maxLifeTime = (String) ConnectorUtils.lookupTemplateParamater(msgContext, AzureConstants.MAX_LIFE_TIME);

        ConnectionConfiguration connectionConfig = new ConnectionConfiguration();
        connectionConfig.setConnectionName(connectionName);
        connectionConfig.setAccountKey(accountKey);
        connectionConfig.setAccountName(accountName);
        connectionConfig.setEndpointProtocol(endpointProtocol);
        connectionConfig.setClientID(clientId);
        connectionConfig.setClientSecret(clientSecret);
        connectionConfig.setTenantID(tenantId);
        connectionConfig.setMaxIdleTime(parseDurationInMillis(AzureConstants.MAX_IDLE_TIME, maxIdleTime));
        connectionConfig.setMaxLifeTime(parseDurationInMillis(AzureConstants.MAX_LIFE_TIME, maxLifeTime));
        return connectionConfig;
    }

    /**
     * Parses an optional duration parameter given in milliseconds.
     *
     * @return the duration, or null if the parameter is not set
     * @throws InvalidConfigurationException if the value is not a positive whole number
     */
    static Duration parseDurationInMillis(String parameterName, String value) throws InvalidConfigurationException {

        if (StringUtils.isBlank(value)) {
            return null;
        }
        long millis;
        try {
            millis = Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new InvalidConfigurationException("Invalid value for '" + parameterName + "': " + value
                    + ". It should be a positive number of milliseconds.");
        }
        if (millis <= 0) {
            throw new InvalidConfigurationException("Invalid value for '" + parameterName + "': " + value
                    + ". It should be a positive number of milliseconds.");
        }
        return Duration.ofMillis(millis);
    }
}
