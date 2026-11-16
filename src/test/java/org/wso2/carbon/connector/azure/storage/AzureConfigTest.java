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

package org.wso2.carbon.connector.azure.storage;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.wso2.carbon.connector.azure.storage.exceptions.InvalidConfigurationException;

import java.time.Duration;

/**
 * Tests parsing of the connection pool timeout parameters.
 */
public class AzureConfigTest {

    @Test
    public void testBlankValueIsNotSet() throws Exception {

        Assert.assertNull(AzureConfig.parseDurationInMillis("maxIdleTime", null));
        Assert.assertNull(AzureConfig.parseDurationInMillis("maxIdleTime", ""));
        Assert.assertNull(AzureConfig.parseDurationInMillis("maxIdleTime", "   "));
    }

    @Test
    public void testValidValue() throws Exception {

        Assert.assertEquals(AzureConfig.parseDurationInMillis("maxIdleTime", "60000"), Duration.ofMillis(60000));
        Assert.assertEquals(AzureConfig.parseDurationInMillis("maxIdleTime", " 1500 "), Duration.ofMillis(1500));
        Assert.assertEquals(AzureConfig.parseDurationInMillis("maxLifeTime", "1"), Duration.ofMillis(1));
    }

    @DataProvider(name = "invalidValues")
    public Object[][] invalidValues() {

        return new Object[][]{{"abc"}, {"0"}, {"-5"}, {"1.5"}, {"12abc"}, {"99999999999999999999"}};
    }

    @Test(dataProvider = "invalidValues")
    public void testInvalidValueIsRejected(String value) {

        try {
            AzureConfig.parseDurationInMillis("maxIdleTime", value);
            Assert.fail("Expected InvalidConfigurationException for value: " + value);
        } catch (InvalidConfigurationException e) {
            Assert.assertTrue(e.getMessage().contains("maxIdleTime"), e.getMessage());
        }
    }
}
