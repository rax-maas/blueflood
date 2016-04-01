/*
 * Copyright 2015 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.io.astyanax.AstyanaxReader;
import com.rackspacecloud.blueflood.io.astyanax.AstyanaxWriter;
import com.rackspacecloud.blueflood.io.DiscoveryIO;
import com.rackspacecloud.blueflood.io.SearchResult;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.*;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class EnumValidatorTest {

    AstyanaxReader readerMock = mock(AstyanaxReader.class);
    AstyanaxWriter writerMock = mock(AstyanaxWriter.class);
    DiscoveryIO discoveryIOMock = mock(DiscoveryIO.class);

    String tenant_id = "tenant1";
    String metric_name = "metric1";
    Locator locator1 = Locator.createLocatorFromPathComponents(tenant_id, metric_name);
    HashSet<Locator> locators;

    @Before
    public void setup() {
        // set threshold
        System.setProperty(CoreConfig.ENUM_UNIQUE_VALUES_THRESHOLD.name(), "3");

        // set locators
        locators = new HashSet<Locator>();
        locators.add(locator1);
    }

    @After
    public void tearDown() {
        System.clearProperty(CoreConfig.ENUM_UNIQUE_VALUES_THRESHOLD.name());
    }

    private EnumValidator setupEnumValidatorWithMock(Set<Locator> locators) {
        EnumValidator enumValidator = new EnumValidator(locators);
        enumValidator.setReader(readerMock);
        enumValidator.setWriter(writerMock);
        enumValidator.setDiscoveryIO(discoveryIOMock);
        return enumValidator;
    }

    @Test
    public void testNullLocators() throws Exception {
        // create EnumValidator with locators set = null
        EnumValidator validator = setupEnumValidatorWithMock(null);

        // execute validator
        validator.run();

        // verify no writes to cassandra and elasticsearch
        verify(readerMock, never()).getEnumStringMappings(anyList());
        verify(writerMock, never()).writeExcessEnumMetric(locator1);
        verify(discoveryIOMock, never()).search(tenant_id, metric_name);
        verify(discoveryIOMock, never()).insertDiscovery(any(IMetric.class));
    }

    @Test
    public void testEmptyLocatorList() throws Exception {
        // mock returned empty list of enum hash values from cassandra
        Map<Locator, ArrayList<String>> locatorEnumsMock = new HashMap<Locator, ArrayList<String>>();
        when(readerMock.getEnumStringMappings(anyList())).thenReturn(locatorEnumsMock);

        // execute validator with locators
        EnumValidator validator = setupEnumValidatorWithMock(locators);
        validator.run();

        // verify no writes to cassandra and elasticsearch
        verify(readerMock, times(1)).getEnumStringMappings(anyList());
        verify(writerMock, never()).writeExcessEnumMetric(locator1);
        verify(discoveryIOMock, never()).search(tenant_id, metric_name);
        verify(discoveryIOMock, never()).insertDiscovery(any(IMetric.class));
    }

    @Test
    public void testEnumsExceedThreshold() throws Exception {
        // mock returned list of enum hash values from cassandra
        Map<Locator, ArrayList<String>> locatorEnumsMock = new HashMap<Locator, ArrayList<String>>();
        ArrayList<String> enumValues = new ArrayList<String>();
        enumValues.add("value1");
        enumValues.add("value2");
        enumValues.add("value3");
        enumValues.add("value4");
        locatorEnumsMock.put(locator1, enumValues);
        when(readerMock.getEnumStringMappings(anyList())).thenReturn(locatorEnumsMock);

        // execute validator with locators
        EnumValidator validator = setupEnumValidatorWithMock(locators);
        validator.run();

        // verify writes to CF_METRICS_EXCESS_ENUMS and no writes elasticsearch
        verify(writerMock, times(1)).writeExcessEnumMetric(locator1);
        verify(discoveryIOMock, never()).search(tenant_id, metric_name);
        verify(discoveryIOMock, never()).insertDiscovery(any(IMetric.class));
    }

    @Test
    public void testEnumsValidatedWithSameValues() throws Exception {
        // mock returned list of enum hash values from cassandra
        Map<Locator, ArrayList<String>> locatorEnumsMock = new HashMap<Locator, ArrayList<String>>();
        ArrayList<String> enumValues = new ArrayList<String>();
        enumValues.add("value1");
        enumValues.add("value2");
        enumValues.add("value3");
        locatorEnumsMock.put(locator1, enumValues);
        when(readerMock.getEnumStringMappings(anyList())).thenReturn(locatorEnumsMock);

        List<SearchResult> esSearchResultMock = new ArrayList<SearchResult>();
        SearchResult result = new SearchResult(tenant_id, metric_name, null, enumValues);
        esSearchResultMock.add(result);
        when(discoveryIOMock.search(tenant_id, metric_name)).thenReturn(esSearchResultMock);

        // execute validator with locators
        EnumValidator validator = setupEnumValidatorWithMock(locators);
        validator.run();

        // verify no writes to CF_METRICS_EXCESS_ENUMS and no writes elasticsearch
        // because CF and ES enum values are equals
        verify(writerMock, never()).writeExcessEnumMetric(locator1);
        verify(discoveryIOMock, times(1)).search(tenant_id, metric_name);
        verify(discoveryIOMock, never()).insertDiscovery(any(IMetric.class));
    }

    @Test
    public void testEnumsValidatedWithNewValues() throws Exception {
        // mock returned list of enum hash values from cassandra
        Map<Locator, ArrayList<String>> locatorEnumsMock = new HashMap<Locator, ArrayList<String>>();
        ArrayList<String> enumValues = new ArrayList<String>();
        enumValues.add("value1");
        enumValues.add("value2");
        enumValues.add("value3");
        locatorEnumsMock.put(locator1, enumValues);
        when(readerMock.getEnumStringMappings(anyList())).thenReturn(locatorEnumsMock);

        List<SearchResult> esSearchResultMock = new ArrayList<SearchResult>();
        SearchResult result = new SearchResult(tenant_id, metric_name, null, null);
        esSearchResultMock.add(result);
        when(discoveryIOMock.search(tenant_id, metric_name)).thenReturn(esSearchResultMock);

        // execute validator with locators
        EnumValidator validator = setupEnumValidatorWithMock(locators);
        validator.run();

        // verify no writes to CF_METRICS_EXCESS_ENUMS and writes elasticsearch with new indexes
        verify(writerMock, never()).writeExcessEnumMetric(locator1);
        verify(discoveryIOMock, times(1)).search(tenant_id, metric_name);
        verify(discoveryIOMock, times(1)).insertDiscovery(any(IMetric.class));
    }
}
