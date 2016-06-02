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

import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.io.astyanax.AEnumIO;
import com.rackspacecloud.blueflood.io.astyanax.AstyanaxIO;
import com.rackspacecloud.blueflood.io.astyanax.AstyanaxReader;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@PowerMockIgnore({"javax.management.*", "com.rackspacecloud.blueflood.utils.Metrics", "com.codahale.metrics.*"})
@PrepareForTest({ IOContainer.class, AstyanaxIO.class, AstyanaxReader.class, ThreadPoolBuilder.class })
@RunWith(PowerMockRunner.class)
@SuppressStaticInitializationFor( {
        "com.rackspacecloud.blueflood.io.astyanax.AstyanaxIO",
        "com.rackspacecloud.blueflood.io.astyanax.AstyanaxReader",
        "com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder"
} )
public class EnumValidatorTest {

    private EnumReaderIO readerMock;
    private DiscoveryIO discoveryIOMock;
    private ExcessEnumIO excessEnumIO;

    private final String tenant_id = "tenant1";
    private final String metric_name = "metric1";
    private HashSet<Locator> locators;
    Locator locator1;

    @Before
    public void setup() {

        // static mock
        readerMock = mock(AEnumIO.class);
        discoveryIOMock = mock(DiscoveryIO.class);
        excessEnumIO = mock(ExcessEnumIO.class);

        //PowerMockito.mockStatic(AstyanaxReader.class);
        //when(AstyanaxReader.getInstance()).thenReturn(readerMock);
        PowerMockito.mockStatic(IOContainer.class);
        IOContainer ioContainer = mock(IOContainer.class);
        when(IOContainer.fromConfig()).thenReturn(ioContainer);
        when(ioContainer.getExcessEnumIO()).thenReturn(excessEnumIO);

        // set threshold
        System.setProperty(CoreConfig.ENUM_UNIQUE_VALUES_THRESHOLD.name(), "3");

        // set locators
        locator1 = Locator.createLocatorFromPathComponents(tenant_id, metric_name);
        locators = new HashSet<Locator>();
        locators.add(locator1);
    }

    @After
    public void tearDown() {
        System.clearProperty(CoreConfig.ENUM_UNIQUE_VALUES_THRESHOLD.name());
    }

    private EnumValidator setupEnumValidatorWithMock(Set<Locator> locators) {
        EnumValidator enumValidator = new EnumValidator(locators, readerMock);
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
        verify(excessEnumIO, never()).insertExcessEnumMetric(locator1);
        verify(discoveryIOMock, never()).search(tenant_id, metric_name);
        verify(discoveryIOMock, never()).insertDiscovery(any(IMetric.class));
    }

    @Test
    public void testEmptyLocatorList() throws Exception {
        // mock returned empty list of enum hash values from cassandra
        Map<Locator, List<String>> locatorEnumsMock = new HashMap<Locator, List<String>>();
        when(readerMock.getEnumStringMappings(anyList())).thenReturn(locatorEnumsMock);

        // execute validator with locators
        EnumValidator validator = setupEnumValidatorWithMock(locators);
        validator.run();

        // verify no writes to cassandra and elasticsearch
        verify(readerMock, times(1)).getEnumStringMappings(anyList());
        verify(excessEnumIO, never()).insertExcessEnumMetric(locator1);
        verify(discoveryIOMock, never()).search(tenant_id, metric_name);
        verify(discoveryIOMock, never()).insertDiscovery(any(IMetric.class));
    }

    @Test
    public void testEnumsExceedThreshold() throws Exception {
        // mock returned list of enum hash values from cassandra
        Map<Locator, List<String>> locatorEnumsMock = new HashMap<Locator, List<String>>();
        List<String> enumValues = new ArrayList<String>();
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
        verify(excessEnumIO, times(1)).insertExcessEnumMetric(locator1);
        verify(discoveryIOMock, never()).search(tenant_id, metric_name);
        verify(discoveryIOMock, never()).insertDiscovery(any(IMetric.class));
    }

    @Test
    public void testEnumsValidatedWithSameValues() throws Exception {
        // mock returned list of enum hash values from cassandra
        Map<Locator, List<String>> locatorEnumsMock = new HashMap<Locator, List<String>>();
        List<String> enumValues = new ArrayList<String>();
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
        verify(excessEnumIO, never()).insertExcessEnumMetric(locator1);
        verify(discoveryIOMock, times(1)).search(tenant_id, metric_name);
        verify(discoveryIOMock, never()).insertDiscovery(any(IMetric.class));
    }

    @Test
    public void testEnumsValidatedWithNewValues() throws Exception {
        // mock returned list of enum hash values from cassandra
        Map<Locator, List<String>> locatorEnumsMock = new HashMap<Locator, List<String>>();
        List<String> enumValues = new ArrayList<String>();
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
        verify(excessEnumIO, never()).insertExcessEnumMetric(locator1);
        verify(discoveryIOMock, times(1)).search(tenant_id, metric_name);
        verify(discoveryIOMock, times(1)).insertDiscovery(any(IMetric.class));
    }

    @Test
    public void getReaderUninitializedReturnsDefaultInstance() {

        // given
        EnumValidator validator = new EnumValidator(null);

        // expect
        assertSame(IOContainer.fromConfig().getEnumReaderIO(), validator.getEnumIO());
    }

    @Test
    public void getDiscoveryIOUninitializedReturnsNull() {

        // given
        EnumValidator validator = new EnumValidator(null);

        // expect
        assertNull(validator.getDiscoveryIO());
    }

    @Test
    public void setReaderSetsReader() {

        // given
        EnumValidator validator = new EnumValidator(null, readerMock);

        // expect
        assertSame(readerMock, validator.getEnumIO());
    }

    @Test
    public void setDiscoveryIOSetsDiscoveryIO() {

        // given
        EnumValidator validator = new EnumValidator(null);

        // precondition
        assertNull(validator.getDiscoveryIO());

        // when
        validator.setDiscoveryIO(discoveryIOMock);

        // then
        assertSame(discoveryIOMock, validator.getDiscoveryIO());
    }
}
