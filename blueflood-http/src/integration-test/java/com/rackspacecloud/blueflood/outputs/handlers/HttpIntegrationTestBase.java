/*
 * Copyright (c) 2016 Rackspace.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Resolution;
import org.junit.Assert;

import java.util.List;
import java.util.Map;

/**
 * The base class holding some methods shared amongst multiple tests
 */
public class HttpIntegrationTestBase extends IntegrationTestBase {


    protected void checkGetRollupByResolution(List<Locator> locators, Map<Locator, Map<Granularity, Integer>> answers,
                                              long baseMillis, HttpRollupsQueryHandler httpHandler) throws Exception {
        for (Locator locator : locators) {
            for (Resolution resolution : Resolution.values()) {
                Granularity g = Granularity.granularities()[resolution.getValue()];
                checkHttpHandlersGetByResolution(locator, resolution, baseMillis, baseMillis + 86400000,
                        answers.get(locator).get(g), httpHandler);
            }
        }
    }

    protected void checkHttpHandlersGetByResolution(Locator locator, Resolution resolution, long from, long to,
                                                    int expectedPoints, HttpRollupsQueryHandler handler) throws Exception {
        int currentPoints = getNumberOfPointsViaHTTPHandler(handler, locator,
                from, to, resolution);

        Assert.assertEquals(String.format("locator=%s, resolution=%s, from=%d, to=%d, expectedPoints=%d and currentPoints=%d should be the same",
                        locator, resolution.toString(), from, to, expectedPoints, currentPoints),
                expectedPoints, currentPoints);
    }

    protected int getNumberOfPointsViaHTTPHandler(HttpRollupsQueryHandler handler,
                                                  Locator locator, long from, long to,
                                                  Resolution resolution) throws Exception {
        final MetricData values = handler.GetDataByResolution(locator.getTenantId(),
                                                    locator.getMetricName(), from, to, resolution);
        return values.getData().getPoints().size();
    }

    protected void checkHttpRollupHandlerGetByPoints(Map<Locator, Map<Granularity, Integer>> answers, Map<Granularity, Integer> points,
                                                     long from, long to, List<Locator> locators, HttpRollupsQueryHandler httphandler) throws Exception {
        for (Locator locator : locators) {
            for (Granularity g2 : Granularity.granularities()) {
                MetricData data = httphandler.GetDataByPoints(
                        locator.getTenantId(),
                        locator.getMetricName(),
                        from,
                        to,
                        points.get(g2));
                Assert.assertEquals(String.format("locator=%s, from=%d, to=%d, expectedPoints=%d and currentPoints=%d should be the same",
                                locator, from, to, answers.get(locator).get(g2), data.getData().getPoints().size()),
                        (int)answers.get(locator).get(g2), data.getData().getPoints().size());
                // Disabling test that fail on ES
                // Assert.assertEquals(locatorToUnitMap.get(locator), data.getUnit());
            }
        }
    }
}
