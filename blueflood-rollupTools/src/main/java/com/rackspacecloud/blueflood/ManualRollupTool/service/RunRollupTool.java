/*
 * Copyright 2014 Rackspace
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
package com.rackspacecloud.blueflood.ManualRollupTool.service;

import com.rackspacecloud.blueflood.ManualRollupTool.io.ManualRollup;
import com.rackspacecloud.blueflood.service.Configuration;

public class RunRollupTool {

    public static void main(String args[]) {
        Configuration conf = Configuration.getInstance();
        String newPrefix = conf.getStringProperty("GRAPHITE_HOST") + ".rollupTool";
        System.setProperty(conf.getStringProperty("GRAPHITE_HOST").toString(), newPrefix);
        ManualRollup rollupTool = new ManualRollup();
        rollupTool.startManualRollup();
    }
}
