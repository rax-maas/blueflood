package com.rackspacecloud.blueflood.tools.ops;

import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.io.*;
import org.slf4j.Logger;
import com.netflix.astyanax.model.*;
import org.slf4j.LoggerFactory;
import java.util.*;

public class FixTTL {
  private static final Logger log = LoggerFactory.getLogger(FixTTL.class);
  public static Map<Locator, ColumnList<Long>> getColumnDataForTenant(String tenantID)
    {
        ColumnFamily CF = CassandraModel.CF_METRICS_FULL;
        long sevenDays = 1000 * 60 * 60 * 24 * 7;
        long now = System.currentTimeMillis();
        Range range = new Range(now - sevenDays,now);
        String metrics[] = {"rackspace.example.metric.one"};
        List<Locator> locators = new ArrayList<Locator>();
        for (String path: metrics) {
            locators.add(Locator.createLocatorFromPathComponents(tenantID, path));
        }
        Map<Locator, ColumnList<Long>> cols = AstyanaxReader.getInstance().getColumnsFromDB(locators, CF, range);
        for (Map.Entry<Locator, ColumnList<Long>> entry: cols.entrySet()) {
            log.info("gbjcolnext " + entry.getKey().toString());
	    
        }
        return cols;
    }

    public static void main(String args[]) {
              log.info("gbjgetting cols2");

        getColumnDataForTenant("835990");
    }
}
