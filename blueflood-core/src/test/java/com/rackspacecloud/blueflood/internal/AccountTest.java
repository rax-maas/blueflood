package com.rackspacecloud.blueflood.internal;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class AccountTest {
    private static final String ackVCKg1rk =
    "{" +
    "    \"id\": \"ackVCKg1rk\"," +
    "    \"external_id\": \"5821004\"," +
    "    \"metadata\": {}," +
    "    \"min_check_interval\": 30," +
    "    \"webhook_token\": \"CnXGHWj14H1koKjY8x5hFHPt5vSAAHTB\"," +
    "    \"account_status\": \"GOOD\"," +
    "    \"rackspace_managed\": false," +
    "    \"cep_group_id\": \"cgA\"," +
    "    \"api_rate_limits\": {" +
    "        \"global\": 50000," +
    "        \"test_check\": 500," +
    "        \"test_alarm\": 500," +
    "        \"test_notification\": 200," +
    "        \"traceroute\": 300" +
    "    }," +
    "    \"limits\": {" +
    "        \"checks\": 10000," +
    "        \"alarms\": 10000" +
    "    }," +
    "    \"features\": {" +
    "        \"agent\": true," +
    "        \"rollups\": true" +
    "    }," +
    "    \"agent_bundle_channel\": \"stable\"," +
    "    \"metrics_ttl\": {" +
    "        \"full\": 1," +
    "        \"5m\": 2," +
    "        \"20m\": 3," +
    "        \"60m\": 4," +
    "        \"240m\": 5," +
    "        \"1440m\": 6" +
    "    }" +
    "}";
    
    private static final String acAAAAAAAA =
    "{" +
    "    \"id\": \"acAAAAAAAA\"," +
    "    \"external_id\": \"12345\"," +
    "    \"metadata\": {}," +
    "    \"min_check_interval\": 30," +
    "    \"webhook_token\": \"CnXGHWj14H1koKjY8x5hFHPt5vSAAHTB\"," +
    "    \"account_status\": \"GOOD\"," +
    "    \"rackspace_managed\": false," +
    "    \"cep_group_id\": \"cgA\"," +
    "    \"api_rate_limits\": {" +
    "        \"global\": 50000," +
    "        \"test_check\": 500," +
    "        \"test_alarm\": 500," +
    "        \"test_notification\": 200," +
    "        \"traceroute\": 300" +
    "    }," +
    "    \"limits\": {" +
    "        \"checks\": 10000," +
    "        \"alarms\": 10000" +
    "    }," +
    "    \"features\": {" +
    "        \"agent\": true," +
    "        \"rollups\": true" +
    "    }," +
    "    \"agent_bundle_channel\": \"stable\"," +
    "    \"metrics_ttl\": {" +
    "        \"full\": 7," +
    "        \"5m\": 8," +
    "        \"20m\": 9," +
    "        \"60m\": 10," +
    "        \"240m\": 11," +
    "        \"1440m\": 12" +
    "    }" +
    "}";
    
    public static final Map<String, String> JSON_ACCOUNTS = Collections.unmodifiableMap(new HashMap<String, String>() {{
        put("ackVCKg1rk", ackVCKg1rk);
        put("acAAAAAAAA", acAAAAAAAA);
    }}); 
    
    @Test(expected = UnsupportedOperationException.class)
    public void ensureUnmodifiableFixtures() {
        JSON_ACCOUNTS.put("foo", "bar");
    }
    
    @Test
    public void testNormalJsonConstruction() {
        Account acct = Account.fromJSON(ackVCKg1rk);
        
        // not default.
        Assert.assertEquals(3, acct.getMetricTtl("20m").toHours());
        
        // not present in json, but should be applied by default.
        Assert.assertNotNull(acct.getMetricTtl("240m"));
        
        Assert.assertEquals("ackVCKg1rk", acct.getId());
    }
}
