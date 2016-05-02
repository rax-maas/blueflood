package com.rackspacecloud.blueflood;

import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A collection of static fields & methods which are useful in testing blueflood.  Most of these methods are involved with
 * generating data for the JSON API.
 */
public class TestUtils {

    public static final String ERROR_TITLE = "The following errors have been encountered:";
    public static final String PAST_COLLECTION_TIME_REGEX = ".* is more than '" + Configuration.getInstance().getLongProperty( CoreConfig.BEFORE_CURRENT_COLLECTIONTIME_MS ) + "' milliseconds into the past\\.$";
    public static final String FUTURE_COLLECTION_TIME_REGEX = ".* is more than '" + Configuration.getInstance().getLongProperty( CoreConfig.AFTER_CURRENT_COLLECTIONTIME_MS ) + "' milliseconds into the future\\.$";
    public static final String NO_TENANT_ID_REGEX = ".* No tenantId is provided for the metric\\.";

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String TIMESTAMP = "\"%TIMESTAMP%\"";
    private static final String POSTFIX = "%POSTFIX%";

    // making this just a static class, no instantiations
    private TestUtils() {}

    /**
     * Generate multi-tenant metrics data using:
     * <li>current timestamp
     * <li>tenant ids of tenantOne and tennatTwo
     * <li>random metric name postfix
     *
     * @return
     * @throws Exception
     */
    public static String generateMultitenantJSONMetricsData() throws Exception {
        long collectionTime = System.currentTimeMillis();

        List<Map<String, Object>> dataOut = new ArrayList<Map<String, Object>>();
        for (Map<String, Object> stringObjectMap : generateMetricsData( "", collectionTime )) {
            stringObjectMap.put("tenantId", "tenantOne");
            dataOut.add(stringObjectMap);
        }
        for (Map<String, Object> stringObjectMap : generateMetricsData( "", collectionTime )) {
            stringObjectMap.put("tenantId", "tenantTwo");
            dataOut.add(stringObjectMap);
        }

        return mapper.writeValueAsString(dataOut);
    }

    /**
     * Generate a single metric data using:
     * <li>current timestamp
     * <li>random metric name postfix
     *
     * @return
     * @throws Exception
     */
    public static String generateJSONMetricsData() throws Exception {
        return generateJSONMetricsData( System.currentTimeMillis() );
    }

    /**
     * Generate single metric data using:
     * <li> provided timestamp
     * <li> provided metric name postfix
     *
     * @param metricPostfix
     * @param collectionTime
     * @return
     * @throws Exception
     */
    public static String generateJSONMetricsData( String metricPostfix, long collectionTime ) throws Exception {

        StringWriter writer = new StringWriter();
        mapper.writeValue(writer, generateMetricsData( metricPostfix, collectionTime ));

        return writer.toString();
    }

    /**
     * Generate single metric data using:
     * <li> current timestamp
     * <li> provided metric name postfix
     *
     * @param metricPostfix
     * @return
     * @throws Exception
     */
    public static String generateJSONMetricsData( String metricPostfix ) throws Exception {
        return generateJSONMetricsData( metricPostfix, System.currentTimeMillis() );
    }

    /**
     * get raw json payload from file at payloadPath
     * @param payloadPath
     * @return String of json payload
     */
    public static String getJsonFromFile(String payloadPath) throws IOException {
        Reader reader = new InputStreamReader(TestUtils.class.getClassLoader().getResourceAsStream( payloadPath ) );
        StringWriter writer = new StringWriter();

        IOUtils.copy( reader, writer );
        IOUtils.closeQuietly( reader );

        return writer.toString();
    }

    /**
     * Given a payloadPath string pointing to a blueflood ingestion payload, replace all POSTFIX patterns with:
     * <li> current timestamp
     * <li> provided metric name postfix
     *
     * @param payloadPath
     * @param postfix
     * @return json string of payload with current TIMESTAMP & POSTFIX patterns replaced
     * @throws IOException
     */
    public static String getJsonFromFile(String payloadPath, String postfix ) throws IOException {
        return getJsonFromFile(payloadPath, System.currentTimeMillis(), postfix);
    }

    /**
     * Given a payloadPath string pointing to a blueflood ingestion payload, replace all TIMESTAMP & POSTFIX patterns with:
     * <li> provided timestamp
     * <li> provided metric name postfix
     *
     * @param payloadPath
     * @param timestamp
     * @param postfix
     * @return json string of payload with TIMESTAMP & POSTFIX patterns replaced
     * @throws IOException
     */
    public static String getJsonFromFile(String payloadPath, long timestamp, String postfix ) throws IOException {
        String json = getJsonFromFile(payloadPath);

        // update timestamp
        json = updateTimeStampJson(json, TIMESTAMP, timestamp);

        // append random number to metric name
        json = json.replace( POSTFIX, postfix );

        return json;
    }

    public static String updateTimeStampJson(String json, String timestampName, long timestampMillis) {
        // JSON might have several entries for the same metric.  If they have the same timestamp, they could overwrite
        // each other in the case of enums.  Not using sleep() here to increment the time as to not have to deal with
        // interruptedexception.  Rather, incrementing the time by 1 ms.
        long increment = 0;

        while( json.contains( timestampName ) ) {

            json = json.replaceFirst( timestampName, Long.toString( timestampMillis + increment++ ) );
        }
        return json;
    }

    /**
     * Generate single metric data using:
     * <li> provided timestamp
     * <li> random generated metric name postfix
     *
     * @param collectionTime
     * @return
     * @throws Exception
     */
    private static String generateJSONMetricsData( long collectionTime ) throws Exception {

        StringWriter writer = new StringWriter();
        mapper.writeValue(writer, generateMetricsData( "", collectionTime ));

        return writer.toString();
    }

    /**
     * Returns a list of list of maps which represent metrics, each metric uses:
     * <li> provided timestamp
     * <li> metric name postfix
     *
     * @param metricPostfix
     * @param collectionTime
     * @return
     */
    private static List<Map<String, Object>> generateMetricsData( String metricPostfix, long collectionTime ) {

        List<Map<String, Object>> metricsList = new ArrayList<Map<String, Object>>();

        // Long metric value
        Map<String, Object> testMetric = new TreeMap<String, Object>();
        testMetric.put("metricName", "mzord.duration" + metricPostfix );
        testMetric.put("ttlInSeconds", 1234566);
        testMetric.put("unit", "milliseconds");
        testMetric.put("metricValue", Long.MAX_VALUE);
        testMetric.put("collectionTime", collectionTime );
        metricsList.add(testMetric);

        // String metric value
        testMetric = new TreeMap<String, Object>();
        testMetric.put("metricName", "mzord.status" + metricPostfix );
        testMetric.put("ttlInSeconds", 1234566);
        testMetric.put("unit", "unknown");
        testMetric.put("metricValue", "Website is up");
        testMetric.put("collectionTime", collectionTime );
        metricsList.add(testMetric);

        // null metric value. This shouldn't be in the final list of metrics because we ignore null valued metrics.
        testMetric = new TreeMap<String, Object>();
        testMetric.put("metricName", "mzord.hipster" + metricPostfix );
        testMetric.put("ttlInSeconds", 1234566);
        testMetric.put("unit", "unknown");
        testMetric.put("metricValue", null);
        testMetric.put("collectionTime", collectionTime );
        metricsList.add(testMetric);

        return metricsList;

    }
}
