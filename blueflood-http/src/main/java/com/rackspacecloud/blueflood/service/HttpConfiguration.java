package com.rackspacecloud.blueflood.service;

/**
 * This is the configuration singleton for blueflood-http.
 * It has default values for all blueflood-http settings.
 * resources/blueflood-http.properties contains default values of all settings
 */
public class HttpConfiguration extends Configuration {
    private static final String defaultPropFileName = "blueflood-http.properties";
    private static final HttpConfiguration INSTANCE = new HttpConfiguration();
    private HttpConfiguration() {
        super(defaultPropFileName);
    }
    public static HttpConfiguration getInstance() {
        return INSTANCE;
    }
}
