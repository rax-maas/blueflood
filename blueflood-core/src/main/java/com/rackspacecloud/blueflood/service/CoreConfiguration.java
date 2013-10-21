package com.rackspacecloud.blueflood.service;

/**
 * This is the configuration singleton for blueflood-core.
 * It has default values for all blueflood-core settings.
 * resources/blueflood.properties contains default values of all settings
 */
public class CoreConfiguration extends Configuration {
    private static String defaultPropFileName = "blueflood.properties";
    private static final CoreConfiguration INSTANCE = new CoreConfiguration();
    private CoreConfiguration() {
        super(defaultPropFileName);
    }
    public static CoreConfiguration getInstance() {
        return INSTANCE;
    }
}
