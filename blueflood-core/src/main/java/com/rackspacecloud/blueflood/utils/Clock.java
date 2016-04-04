package com.rackspacecloud.blueflood.utils;

import org.joda.time.Instant;

/**
 * Created this class to allow alternate clocks to be plugged in
 * with dependency injection.
 *
 * java 8 has java.time.Clock which was created for similar purpose.
 * When we migrate to java 8, we can remove this class.
 */
public interface Clock {

    /**
     * Obtains the current instant from the system clock.
     */
    Instant now();
}
