package com.rackspacecloud.blueflood.utils;

/**
 * Marks a module as being qualified to aid in its loading. See {@link ModuleLoader} for more details.
 */
public interface QualifiedModule {
    String getQualifier();
}
