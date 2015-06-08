package com.rackspacecloud.blueflood.utils;

import com.rackspacecloud.blueflood.io.GenericElasticSearchIO;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class EventModuleLoader {
    private static final Logger log = LoggerFactory.getLogger(EventModuleLoader.class);
    private static GenericElasticSearchIO instance = null;

    public static GenericElasticSearchIO getInstance() {
        if (instance == null) {
            loadEventModule();
        }

        return instance;
    }

    public static synchronized void loadEventModule() {
        List<String> modules = Configuration.getInstance().getListProperty(CoreConfig.EVENTS_MODULES);

        if (modules.isEmpty() || instance != null)
            return;

        ClassLoader classLoader = GenericElasticSearchIO.class.getClassLoader();
        for (String module : modules) {
            log.info("Loading metric event module " + module);
            try {
                Class discoveryClass = classLoader.loadClass(module);
                instance = (GenericElasticSearchIO) discoveryClass.newInstance();
                log.info("Registering metric event module " + module);
            } catch (InstantiationException e) {
                log.error("Unable to create instance of metric event class for: " + module, e);
            } catch (IllegalAccessException e) {
                log.error("Error starting metric event module: " + module, e);
            } catch (ClassNotFoundException e) {
                log.error("Unable to locate metric event module: " + module, e);
            } catch (RuntimeException e) {
                log.error("Error starting metric event module: " + module, e);
            } catch (Throwable e) {
                log.error("Error starting metric event module: " + module, e);
            }
        }
    }
}
