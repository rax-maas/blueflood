package com.rackspacecloud.blueflood.utils;

import com.rackspacecloud.blueflood.io.DiscoveryIO;
import com.rackspacecloud.blueflood.outputs.handlers.RollupHandler;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.sun.org.apache.xpath.internal.operations.Mod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ModuleLoader {
    private static final Logger log = LoggerFactory.getLogger(RollupHandler.class);
    private static ModuleLoader instance;

    public static ModuleLoader getInstance() {
        if (instance == null) {
            instance = new ModuleLoader();
        }
        return instance;
    }

    public DiscoveryIO loadElasticIOModule() {
        List<String> modules = Configuration.getInstance().getListProperty(CoreConfig.DISCOVERY_MODULES);

        if (!modules.isEmpty() && modules.size() != 1) {
            throw new RuntimeException("Cannot load query service with more than one discovery module");
        }

        ClassLoader classLoader = DiscoveryIO.class.getClassLoader();
        for (String module : modules) {
            log.info("Loading metric discovery module " + module);
            try {
                Class discoveryClass = classLoader.loadClass(module);
                log.info("Registering metric discovery module " + module);
                return (DiscoveryIO) discoveryClass.newInstance();
            } catch (InstantiationException e) {
                log.error("Unable to create instance of metric discovery class for: " + module, e);
            } catch (IllegalAccessException e) {
                log.error("Error starting metric discovery module: " + module, e);
            } catch (ClassNotFoundException e) {
                log.error("Unable to locate metric discovery module: " + module, e);
            } catch (RuntimeException e) {
                log.error("Error starting metric discovery module: " + module, e);
            } catch (Throwable e) {
                log.error("Error starting metric discovery module: " + module, e);
            }
        }

        return null;
    }

}
