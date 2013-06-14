package com.cloudkick.blueflood.stress;

import com.cloudkick.blueflood.service.Configuration;
import com.cloudkick.blueflood.service.RollupServiceMBean;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class RealtimeBlaster {
    private static final Random random = new Random(System.currentTimeMillis());
    
    public static void main(String args[]) {
        try {
            Configuration.init();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        
        for (String resource : System.getProperty("BLUEFLOOD_JMX_LIST").split(",", -1)) {
            try {
                RollupServiceMBean bean = Blaster.getRollupService(resource);
                bean.setKeepingServerTime(true);
                bean.setPollerPeriod(1000L);
                bean.setServerTime(System.currentTimeMillis());
            } catch (Throwable mostlyExpected) {
                System.out.println(mostlyExpected.getMessage());
            }
        }
        
        final ArrayList<Blaster2.PushThread> pushers = new ArrayList<Blaster2.PushThread>();
        final ArrayList<Blaster2.TelescopeGenerator> telescopes = new ArrayList<Blaster2.TelescopeGenerator>();
        
        final Blaster2 bean = new Blaster2(telescopes, pushers);
        final Thread thread = new Thread(bean, "Blaster");
        thread.start();
        
    }
}
