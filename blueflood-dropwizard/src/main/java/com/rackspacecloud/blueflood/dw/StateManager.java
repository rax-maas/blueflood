package com.rackspacecloud.blueflood.dw;

import com.rackspacecloud.blueflood.service.ScheduleContext;
import com.rackspacecloud.blueflood.service.ShardStateServices;
import io.dropwizard.lifecycle.Managed;

public class StateManager implements Managed {
    private ShardStateServices services;
    
    public StateManager(ScheduleContext context) {
        services = new ShardStateServices(context);
    }
    
    @Override
    public void start() throws Exception {
        services.start();
    }

    @Override
    public void stop() throws Exception {
        services.stop();
    }
}
