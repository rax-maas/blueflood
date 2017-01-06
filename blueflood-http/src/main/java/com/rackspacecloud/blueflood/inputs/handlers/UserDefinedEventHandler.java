package com.rackspacecloud.blueflood.inputs.handlers;

import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.HttpConfig;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A generic handler to handle user triggered events. Currently this handler only
 * handles {@link IdleStateEvent} triggered by {@link IdleStateHandler}
 *
 */
public class UserDefinedEventHandler extends ChannelDuplexHandler {

    private static final Logger log = LoggerFactory.getLogger(UserDefinedEventHandler.class);

    private int HTTP_CONNECTION_READ_IDLE_TIME_SECONDS =
            Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_CONNECTION_READ_IDLE_TIME_SECONDS);

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

        if (evt instanceof IdleStateEvent) {

            IdleStateEvent e = (IdleStateEvent) evt;
            if (e.state() == IdleState.READER_IDLE) {

                log.info("Connection is closed as there is no inbound traffic for " +
                        HTTP_CONNECTION_READ_IDLE_TIME_SECONDS + " seconds. Connection: [" + ctx.channel().toString() + "]");
                ctx.close();
            }
        } else {
            log.warn("Unhandled event:" + evt + " on connection: " + ctx.channel().toString());
        }
    }
}
