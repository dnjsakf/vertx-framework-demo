package com.dms.apps.vertx.core;

import io.vertx.core.AbstractVerticle;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DmsVertxEventBus extends AbstractVerticle {

    @Override
    public void start() throws Exception {
        // EventBus Verticle 배포
        vertx.eventBus().consumer("eventbus.ping", message -> {
            log.info("Received message: " + message.body());
            message.reply("pong");
        });
    }

}
