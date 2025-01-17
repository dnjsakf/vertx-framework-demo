package com.dms.apps.vertx.common.api;

import io.vertx.core.AbstractVerticle;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SampleVerticle extends AbstractVerticle {

  @Override
  public void start() throws Exception {

    log.info(config().toString());

    vertx.createHttpServer().requestHandler(ctx -> {
      ctx.response().end("Hello, Vertx!!!");
    }).listen(8081, reulst -> {
      log.info("hi");
    });
  }
  
}
