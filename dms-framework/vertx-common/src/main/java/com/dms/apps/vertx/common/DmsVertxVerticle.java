package com.dms.apps.vertx.common;

import io.vertx.core.AbstractVerticle;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DmsVertxVerticle extends AbstractVerticle {
  
  @Override
  public void start() throws Exception {
    log.info("DmsVertxVerticle: {}", config().toString());
  }

}
