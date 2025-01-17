package com.dms.apps.vertx.common;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Launcher;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DmsVertxLauncher extends Launcher {

  @Override
  public void beforeStartingVertx(VertxOptions options) {
    log.info("1. beforeStartingVertx");
  }

  @Override
  public void afterStartingVertx(Vertx vertx) {
    log.info("2. afterStartingVertx");
  }

  @Override
  public void beforeDeployingVerticle(DeploymentOptions deploymentOptions) {
    log.info("3. beforeDeployingVerticle");
  }

  @Override
  public void beforeStoppingVertx(Vertx vertx) {
    log.info("4. beforeStoppingVertx");
  }
  
}
