package com.dms.apps.vertx.core.abs;

import java.util.concurrent.atomic.AtomicBoolean;

import com.dms.apps.vertx.core.annotations.DmsInject;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisConnection;
import io.vertx.redis.client.RedisOptions;
import io.vertx.sqlclient.Pool;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class DmsAbstractVerticle extends AbstractVerticle {


}
