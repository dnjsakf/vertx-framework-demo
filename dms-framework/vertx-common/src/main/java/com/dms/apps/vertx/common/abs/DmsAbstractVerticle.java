package com.dms.apps.vertx.common.abs;

import io.vertx.core.AbstractVerticle;
import io.vertx.sqlclient.Pool;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class DmsAbstractVerticle extends AbstractVerticle {

    private Pool pool;

    public void setPool(Pool pool){
        this.pool = pool;
    }
    public Pool pool(){
        return this.pool;
    }

}
