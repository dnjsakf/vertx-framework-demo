package com.dms.apps.vertx.core.controller;

import java.util.Map;

import com.dms.apps.vertx.core.annotations.DmsInject;
import com.dms.apps.vertx.core.annotations.DmsService;
import com.dms.apps.vertx.core.utils.DmsDBClient;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;

@DmsService
public class SampleService {

    @DmsInject
    private DmsDBClient dbClient;

    public Future<JsonArray> pingPong(Map<String, Object> params){
        return dbClient.execute("SELECT 'PONG' AS PING_PONG", params);        
    }
}
