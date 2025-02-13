package com.dms.apps.flume.sink;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FlumeJDBCSink extends AbstractJDBCSink {

    @Override
    protected int processJDBC(JsonObject body) throws Exception {
        statement.setString(1, body.get("data").getAsString());
        statement.setObject(2, new GsonBuilder().create().toJson(body));
        return 1;
    }
}