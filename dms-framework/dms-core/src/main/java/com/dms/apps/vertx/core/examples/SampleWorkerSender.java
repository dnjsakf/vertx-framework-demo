package com.dms.apps.vertx.core.examples;

import com.dms.apps.vertx.core.abs.DmsAbstractWorker;
import com.dms.apps.vertx.core.annotations.DmsInject;
import com.dms.apps.vertx.core.annotations.DmsSubscribe;
import com.dms.apps.vertx.core.utils.DmsRedisClient;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Response;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@DmsSubscribe("sample.data")
public class SampleWorkerSender extends DmsAbstractWorker {

    @DmsInject
    private DmsRedisClient client;

    @Override
    public void start() throws Exception {
        final String redisKey = getMessage();

        JsonObject redisConfig = config().getJsonObject("redis");
        client = new DmsRedisClient(vertx, redisConfig);
        client.createApi().onSuccess(redisAPI -> {
            // vertx.setPeriodic(1000, id -> {
            //     redisAPI.rpush(redisKey, new JsonObject().put("ping", "pong").encode())
            //         .onSuccess(resp -> {
            //             handler("solo", resp);
            //         })
            //         .onFailure(t -> {
            //             t.printStackTrace();
            //         });
            // });
            vertx.eventBus().consumer(redisKey, message -> {
                String sendData = new JsonObject()
                    .put("key", redisKey)
                    .put("data", message.body())
                    .encode();

                redisAPI.send(Command.RPUSH, redisKey, sendData)
                    .onSuccess(resp -> {
                        handler("solo", resp);
                    })
                    .onFailure(resp -> {
                        resp.printStackTrace();
                    })
                    .onComplete(resp -> {
                        message.reply("done");
                    });
            });
        }).onFailure(t -> {
            t.printStackTrace();
        });
    }

    @Override
    public Future<Void> handler(String workerId, Response response) {
        Promise<Void> promise = Promise.promise();
        promise.complete();
        return promise.future();
    }
    
}
