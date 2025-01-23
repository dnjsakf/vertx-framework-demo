package com.dms.apps.vertx.core.abs;

import com.dms.apps.vertx.core.utils.DmsRedisClient;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class DmsAbstractWorker extends DmsAbstractBase {

    private String message;
    private Integer workers;

    public void setMessage(String message){
        this.message = message;
    }
    public void setWorkers(Integer workers){
        this.workers = workers;
    }
    public String getMessage(){
        return this.message;
    }
    public Integer getWorkers(){
        return this.workers;
    }

    @Override
    public void start() throws Exception {
        log.info("message:{}, workers:{}", message, workers);
        // 멀티쓰레딩
        for (int i = 0; i < workers; i++) {
            final int workerIndex = i;
            log.debug("Start Worker : {} : {}", Thread.currentThread().getId(), workerIndex);

            JsonObject redisConfig = config().getJsonObject("redis");
            DmsRedisClient client = new DmsRedisClient(vertx, redisConfig);
            client.createApi()
                .onSuccess(redisAPI -> {
                    vertx.executeBlocking(() -> {
                        vertx.setPeriodic(100, id -> {
                            redisAPI.send(Command.LPOP, message)
                                .compose(resp -> {
                                    log.debug("[{}] Before Listener", workerIndex);
                                    Future<Void> future = handler(String.valueOf(workerIndex), resp);
                                    return future;
                                })
                                .onSuccess(resp -> {
                                    log.debug("[{}] After Listener", workerIndex);
                                })
                                .onFailure(resp -> {
                                    log.debug("[{}] Error Listener", workerIndex);
                                })
                                .onComplete(resp -> {
                                    log.debug("[{}] Complete Listener", workerIndex);
                                });
                        });
                        return Future.succeededFuture();
                    }, false);
                })
                .onFailure(t -> {
                    t.printStackTrace();
                });
        }
    }

    public abstract Future<Void> handler(String id, Response response);
}
