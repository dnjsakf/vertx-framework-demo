package com.dms.apps.vertx.core.examples;

import com.dms.apps.vertx.core.abs.DmsAbstractWorker;
import com.dms.apps.vertx.core.annotations.DmsSubscribe;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.redis.client.Response;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@DmsSubscribe(message = "sample.data", workers = 5)
public class SampleWorkerReseiver extends DmsAbstractWorker {

    @Override
    public Future<Void> handler(String workerId, Response response) {
        Promise<Void> promise = Promise.promise();

        if( response != null ){
            log.info("[{}] sample.data: {}", workerId, response.toString());
        }

        promise.complete();

        return promise.future();
    }

}
