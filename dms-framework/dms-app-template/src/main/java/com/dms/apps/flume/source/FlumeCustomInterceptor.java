package com.dms.apps.flume.source;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FlumeCustomInterceptor implements Interceptor  {
    
    @Override
    public void initialize() {
        // 초기화 작업
    }

    @Override
    public Event intercept(Event event) {

        byte[] body = event.getBody();
        String eventBody = new String(body, StandardCharsets.UTF_8);

        if( eventBody.contains("ping") ){
            return event;
        }

        // JsonParser parser = new JsonParser();
        // JsonElement jsonElement = parser.parse(eventBody);
        // JsonObject jsonObject = jsonElement.getAsJsonObject();

        // JsonElement stbIdElement = jsonObject
        //         .getAsJsonObject("body")
        //         .getAsJsonObject("data")
        //         .getAsJsonObject("contents")
        //         .get("stbId");

        // if (stbIdElement != null && !stbIdElement.isJsonNull()) {
        //     return event;
        // }

        // stbId가 없는 경우 이벤트를 필터링
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> interceptedEvents = new ArrayList<>();
        for (Event event : events) {
            Event interceptedEvent = intercept(event);
            if (interceptedEvent != null) {
                interceptedEvents.add(interceptedEvent);
            }
        }
        return interceptedEvents;
    }

    @Override
    public void close() {
        // 종료 작업
    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new FlumeCustomInterceptor();
        }

        @Override
        public void configure(Context context) {
            // 구성 설정
        }
    }
}
