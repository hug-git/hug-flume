package com.atguigu.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

public class MyInterceptor implements Interceptor {
    private String prefix;
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        Map<String,String> headers = event.getHeaders();
        byte[] body = event.getBody();
        String line = new String(body);
        char c = line.charAt(0);
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
            headers.put("alphabet", "is_alphabet");
        } else {
            headers.put("alphabet", "not_alphabet");
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    @Override
    public void close() {

    }

    public static class MyBuild implements Builder{

        @Override
        public Interceptor build() {
            return new MyInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
