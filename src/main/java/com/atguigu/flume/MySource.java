package com.atguigu.flume;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.avro.Status;

public class MySource extends AbstractSource implements Configurable, PollableSource {
    private String myProp;
    private Long interval;

    @Override
    public void configure(Context context) {
        String myProp = context.getString("myProp", "defaultValue");
        this.interval = context.getLong("interval", 500L);
        // Process the myProp value (e.g. validation, convert to another type, ...)

        // Store myProp for later retrieval by process() method
        this.myProp = myProp;
    }

    @Override
    public void start() {
        // Initialize the connection to the external client
    }

    @Override
    public void stop () {
        // Disconnect from external client and do any additional cleanup
        // (e.g. releasing resources or nulling-out field values) ..
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;

        try {
            // This try clause includes whatever Channel/Event operations you want to do

            // Receive new data
            Event e = getSomeData();

            Thread.sleep(interval);
            // Store the Event into this Source's associated Channel(s)
            getChannelProcessor().processEvent(e);

            status = Status.READY;
        } catch (Throwable t) {
            // Log exception, handle individual exceptions as needed

            status = Status.BACKOFF;

            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error)t;
            }
        }
        return status;
    }

    private Event getSomeData() {
        String text = myProp + "Data is: " + Math.random();
        Event event = new SimpleEvent();
        event.setBody(text.getBytes());
        return event;
    }


    @Override
    public long getBackOffSleepIncrement() {
        return 1000;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 10000;
    }


}
