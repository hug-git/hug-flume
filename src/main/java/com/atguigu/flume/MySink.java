package com.atguigu.flume;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;


public class MySink extends AbstractSink implements Configurable {
    private String output;

    @Override
    public void configure(Context context) {
        String output = context.getString("output");

        // Process the myProp value (e.g. validation)

        // Store myProp for later retrieval by process() method
        this.output = output;
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;

        // Start transaction
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();
        try {
            // This try clause includes whatever Channel operations you want to do

            Event event;
            while((event = ch.take()) == null)
                Thread.sleep(500);

            // Send the Event to the external repository.
            storeSomeData(event);

            txn.commit();
            status = Status.READY;
        } catch (Throwable t) {
            txn.rollback();

            // Log exception, handle individual exceptions as needed

            status = Status.BACKOFF;

            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error)t;
            }
        } finally {
            txn.close();
        }
        return status;
    }

    public void storeSomeData(Event event) throws IOException {
        FileOutputStream outputStream = new FileOutputStream(output,true);
        outputStream.write(event.getBody());
        outputStream.write('\n');
        outputStream.close();
    }
}
