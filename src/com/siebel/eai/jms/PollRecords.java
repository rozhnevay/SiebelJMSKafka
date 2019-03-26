package com.siebel.eai.jms;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;

import java.util.concurrent.Callable;
import org.slf4j.Logger;

public class PollRecords implements Callable<ConsumerRecord> {

    private Consumer<String, String> consumer;
    private Logger Logger;
    private Integer pollTimeout;

    public PollRecords(Consumer<String, String> consumer, Integer pollTimeout, Logger mainLogger) {
        this.consumer = consumer;
        this.Logger = mainLogger;
        this.pollTimeout = pollTimeout;
        Logger.warn("{Poll - Constructor} FINISH, [consumer = " + this.consumer.hashCode() + "]");
    }

    @Override
    public ConsumerRecord call() {
        Logger.warn("{Poll - call} START [consumer = " + this.consumer.hashCode() + "]");
        try {
            ConsumerRecords<String, String> consumerRecords = this.consumer.poll(1000);
            if (consumerRecords.count() > 0) {
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    return record;
                }
            }
        } catch (WakeupException e){
            Logger.warn("{Poll - call Wakeup Exception} - START");
            try {
                this.consumer.close();
                Logger.warn("{Poll - call Wakeup Exception} - Consumer CLOSED");
            } catch (Exception ex) {}
            finally {
                this.consumer = null;
            }
            Logger.warn("{Poll Wakeup Exception} - FINISH");
        } finally {
            Logger.warn("{Poll - call} FINISH");
        }
        return null;
    }
}
