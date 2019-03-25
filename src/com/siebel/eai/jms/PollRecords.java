package com.siebel.eai.jms;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;

import java.util.concurrent.Callable;
import org.slf4j.Logger;

public class PollRecords implements Callable<ConsumerRecord> {

    private Consumer<String, String> consumer;
    Logger LOGGER;

    public PollRecords(Consumer<String, String> consumer, Logger LOGGER) {
        this.consumer = consumer;
        this.LOGGER = LOGGER;
        LOGGER.info("Inside Poll this.consumer = " + this.consumer.hashCode());
    }

    @Override
    public ConsumerRecord call() throws Exception {

        try {
            LOGGER.info("Poll this.consumer = " + this.consumer.hashCode());
            ConsumerRecords<String, String> consumerRecords = this.consumer.poll(500);
            if (consumerRecords.count() > 0) {
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    return record;
                }
            }
            return null;
        } catch (WakeupException e){
            LOGGER.info("Inside Poll Wakeup");
            //this.consumer.close();
            //LOGGER.info("Consumer Closed");
        }
        return null;
    }
}
