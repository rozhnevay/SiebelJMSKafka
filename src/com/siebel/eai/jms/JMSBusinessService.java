package com.siebel.eai.jms;

import com.siebel.data.SiebelPropertySet;
import com.siebel.eai.SiebelBusinessService;
import com.siebel.eai.SiebelBusinessServiceException;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;


public class JMSBusinessService extends SiebelBusinessService {
    private Consumer<String, String> consumer;
    private Producer<String, String> producer;

    private static Logger Logger = LoggerFactory.getLogger(JMSBusinessService.class);
    private ExecutorService executorService;
    final static int pollTimeout = 1000;
    final static int threadTimeout = 5000;

    final static int heartBeatInterval = 10000;
    final static int sessionTimeout = 30000;
    //FileHandler fileHandler;

    public JMSBusinessService() {
        Logger.info("{MAIN} Constructor START");
        Logger = LoggerFactory.getLogger(JMSBusinessService.class);
        this.executorService = Executors.newSingleThreadExecutor();
    }

    public void doInvokeMethod(String method, SiebelPropertySet inputs, SiebelPropertySet outputs) throws SiebelBusinessServiceException {
        Logger.info("{MAIN} doInvokeMethod START [method = " + method + "]");
        if (Thread.currentThread().getContextClassLoader() == null) {
            Thread.currentThread().setContextClassLoader(JMSBusinessService.class.getClassLoader());
        }

        if (method.equals("Subscribe")) {
            try {
                ConsumerRecord<String, String> record;

                while (true) {
                    try {
                        if (this.consumer == null) {
                            createConsumer(inputs);
                        }
                        Future<ConsumerRecord> result = this.executorService.submit(new PollRecords(this.consumer, pollTimeout, Logger));

                        record = result.get(threadTimeout, TimeUnit.MILLISECONDS);
                        if (record != null) {
                            outputs.setProperty("Topic", record.topic());
                            outputs.setProperty("Partition", String.valueOf(record.partition()));
                            outputs.setProperty("Offset", String.valueOf(record.offset()));
                            outputs.setValue(record.value());
                            this.consumer.commitAsync();
                            break;
                        }

                    } catch (TimeoutException e) {
                        this.consumer.wakeup();
                        if (this.consumer != null) {
                            this.consumer = null;
                        }
                        Logger.error("{MAIN} Timeout Exception", e);
                        throw new SiebelBusinessServiceException("ERROR", e.getMessage());
                    } catch (InterruptedException e) {
                        Logger.error("{MAIN} InterruptedException", e);
                        throw new SiebelBusinessServiceException("ERROR", e.getMessage());
                    } catch (ExecutionException e) {
                        Logger.error("{MAIN} ExecutionException", e);
                        throw new SiebelBusinessServiceException("ERROR", e.getMessage());
                    }

                }
            } catch (Exception e) {
                Logger.error("{MAIN} Exception", e);
                throw new SiebelBusinessServiceException("ERROR", e.getMessage());
            }
        } else if (method.equals("Publish")) {
            if (this.producer == null) {
                createProducer(inputs);
            }
            String topic = inputs.getProperty("SendTopic");

            if (topic == null || topic.isEmpty()) {
                topic = inputs.getProperty("Topic");
                if (topic == null || topic.isEmpty()) {
                    throw new SiebelBusinessServiceException("MISSING_PARAMETER", "Missing parameter \"Topic\"");
                }
            }

            final ProducerRecord<String, String> record = new ProducerRecord<>(topic, inputs.getProperty("Key"), inputs.getValue());
            try {
                RecordMetadata metadata = this.producer.send(record).get();
                outputs.setProperty("Topic", metadata.topic());
                outputs.setProperty("Offset", String.valueOf(metadata.offset()));
                outputs.setProperty("Partition", String.valueOf(metadata.partition()));
            } catch (InterruptedException | ExecutionException e) {
                Logger.error("{MAIN} Exception", e);
                try {
                    this.producer.close();
                } catch (Exception ex){} finally {
                    this.producer = null;
                }

                throw new SiebelBusinessServiceException("ERROR", e.getMessage());
            }
        } else if (method.equals("Commit")) {
            //this.consumer.commitSync();
        } else if (method.equals("Rollback")) {
            throw new SiebelBusinessServiceException("ROLLBACK", "Error on processing records on Siebel side!");
        } else if (method.equals("CloseConnection")) {
            //this.consumer.close();
        }
        else {
            throw new SiebelBusinessServiceException("NO_SUCH_METHOD", "No such method: \"" + method + "\"");
        }
    }

    public void finalize() {
        destroy();
    }

    public void destroy() {
        if (this.consumer != null) {
            try {
                this.consumer.close();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                this.consumer = null;
            }
        }
        if (this.producer != null) {
            try {
                this.producer.close();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                this.producer = null;
            }
        }
    }

    private void createConsumer(SiebelPropertySet inputs)  throws SiebelBusinessServiceException {
        Logger.info("{MAIN - createConsumer} START");
        final Properties props = new Properties();
        final String btServers  = inputs.getProperty("ConnectionFactory");
        final String groupId    = inputs.getProperty("SubscriberIdentifier");
        final String topic      = inputs.getProperty("Topic");
        if (btServers == null || btServers.isEmpty()) {
            throw new SiebelBusinessServiceException("MISSING_PARAMETER", "Missing parameter \"ConnectionFactory\" (Kafka param: \"bootstrap.servers\")");
        }
        if (groupId == null || groupId.isEmpty()) {
            throw new SiebelBusinessServiceException("MISSING_PARAMETER", "Missing parameter \"SubscriberIdentifier\" (Kafka param: \"group.id\")");
        }
        if (topic == null || topic.isEmpty()) {
            throw new SiebelBusinessServiceException("MISSING_PARAMETER", "Missing parameter \"Topic\"");
        }
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  btServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartBeatInterval);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 31000);

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(topic));
        Logger.info("{MAIN - createConsumer} FINISH [consumer = " + this.consumer.hashCode() + "]");
    }

    private void createProducer(SiebelPropertySet inputs) throws SiebelBusinessServiceException {
        Logger.info("{MAIN - createProducer} START");
        Properties props = new Properties();
        final String btServers  = inputs.getProperty("ConnectionFactory");
        if (btServers == null || btServers.isEmpty()) {
            throw new SiebelBusinessServiceException("MISSING_PARAMETER", "Missing parameter \"ConnectionFactory\" (Kafka param: \"bootstrap.servers\")");
        }

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, btServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, "15");

        this.producer =  new KafkaProducer(props);
        Logger.info("{MAIN - createProducer} FINISH [producer = " + this.producer.hashCode() + "]");
    }

}
