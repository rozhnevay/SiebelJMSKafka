package com.siebel.eai.jms;

import com.siebel.data.SiebelPropertySet;
import com.siebel.eai.SiebelBusinessService;
import com.siebel.eai.SiebelBusinessServiceException;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;



import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;


public class JMSBusinessService extends SiebelBusinessService {
    private Consumer<String, String> consumer;
    private Producer<String, String> producer;

    public void doInvokeMethod(String method, SiebelPropertySet inputs, SiebelPropertySet outputs) throws SiebelBusinessServiceException {


        if (method.equals("Subscribe")) {
            if (this.consumer == null) {
                this.consumer = createConsumer(inputs);
            }

            while (true) {
                try {
                    final ConsumerRecords<String, String> consumerRecords = this.consumer.poll(500);
                    if (consumerRecords.count() > 0) {
                        for(ConsumerRecord<String,String> record:consumerRecords){
                            SiebelPropertySet msg = new SiebelPropertySet();
                            msg.setValue(record.value());
                            msg.setProperty("Topic", record.topic());
                            msg.setProperty("Partition", String.valueOf(record.partition()));
                            msg.setProperty("Offset", String.valueOf(record.offset()));
                            msg.setProperty("Key", record.key());
                            outputs.addChild(msg);
                        }
                        break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        } else if (method.equals("Publish")) {
            this.producer = createProducer(inputs);
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
                e.printStackTrace();
            }

            producer.flush();
            producer.close();
        } else if (method.equals("Commit")) {
            //this.consumer.commitSync();
        } else if (method.equals("Rollback")) {
            throw new SiebelBusinessServiceException("ROLLBACK", "Error on processing records on Siebel side!");
        } else if (method.equals("CloseConnection")) {}
        else {
            throw new SiebelBusinessServiceException("NO_SUCH_METHOD", "No such method: \"" + method + "\"");
        }
    }

    public void finalize() {
        if (this.consumer != null) {
            try {
                this.consumer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void destroy() {
        if (this.consumer != null) {
            try {
                this.consumer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static Consumer<String, String> createConsumer(SiebelPropertySet inputs)  throws SiebelBusinessServiceException {
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
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 102400);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  btServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        final Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

    private static Producer<String, String> createProducer(SiebelPropertySet inputs) throws SiebelBusinessServiceException {
        Properties props = new Properties();
        final String btServers  = inputs.getProperty("ConnectionFactory");
        if (btServers == null || btServers.isEmpty()) {
            throw new SiebelBusinessServiceException("MISSING_PARAMETER", "Missing parameter \"ConnectionFactory\" (Kafka param: \"bootstrap.servers\")");
        }

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, btServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer(props);
    }

}
