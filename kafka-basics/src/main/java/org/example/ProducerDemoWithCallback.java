package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger log= LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());
    public static void main(String[] args) {

    log.info("Hello world!");

    //Create Producer Properties
        Properties properties= new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the Producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        //Create a Producer Record
        ProducerRecord<String, String> producerRecord= new ProducerRecord<>("demo_java","User_Id_123", "Hi there !!");

        //this is an async operation
        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if(e == null)
                {
                    // the record was successfully send
                    log.info("received Meta data/ \n"+
                            "Topic: "+ metadata.topic() +"\n"+
                            "Partition: " + metadata.partition()+"\n"+
                            "Offset: "  + metadata.offset()+"\n"+
                            "Timestamp: " + metadata.timestamp() );
                }
                else
                {
                    log.error("error while producing", e);
                }
            }
        });




        //flush
        kafkaProducer.flush();

        //close and flush
        kafkaProducer.close();

    }
}