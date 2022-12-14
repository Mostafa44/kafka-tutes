package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger log= LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
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
        ProducerRecord<String, String> producerRecord= new ProducerRecord<>("demo_java","User_Id", "Hello World !!");

        //this is an async operation
        kafkaProducer.send(producerRecord);

        //flush
        kafkaProducer.flush();

        //close and flush
        kafkaProducer.close();

    }
}