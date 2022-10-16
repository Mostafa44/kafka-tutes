package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutDown {
    private static final Logger log= LoggerFactory.getLogger(ConsumerDemoWithShutDown.class.getSimpleName());
    public static void main(String[] args) {

    log.info("Hi there I am a Java Consumer!");

        String groupId = "my-Third-application";
        String topic = "demo_java";

    //Create Producer Properties
        Properties properties= new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Create the Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //Get a reference to the Current Thread
        final Thread mainThread= Thread.currentThread();

        //Adding the shut down hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public  void run(){
                log.info("Detected shutdown , let's exit by calling consumer wakeup()....");
                consumer.wakeup();
                try{
                    mainThread.join();
                } catch (InterruptedException e) {
                   e.printStackTrace();
                }
            }
        });

        //Subscribe the consumer to our topic
        consumer.subscribe(Arrays.asList(topic));

        try{
            while(true)
            {
                log.info("Polling");
                ConsumerRecords<String, String> records=
                        consumer.poll(Duration.ofMillis(100));
                for(ConsumerRecord<String, String> record: records)
                {
                    log.info("Key:  " + record.key() +", value: " + record.value());
                    log.info("Partition: "+ record.partition() + ", offset: "+ record.offset());
                    log.info("===================================");
                }
            }
        }catch ( WakeupException e){
            log.info("Wakeup exception expected!!");
        }catch (Exception e){
           log.error("Unexpected Exception");
        }finally {
            consumer.close();
            log.info("The consumer is now gracefully closed");
        }

    }
}