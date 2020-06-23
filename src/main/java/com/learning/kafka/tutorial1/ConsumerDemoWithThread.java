package com.learning.kafka.tutorial1;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ConsumerDemo
 */
public class ConsumerDemoWithThread {

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();

    }

    private ConsumerDemoWithThread() {
    }

    private void run() {
        final Logger logger = LoggerFactory.getLogger(ConsumerThread.class);
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-sixt-application";
        String topic = "first_topic";
        final CountDownLatch latch = new CountDownLatch(1);
        logger.info("###############Creating the consumer thread");
        final Runnable myConsumer = new ConsumerThread(bootstrapServer, groupId, topic, latch);
        Thread myThread = new Thread(myConsumer);
        myThread.start();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("############ Caught shutdown hook");
            ((ConsumerThread) myConsumer).shutdown();
            try{
                latch.await();
            }catch(InterruptedException e){

            }
            logger.info("###############Application has exited");
        })
        );
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        }finally {
            logger.info("Application is closing");
        }

    }

    public class ConsumerThread implements Runnable {
        private CountDownLatch latch; // ce latch peut arrêté notre application proprement
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerThread.class);
         
        public ConsumerThread (String bootstrapServer, String groupId, String topic,
        CountDownLatch latch) {
            this.latch = latch;
            // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//earliest, latest, none
        // earliest from beginning
        // latest from only the new msgs 
        // none throw error if there not offset been saved
        consumer = new KafkaConsumer<String,String>(properties);
        consumer.subscribe(Collections.singleton(topic));//Array.asList(topic, "second_topic");
        
        }

        public void run() {
            try {
                //poll the data
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String,String> it : records) {
                    logger.info("Key = [{}]", it.key());
                    logger.info("Value = [{}]", it.value());
                    logger.info("Partition = [{}]", it.partition());
                    logger.info("Offset = [{}]", it.offset());
                }
            }  
        }catch(WakeupException e){
            logger.info("Received shutdown signal#################################");
        }finally {
            consumer.close();
            // dire au prog main que le consumer a fini
            latch.countDown();
        }
        }
        public void shutdown () {
            // this method interrupt consumer.poll(). it will throw an exception WakeUpException 
            consumer.wakeup();
        }
    }

    //}
}