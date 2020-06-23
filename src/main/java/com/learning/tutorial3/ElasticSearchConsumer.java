package com.learning.tutorial3;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import com.google.gson.JsonParser;

import org.apache.http.HttpHost;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ElasticSearchConsumer
 */
public class ElasticSearchConsumer {

    public static RestHighLevelClient createClient() {
        String hostName = "localhost";
        //String userName = "";
        //String password = "";
        // dont do if you run a local ES
        // final CredentialsProvider credentialsProvider = new
        // BasicCredentialsProvider();
        // credentialsProvider.setCredentials(AuthScope.ANY, new
        // UsernamePasswordCredentials(userName, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostName, 9200, "http"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {

                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder arg0) {
                        // return arg0.setDefaultCredentialsProvider(credentialsProvider);//not local
                        // enable this
                        return arg0;
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    // consumer pour mettre toutes les données dans elastic search
    public static KafkaConsumer<String, String> createConsumer(String topic) {
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";
        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");// earliest, latest, none
        // earliest from beginning
        // latest from only the new msgs
        // none throw error if there not offset been saved
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");// disable autocomit
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");// dés quon recoit 100 record on commence l'insertion
        

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    public static void main(String[] args) throws IOException {
        Logger log = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = createClient();

        // String jsonString = "{\"foo\":\"bar\"}";

        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");// twitter_tweets a ete cree
                                                                                  // manuellement avec cmd kafka
        // poll for new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));// new in kafka 2.0.0
            Integer count = records.count();
            log.info("Received [{}] records", count);

            BulkRequest bulkRequest = new BulkRequest();

            for (ConsumerRecord<String, String> it : records) {
                String jsonString = it.value();
                //gestion des id pour avoir un consumer idempotent (insertion du msg une seule fois)
                //1er cas Kafka generic Id
                //String id = it.topic() +"_"+ it.partition() +"_"+ it.offset();
                //2e cas twitter feed specific
                try {
                    String id = extractIdFromTweet(jsonString);
                    // insert value into esalstic search
                    
                    IndexRequest indexRequest = new IndexRequest("twitter");
                    indexRequest.id(id);//optionel c pour fixer l'id (bon pour idempotent cad pas duplicat de donnes inserer)
                    indexRequest.source(jsonString, XContentType.JSON);

                    bulkRequest.add(indexRequest);

                } catch (Exception e) {
                    log.warn("Skipping bad data [{}]", it.value());
                }
                //IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
                //String id = response.getId();
                //log.info("id = [{}]", response.getId());

                
            }
            if(count > 0){
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                log.info("Commiting offsets...");
                consumer.commitSync();
                log.info("Offsets have been commited!");
                try {
                    Thread.sleep(1000);//pour but de demo, on peut l'enlever pour aller plus vite,
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } 

        //client.close();
    }
    private static String extractIdFromTweet (String tweetJson) {
        return JsonParser.parseString(tweetJson)
            .getAsJsonObject()
            .get("id_str")
            .getAsString();
    }
}