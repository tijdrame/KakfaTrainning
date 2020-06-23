package com.learning.kafka.tutorial4streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

import com.google.gson.JsonParser;

public class StreamsFilterTweets {

    public static void main(String[] args) {
        //create properties
    Properties properties = new Properties();
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");//like consumer groups but for streams apps
    properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    
    //create topology
    StreamsBuilder streamsBuilder = new StreamsBuilder();

    //input topic
    KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");//source
    KStream<String, String> filteredStream = inputTopic.filter(
        (k, jsonTweet) -> extractUserFollowerInTweet(jsonTweet) > 1000
    );
    filteredStream.to("important_tweets");//create topic in cmd pour recevoir les données

    //build topology
    KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

    //start our streams application
    kafkaStreams.start();
    }
    
    private static Integer extractUserFollowerInTweet (String tweetJson) {
        try {
            return JsonParser.parseString(tweetJson)
            .getAsJsonObject()
            .get("user")
            .getAsJsonObject()
            .get("followers_count")
            .getAsInt();
        } catch (Exception e) {
           return 0;
        }
        
    }
    
}