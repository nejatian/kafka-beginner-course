package com.github.nejatian.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class consumerDemoAssignSeek {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(consumerDemo.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String topic = "first_topic";

        // create properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // Assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        Long offsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));
        // Seek
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        // poll for new data
        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

        while (keepOnReading){
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record :records ){
                numberOfMessagesReadSoFar += 1;
                logger.info("Key: " + record.key() + ", value: "+ record.value());
                logger.info("partition: " + record.partition() + ", Offset: " + record.offset());
                if(numberOfMessagesReadSoFar >= numberOfMessagesToRead){
                    keepOnReading = false;
                    break;
                }
            }
        }
        logger.info("Exiting Application");
    }
}
