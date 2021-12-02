package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class producerDemoWithCallback {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(producerDemoWithCallback.class);

        String bootstrapServer = "127.0.0.1:9092";
        // create properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        // create a record
        for (int i = 0; i <10; i++){
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("first_topic", "Hello wold!"+ Integer.toString(i));
            // send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        logger.info("received new metadata. \n" +
                                "topic: " + metadata.topic() + "\n" +
                                "partition: " + metadata.partition() + "\n" +
                                "offset: " + metadata.offset() + "\n" +
                                "timestamp: " + metadata.timestamp());
                    } else {
                        logger.error(exception.getMessage());
                    }
                }
            });
        }
        // flush data
        producer.flush();
        // flush and close producer
        producer.close();

    }
}
