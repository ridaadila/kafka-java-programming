package kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerCallback.class.getSimpleName());
    public static void main(String[] args) {

        log.info("hello world");

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.199.16.93:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            //create producer record
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>("java_topic", "data " + i + "dikirim dari program producer java");

            //send data with an asyncrhonous
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes everytime record successfully sent or exception thrown
                    if(e==null)
                    {
                        log.info(
                                "Topic: " + recordMetadata.topic() + "\n" +
                                        "Partition: " +recordMetadata.partition() + "\n" +
                                        "Offset: " + recordMetadata.offset() + "\n" +
                                        "Timestamp: " + recordMetadata.timestamp()
                        );
                    }
                    else {
                        log.error("Error while producing: ", e);
                    }
                }
            });


            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        //flush - synchronous
        producer.flush();

        //flush and close producer
        producer.close();
    }
}
