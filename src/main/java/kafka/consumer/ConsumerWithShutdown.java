package kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

public class ConsumerWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerWithShutdown.class.getSimpleName());
    public static void main(String[] args) {

        log.info("consumer");

        Properties properties = new Properties();
        Collection<String> topicList = List.of("java_topic");

        //create consumer config
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.199.16.93:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "java-group-id-second");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        //get the reference to the current thread
        final Thread mainThread = Thread.currentThread();

        //adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){

            public void run()
            {
                log.info("Detected a shutdown, lets exit by calling consumer.wakeup()...");
                kafkaConsumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try {
            kafkaConsumer.subscribe(topicList);

            while(true)
            {
                ConsumerRecords<String, String> records =
                        kafkaConsumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record: records)
                {
                    log.info("key: " + record.key() +  ", value: " + record.value() + "\n");
                    log.info("offset: " + record.offset() +  ", partition: " + record.partition() + "\n");
                }
            }
        }
        catch (WakeupException e)
        {
            log.info("Wake Up Exception!");
        }
        catch (Exception e)
        {
            log.error("Unexpected Exception");
        }
        finally {
            kafkaConsumer.close();
            log.info("The consumer is now gracefully closed");
        }


    }
}
