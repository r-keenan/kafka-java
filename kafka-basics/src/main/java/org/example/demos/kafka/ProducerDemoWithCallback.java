package org.example.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemoWithCallback {
    
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());
    
    public static void main(String[] args) {
       log.info("I am a Kafka Producer");
        
        // create Producer properties
        Properties properties = new Properties();
        
        // connect to local host
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        
        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        
        // setting batch size
        // WARNING: NEVER DO THIS IN PROD! STAY WITH THE KAFKA DEFAULT OF 16KB.
        //properties.setProperty("batch.size", "400");
        
        // Setting to Round Robin instead of StickyPartitioner
        // WARNING: NEVER DO THIS IN PROD!
        //properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());
        
        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        
        for (int j = 0; j < 10; j++) {

            for (int i = 0; i < 30; i++ ){

                String message = STR."hello world \{i}";
                // create a Producer Record (message)
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>("demo_java", message);

                // send the data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // executes every time a record successfully sent or an exception is thrown
                        if (e == null) {
                            // record successfully sent
                            log.info("Received new metadata \n" +
                                    "Topic: " + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp()
                            );
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        
        // tell the producer to send all data and block until done -- synchronous operation
        producer.flush();
        
        // flush and close the producer
        producer.close();
    }
}
