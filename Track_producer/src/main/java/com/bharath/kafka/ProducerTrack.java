package com.bharath.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.Random;

public class ProducerTrack {

    public static void main(String[] args) {
        Properties props = new Properties();

        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Random random = new Random();
        for(int i = 1; i<=10; i++){
            String id = "id" + i;
            double latitude = random.nextDouble()*90;
            double longitude = random.nextDouble()*180;
            String message = id + "," + latitude + "," + longitude;

            ProducerRecord<String, String> record = new ProducerRecord<>("TrackTracking", id, message);

            producer.send(record, (metadata, exception) -> {
                if(exception!=null){
                    exception.printStackTrace();
                } else {
                    System.out.println("Sent record:" + id + ", Partiion:" + metadata.partition() +", Offset: " + metadata.offset());
                }
            });
        }

        producer.flush();
        producer.close();
    }
}
