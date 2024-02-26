package com.bharath.kafka;

import com.bharath.kafka.data.Tracking;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerTrack {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "com.bharath.kafka.data.TrackDeserializer");
        props.setProperty("group.id", "TrackGroup");
        KafkaConsumer<String, Tracking> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList("TrackTracking2"));

        while (true) {
            ConsumerRecords<String, Tracking> records = consumer.poll(Duration.ofSeconds(20));

            for (ConsumerRecord<String, Tracking> record : records) {
                String id = record.key();
                Tracking tracking = record.value();

                System.out.println("Received record: Id = " + id + ", Latitude = " + tracking.getLatitude() + ", Longitude = " + tracking.getLongitude());
            }
        }
    }
}
