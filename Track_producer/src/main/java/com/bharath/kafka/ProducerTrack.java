package com.bharath.kafka;

import com.bharath.kafka.data.Tracking;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class ProducerTrack {

    public static void main(String[] args) {
        Properties props = new Properties();

        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "com.bharath.kafka.data.TrackSerializer");

        KafkaProducer<String, Tracking> producer = new KafkaProducer<>(props);

        Random random = new Random();
        for(int i = 1; i<=10; i++){
            String id = "id" + i*random.nextInt();
            double latitude = random.nextDouble()*90;
            double longitude = random.nextDouble()*180;
           Tracking tracking = new Tracking();
           tracking.setLatitude(latitude);
           tracking.setLongitude(longitude);

            ProducerRecord<String, Tracking> record = new ProducerRecord<>("TrackTracking2", id, tracking);

            producer.send(record, (metadata, exception) -> {
                if(exception!=null){
                    exception.printStackTrace();
                } else {
                    System.out.println("Sent record:" + id + ", Partition:" + metadata.partition() +", Offset: " + metadata.offset());
                }
            });
        }

        producer.flush();
        producer.close();
    }
}
