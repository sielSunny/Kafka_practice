package com.bharath.kafka.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class TrackDeserializer implements Deserializer<Tracking> {
    @Override
    public Tracking deserialize(String s, byte[] bytes) {
        ObjectMapper objectMapper = new ObjectMapper();
        Tracking tracking = null;
        try {
            tracking = objectMapper.readValue(bytes, Tracking.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return tracking;
    }
}
