package com.bharath.kafka.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class TrackSerializer implements Serializer<Tracking> {
    @Override
    public byte[] serialize(String s, Tracking tracking) {
        byte[] response = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            response = objectMapper.writeValueAsString(tracking).getBytes();
        }
        catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        return response;
    }
}
