package com.eozd;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class StatsSerializer implements Serializer<Stats> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, Stats stats) {
        byte[] retVal = null;
        ObjectMapper mapper = new ObjectMapper();
        try {
            retVal = mapper.writeValueAsString(stats).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retVal;
    }

    @Override
    public void close() {

    }
}
