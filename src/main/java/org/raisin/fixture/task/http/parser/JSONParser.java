package org.raisin.fixture.task.http.parser;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.AbstractMap;
import java.util.Map;

public class JSONParser implements Parser {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static JSONParser parser = new JSONParser();

    private JSONParser() {
    }

    @Override
    public Map.Entry<String, String> parse(InputStream stream) throws IOException {
        JsonNode jsonNode;
        try {
            jsonNode = objectMapper.readTree(stream);
        } catch (StreamReadException ex) {
            // invalid record
            return null;
        }
        if (jsonNode == null) {
            return null;
        }
        Map.Entry<String, String> recordAndStatus = null;
        String status = jsonNode.get("status").asText();
        if (status.equalsIgnoreCase("done")) {
            recordAndStatus = new AbstractMap.SimpleEntry<>(status, null);
        } else if (status.equalsIgnoreCase("ok")) {
            if (jsonNode.has("id")) {
                String id = jsonNode.get("id").asText();
                recordAndStatus = new AbstractMap.SimpleEntry<>(status, id);
            } else {
                recordAndStatus = new AbstractMap.SimpleEntry<>(status, null);
            }
        } else if (status.equalsIgnoreCase("fail")) {
            recordAndStatus = new AbstractMap.SimpleEntry<>(status, null);
        }
        if (recordAndStatus != null) {
            return recordAndStatus;
        } else {
            throw new RuntimeException("received unknown status: " + status);
        }
    }
}
