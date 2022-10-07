package org.raisin.fixture.task.http.parser;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.AbstractMap;
import java.util.Map;

public class XMLParser implements Parser {

    public static XMLParser parser = new XMLParser();
    private static XmlMapper xmlMapper = new XmlMapper();

    private XMLParser() {
    }

    @Override
    public Map.Entry<String, String> parse(InputStream stream) throws IOException {
        JsonNode jsonNode;
        try {
            jsonNode = xmlMapper.readTree(stream);
        } catch (StreamReadException ex) {
            // invalid record
            return null;
        }
        if (jsonNode == null) {
            return null;
        }
        Map.Entry<String, String> recordAndStatus = null;
        if (jsonNode.has("done")) {
            recordAndStatus = new AbstractMap.SimpleEntry<>("done", null);
        } else if (jsonNode.has("id")) {
            JsonNode jsonNode1 = jsonNode.get("id");
            if (jsonNode1.has("value")) {
                String id = jsonNode1.get("value").asText();
                recordAndStatus = new AbstractMap.SimpleEntry<>("ok", id);
            }
        }
        if (recordAndStatus != null) {
            return recordAndStatus;
        } else {
            throw new RuntimeException("received unknown data: " + jsonNode.asText());
        }
    }
}
