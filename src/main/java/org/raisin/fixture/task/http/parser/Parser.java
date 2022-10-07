package org.raisin.fixture.task.http.parser;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public interface Parser {

    public Map.Entry<String, String> parse(InputStream stream) throws IOException;
}
