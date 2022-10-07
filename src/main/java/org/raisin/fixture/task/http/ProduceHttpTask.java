package org.raisin.fixture.task.http;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.raisin.fixture.task.http.parser.JSONParser;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class ProduceHttpTask extends HttpTask {

    private static final AtomicLong recordsProcessed = new AtomicLong(0);
    private static final String producerURI = baseURI + "sink/a";

    private final String id;
    private final String type;

    public ProduceHttpTask(String id, String type) {
        this.id = id;
        this.type = type;
    }

    @Override
    public void run() {
        String threadName = Thread.currentThread().getName();
        while (true) {  // will break out when posted successfully
            HttpPost httpPost = getHttpPostRequest(id, type);
            CloseableHttpResponse httpResponse;
            try {
                httpResponse = httpClient.execute(httpPost);
            } catch (IOException e) {
                // ignoring exception for faster processing
                //System.err.println(threadName + ": " + e.getMessage());
                continue;   // re-fetch
            }
            try {
                HttpEntity httpEntity = httpResponse.getEntity();
                if (httpResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    if (postedSuccessfully(httpEntity)) {
                        //long recordsProcessedSoFar = recordsProcessed.addAndGet(1);
                        //if (recordsProcessedSoFar % 1000 == 0) {
                        //    System.err.println(Thread.currentThread().getName() + ": " + new Date() + ": records processed so far: " + recordsProcessedSoFar);
                        //}
                        break;
                    }
                } else {
                    throw new RuntimeException();
                }
            } catch (IOException e) {
                // ignoring processing to fast process
                //System.err.println(threadName + ": " + e.getMessage());
                //try {
                //    httpResponse.close();
                //} catch (IOException ex) {
                //    throw new RuntimeException(ex);
                //}
            }
        }
    }

    private boolean postedSuccessfully(HttpEntity httpEntity) throws IOException {
        if (httpEntity == null) {
            return false;
        }
        Map.Entry<String, String> statusAndRecord;
        try (InputStream inputStream = httpEntity.getContent()) {
            statusAndRecord = JSONParser.parser.parse(inputStream);
        }
        if (statusAndRecord == null) {
            return false;
        }
        String status = statusAndRecord.getKey();
        if (status.equalsIgnoreCase("ok")) {
            return true;
        } else if (status.equalsIgnoreCase("fail")) {
            return false;
        } else {
            throw new RuntimeException("received unknown status: " + status);
        }
    }

    private HttpPost getHttpPostRequest(String id, String type) {
        HttpPost httpPost = new HttpPost(producerURI);
        try {
            httpPost.setEntity(new StringEntity("{\"id\":\"" + id + "\",\"kind\":\"" + type + "\"}"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        httpPost.addHeader(new BasicHeader("Content-Type", "application/json"));
        httpPost.addHeader(new BasicHeader("Connection", "Keep-Alive"));
        return httpPost;
    }
}
