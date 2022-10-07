package org.raisin.fixture.task.http;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.raisin.fixture.ExecutionContext;
import org.raisin.fixture.task.http.parser.Parser;
import org.raisin.fixture.task.processor.AggregateTask;
import org.raisin.fixture.task.processor.FlushTask;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumeHttpTask extends HttpTask {

    private static final String consumerBaseURI = baseURI + "source/";
    private static final Map<String, HttpGet> httpGets = new HashMap<>() {{
        put("a", getHttpGetRequest("a"));
        put("b", getHttpGetRequest("b"));
    }};

    private final String source;
    private final String otherSource;
    private final AtomicBoolean fetchedAllRecords;
    private final AtomicBoolean fetchedAllRecordsFromOtherSource;
    private final ExecutionContext executionContext;

    private final Parser parser;

    public ConsumeHttpTask(String source, Parser parser, ExecutionContext executionContext) {
        super();
        this.source = source;
        this.otherSource = getOtherSource();
        this.executionContext = executionContext;
        this.fetchedAllRecords = this.executionContext.fetchedAllRecords.get(source);
        this.fetchedAllRecordsFromOtherSource = this.executionContext.fetchedAllRecords.get(otherSource);
        this.parser = parser;
    }

    @Override
    public void run() {
        String threadName = Thread.currentThread().getName();
        HttpGet httpGet = httpGets.get(source);
        while (!fetchedAllRecords.get()) {  // fetch all records from source
            CloseableHttpResponse httpResponse;
            try {
                httpResponse = httpClient.execute(httpGet);
            } catch (IOException e) {
                // ignoring exception for faster processing
                System.err.println(threadName + ": " + e.getMessage());
                continue;   // re-fetch
            }
            try {
                HttpEntity httpEntity = httpResponse.getEntity();
                if (httpResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    consumeHttpEntity(httpEntity);
                } else if (httpResponse.getStatusLine().getStatusCode() == HttpStatus.SC_NOT_ACCEPTABLE) {
                    // discard content and release the connection
                    EntityUtils.consume(httpEntity);
                } else {
                    throw new RuntimeException();
                }
            } catch (IOException e) {
                // ignoring exception for faster processing
                // todo should http response be closed?
                //try {
                //    httpResponse.close();
                //} catch (IOException ex) {
                //    throw new RuntimeException(ex);
                //}
            }
        }
        // not closing httpclient as that will break sockets managed by underlying connection manager
        // these sockets can be used for executing other tasks
        //try {
        //    httpClient.close();
        //} catch (IOException e) {
        //    throw new RuntimeException(e);
        //}
    }

    private void consumeHttpEntity(HttpEntity httpEntity) throws IOException {
        if (httpEntity != null) {
            Map.Entry<String, String> statusAndRecord;
            try (InputStream inputStream = httpEntity.getContent()) {
                statusAndRecord = parser.parse(inputStream);
            }
            if (statusAndRecord == null) {
                return;
            }
            String status = statusAndRecord.getKey();
            if (status.equalsIgnoreCase("ok")) {
                String id = statusAndRecord.getValue();
                if (id != null) {
                    /*
                     submit id to one of the processor thread pool based on hash based partitioning
                     sending id to thread pool based on hash based partitioning, to ensure that same thread pool processes record from both the sources A & B
                     this way we will not have to worry about synchronization issues for a specific record
                     */
                    int processorThreadPoolId = getProcessorThreadPoolId(id);
                    executionContext.processorThreadPools[processorThreadPoolId].submit(
                            new AggregateTask(id, source, otherSource, executionContext.recordHolders.get(processorThreadPoolId), executionContext)
                    );
                }
            } else if (status.equalsIgnoreCase("done")) {
                // signal that all records has been fetched from the source
                if (!fetchedAllRecords.get()) {
                    if (!fetchedAllRecords.getAndSet(true)) {
                        // this code block is only executed by the first thread setting up the atomic boolean flag to true
                        // flush all records consumed from the other source
                        flushRecordsConsumedFromOtherSource();
                        // countdown latch
                        executionContext.consumerCountDownLatch.countDown();
                        // shutdown the consumer thread pool managing this source
                        executionContext.consumerThreadPools.get(source).shutdown();
                    }
                }
            } else {
                throw new RuntimeException("received unknown status: " + status);
            }
        } else {
            throw new RuntimeException();
        }
    }

    private int getProcessorThreadPoolId(String id) {
        // set most significant bit to 0 to ensure hashcode is positive integer
        return (id.hashCode() & 0xfffffff) % executionContext.processorThreadPools.length;
    }

    private void flushRecordsConsumedFromOtherSource() {
        for (int a = 0; a < executionContext.processorThreadPools.length; ++a) {
            executionContext.processorThreadPools[a].submit(new FlushTask(getOtherSource(), executionContext.recordHolders.get(a), executionContext));
        }
    }

    private String getOtherSource() {
        return source.equalsIgnoreCase("a") ? "b" : "a";
    }

    private static HttpGet getHttpGetRequest(String source) {
        HttpGet httpGet = new HttpGet(consumerBaseURI + source);
        httpGet.addHeader(new BasicHeader("Connection", "Keep-Alive"));
        return httpGet;
    }
}
