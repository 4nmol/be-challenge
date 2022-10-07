package org.raisin.fixture.task.processor;

import org.raisin.fixture.ExecutionContext;
import org.raisin.fixture.task.http.ProduceHttpTask;

import java.util.Map;
import java.util.Set;

public class FlushTask implements Runnable {

    private final String source;
    private final Map<String, Set<String>> recordsHolder;
    private final ExecutionContext executionContext;

    public FlushTask(String source, Map<String, Set<String>> recordsHolder, ExecutionContext executionContext) {
        this.source = source;
        this.recordsHolder = recordsHolder;
        this.executionContext = executionContext;
    }

    @Override
    public void run() {
        // flush out orphan records to the server
        recordsHolder.get(source).forEach(id -> executionContext.producerThreadPool.submit(new ProduceHttpTask(id, "orphaned")));
        recordsHolder.get(source).clear();
    }
}
