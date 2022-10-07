package org.raisin.fixture.task.processor;

import org.raisin.fixture.ExecutionContext;
import org.raisin.fixture.task.http.ProduceHttpTask;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class AggregateTask implements Runnable {

    private final String id;
    private final String source;
    private final String otherSource;
    private final AtomicBoolean fetchedAllRecordsFromOtherSource;
    private final Map<String, Set<String>> recordsHolder;
    private final ExecutionContext executionContext;

    public AggregateTask(String id, String source, String otherSource, Map<String, Set<String>> recordsHolder, ExecutionContext executionContext) {
        this.id = id;
        this.source = source;
        this.otherSource = otherSource;
        this.recordsHolder = recordsHolder;
        this.executionContext = executionContext;
        this.fetchedAllRecordsFromOtherSource = this.executionContext.fetchedAllRecords.get(this.otherSource);
    }

    @Override
    public void run() {
        if (recordsHolder.get(otherSource).contains(id)) {
            // send record back to server and erase the entry
            executionContext.producerThreadPool.submit(new ProduceHttpTask(id, "joined"));
            recordsHolder.get(otherSource).remove(id);
        } else {
            if (fetchedAllRecordsFromOtherSource.get()) {
                // flush this record if all records have already been read from other source, there are no more records to match to
                executionContext.producerThreadPool.submit(new ProduceHttpTask(id, "orphaned"));
            } else {
                recordsHolder.get(source).add(id);
            }
        }
    }
}
