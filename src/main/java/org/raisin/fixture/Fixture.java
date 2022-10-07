package org.raisin.fixture;

import org.raisin.fixture.task.http.ConsumeHttpTask;
import org.raisin.fixture.task.http.parser.JSONParser;
import org.raisin.fixture.task.http.parser.XMLParser;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class Fixture {

    ExecutionContext executionContext = new ExecutionContext();

    public static void main(String[] args) throws InterruptedException {
        Fixture fixture = new Fixture();
        Date startedAt = new Date();
        Date readFromBothSourcesAt = fixture.run();
        Date endedAt = new Date();
        printTimeElapsed(startedAt, readFromBothSourcesAt, endedAt);
        System.err.println();
    }

    private static void printTimeElapsed(Date startedAt, Date readFromAllSourcesAt, Date endedAt) {
        String threadName = Thread.currentThread().getName();
        System.err.println(threadName + ": Started At: " + startedAt);
        System.err.println(threadName + ": Read from all Sources At: " + readFromAllSourcesAt);
        System.err.println(threadName + ": Ended At: " + endedAt);
        List<Long> timeElapsedFromMidToEnd = timeElapsed(readFromAllSourcesAt, endedAt);
        System.err.println(threadName + ": Time elapsed from 'Read from all Sources' to End: " + timeElapsedFromMidToEnd.get(0) + " min " + timeElapsedFromMidToEnd.get(1) + " sec " + timeElapsedFromMidToEnd.get(2) + " millis");
        List<Long> timeElapsedFromStartToEnd = timeElapsed(startedAt, endedAt);
        System.err.println(threadName + ": Total Time Elapsed: " + timeElapsedFromStartToEnd.get(0) + " min " + timeElapsedFromStartToEnd.get(1) + " sec " + timeElapsedFromStartToEnd.get(2) + " millis");
    }

    private static List<Long> timeElapsed(Date start, Date end) {
        long totalMillisElapsed = end.getTime() - start.getTime();
        long minutesElapsed = totalMillisElapsed / 1000 / 60;
        long secondsElapsed = (totalMillisElapsed - minutesElapsed * 60000) / 1000;
        long millisElapsed = totalMillisElapsed - minutesElapsed * 60000 - secondsElapsed * 1000;
        return Arrays.asList(minutesElapsed, secondsElapsed, millisElapsed);
    }

    public Date run() throws InterruptedException {
        // submit task (to consume records from server until all records are consumed) to consumer thread pool's thread
        for (int a = 0; a < executionContext.consumerThreadPoolSize; ++a) {
            executionContext.consumerThreadPools.get("a").submit(new ConsumeHttpTask("a", JSONParser.parser, executionContext));
            executionContext.consumerThreadPools.get("b").submit(new ConsumeHttpTask("b", XMLParser.parser, executionContext));
        }
        // wait until all the records from all the sources has been consumed
        try {
            executionContext.consumerCountDownLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Date readFromAllSourcesAt = new Date();
        // shutdown processor thread pools
        Stream.of(executionContext.processorThreadPools).forEach(this::shutdownAndAwaitTermination);
        // consumer thread pool is shutdown by the executing task (when all events are received from the associated source)
        // shutdown producer thread pool
        shutdownAndAwaitTermination(executionContext.producerThreadPool);
        return readFromAllSourcesAt;
    }

    private void shutdownAndAwaitTermination(ExecutorService threadPool) {
        threadPool.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
                threadPool.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!threadPool.awaitTermination(60, TimeUnit.SECONDS))
                    System.err.println("Thread pool didn't terminate");
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            threadPool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }
}

