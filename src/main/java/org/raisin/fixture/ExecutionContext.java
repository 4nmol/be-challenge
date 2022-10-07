package org.raisin.fixture;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class ExecutionContext {

    // http connection configurations
    public static final int totalMaxConnections = 1024;
    public static final int maxConnectionsPerRoute = 1024;

    //thread pools
    public Map<String, ExecutorService> consumerThreadPools = new HashMap<>();
    public ExecutorService[] processorThreadPools;
    public ExecutorService producerThreadPool;

    // thread pool sizes
    public int consumerThreadPoolSize = 1;
    public int processorThreadPoolSize = 1;
    public int producerThreadPoolSize = 1;

    // flags that maintain if all records have been fetched from respective source
    public Map<String, AtomicBoolean> fetchedAllRecords = new HashMap<>();

    // sources
    public List<String> sources = Arrays.asList("a", "b");

    /*
     countdown latch
     it is used to make main thread wait(to stop consumer threads from consuming records any further) until all records has been fetched from both the sources on the server
     it will be decremented once for each source when all records from that source has been read
     */
    public CountDownLatch consumerCountDownLatch = new CountDownLatch(sources.size());

    /*
     record holders
     each processor thread maintains its own record holder per source
     Map<String, Set<String>> => source -> record holder
     */
    public List<Map<String, Set<String>>> recordHolders;

    public ExecutionContext() {
        initializeThreadPools();
        // initialize flags
        for (String source : sources) {
            fetchedAllRecords.put(source, new AtomicBoolean(false));
        }
        //
        initializeRecordHolders();
    }

    private void initializeThreadPools() {
        // initialize consumer thread pools
        for (String source : sources) {
            consumerThreadPools.put(source, Executors.newFixedThreadPool(consumerThreadPoolSize, new ThreadFactoryBuilder().setNameFormat("pool-consumer-" + source.toUpperCase() + "-thread-%d").build()));
        }

        // initialize processor thread pools
        processorThreadPools = new ExecutorService[processorThreadPoolSize];
        for (int a = 0; a < processorThreadPoolSize; ++a) {
            processorThreadPools[a] = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("pool-processor-thread-%d").build());

        }

        // initialize producer thread pool
        producerThreadPool = Executors.newFixedThreadPool(producerThreadPoolSize, new ThreadFactoryBuilder().setNameFormat("pool-producer-thread-%d").build());
        //producerThreadPool = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("pool-producer-thread-%d").build());
    }

    private void initializeRecordHolders() {
        recordHolders = new ArrayList<>(processorThreadPoolSize);
        for (int a = 0; a < processorThreadPoolSize; ++a) {
            Map<String, Set<String>> recordHolder = new HashMap<>();
            for (String source: sources) {
                recordHolder.put(source, new HashSet<>());
            }
            recordHolders.add(recordHolder);
        }
    }
}
