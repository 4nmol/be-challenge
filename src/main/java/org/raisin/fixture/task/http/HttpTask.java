package org.raisin.fixture.task.http;

import org.apache.http.config.SocketConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.raisin.fixture.ExecutionContext;

public abstract class HttpTask implements Runnable {

    protected static final String baseURI = "http://127.0.0.1:7299/";

    private static final PoolingHttpClientConnectionManager connectionManager = getConnectionManager();

    // http objects
    protected CloseableHttpClient httpClient;

    public HttpTask() {
        this.httpClient = getCloseableHttpClient();
    }

    private static PoolingHttpClientConnectionManager getConnectionManager() {
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        SocketConfig socketConfig = SocketConfig.custom().setSoReuseAddress(true)
                .setSoKeepAlive(true) //todo see if this needs to be uncommented depending upon the performance
                .build();
        connectionManager.setDefaultSocketConfig(socketConfig);
        connectionManager.setMaxTotal(ExecutionContext.totalMaxConnections);
        connectionManager.setDefaultMaxPerRoute(Math.min(ExecutionContext.maxConnectionsPerRoute, ExecutionContext.totalMaxConnections));
        return connectionManager;
    }

    private CloseableHttpClient getCloseableHttpClient() {
        return HttpClients.custom().setConnectionManager(connectionManager).build();
    }
}
