package com.metinform.common.monitor;

import com.metinform.common.ConnectorPool;
import com.simple.datax.service.impl.SendStatusHandler;

public class ConnectorPoolManager {
    public static final String CONN_SEND = "SEND";
    public static final String CONN_RECV = "RECEIVE";
    private static ConnectorPoolManager manager;

    public static synchronized ConnectorPoolManager getInstance() {
        if (manager == null) {
            manager = new ConnectorPoolManager();
        }
        return manager;
    }

    public ConnectorPool getConnectorPool(String connType, SendStatusHandler handler, SendThreadPoolExecutor poolExecutor) {
        if ("SEND".equalsIgnoreCase(connType)) {
            return SendConnectorPool.getInstance(handler, poolExecutor);
        }
        if ("RECEIVE".equalsIgnoreCase(connType)) {
            return ReceiveConnectorPool.getInstance();
        }
        return null;
    }
}



