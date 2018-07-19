package com.metinform.common.monitor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.metinform.common.BaseConnector;
import com.metinform.common.ConnectorPool;
import com.simple.datax.core.model.connpara.CommonConnPara;
import com.simple.datax.service.impl.SendStatusHandler;

public class SendConnectorPool
        extends ConnectorPool {
    private static Logger logger = Logger.getLogger(SendConnectorPool.class);
    private static SendConnectorPool instance;
    private Map<String, BaseConnector> pool;
    private SendStatusHandler handler = null;
    private SendThreadPoolExecutor poolExecutor;

    private SendConnectorPool(SendStatusHandler handler, SendThreadPoolExecutor poolExecutor) {
        this.handler = handler;
        this.poolExecutor = poolExecutor;
        pool = new ConcurrentHashMap();
    }

    public static synchronized SendConnectorPool getInstance(SendStatusHandler handler, SendThreadPoolExecutor poolExecutor) {
        if (instance == null) {
            instance = new SendConnectorPool(handler, poolExecutor);
        }
        return instance;
    }

    @Override
    public BaseConnector borrowObject(CommonConnPara connPara) {
        String key = getConnKey(connPara.getTransportType(), connPara.getTransportPara());
        BaseConnector connector = null;
        synchronized (pool) {
            connector = (BaseConnector) pool.get(key);
            if (connector == null) {
                ConnectorFactory factory = ConnectorFactory.getInstance();
                try {
                    connector = factory.makeObject(key);
                    if (handler == null) {
                        handler = SendStatusHandler.getInstance();
                    } else if (SendStatusHandler.getInstance() != handler) {
                        logger.error("Handler created by Spring is different from return by SendStatusHandler.getInstance()");
                    }
                    connector.setHandler(handler);
                    if (poolExecutor == null) {
                        poolExecutor = SendThreadPoolExecutor.getInstance();
                    } else if (SendThreadPoolExecutor.getInstance() != poolExecutor) {
                        logger.error("SendThreadPoolExecutor created by Spring is different from return by SendThreadPoolExecutor.getInstance()");
                    }
                    connector.setPoolExecutor(poolExecutor);
                    pool.put(key, connector);
                    connector.setEncoding(connPara.getEncoding());
                } catch (Exception e) {
                    logger.error("ConnectorFactory makeObject failed.", e);
                }
            }
        }
        return connector;
    }

    @Override
    public void returnObject(BaseConnector connector) {
    }
}



