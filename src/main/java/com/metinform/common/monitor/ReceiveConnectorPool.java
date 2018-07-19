package com.metinform.common.monitor;

import java.util.NoSuchElementException;

import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.impl.GenericKeyedObjectPoolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.metinform.common.BaseConnector;
import com.metinform.common.ConnectorPool;
import com.simple.datax.core.model.connpara.CommonConnPara;

public class ReceiveConnectorPool
        extends ConnectorPool {
    private static final Logger logger = LoggerFactory.getLogger(ReceiveConnectorPool.class);
    private static KeyedObjectPool<String, BaseConnector> internal_pool = null;
    private static ReceiveConnectorPool pool = new ReceiveConnectorPool();

    public static synchronized ReceiveConnectorPool getInstance() {
        if (internal_pool == null) {
            GenericKeyedObjectPoolFactory<String, BaseConnector> factory = new GenericKeyedObjectPoolFactory(
                    ConnectorFactory.getInstance(), 1, (byte) 0, -1L);
            internal_pool = factory.createPool();
        }
        return pool;
    }

    @Override
    public BaseConnector borrowObject(CommonConnPara connPara) {
        try {
            logger.debug("ReceiveConnectorPool.borrowObject - key:" + getConnKey(connPara.getTransportType(), connPara.getTransportPara()));
            BaseConnector connector = (BaseConnector) internal_pool.borrowObject(getConnKey(connPara.getTransportType(), connPara.getTransportPara()));
            if (connector != null) {
                connector.setEncoding(connPara.getEncoding());
            }
            return connector;
        } catch (NoSuchElementException ex) {
            logger.warn("No available connector in pool - " + connPara + ": " + ex.getMessage());
            return null;
        } catch (IllegalStateException ex) {
            logger.warn("ReceiveConnectorPool in illegal status", ex);
            return null;
        } catch (Exception ex) {
            logger.warn("makeObject error", ex);
        }
        return null;
    }

    @Override
    public void returnObject(BaseConnector connector) {
        try {
            internal_pool.returnObject(getConnKey(connector.getConnType(), connector.getConnPara()), connector);
            logger.debug("ReceiveConnectorPool.returnObject - " + getConnKey(connector.getConnType(), connector.getConnPara()));
        } catch (Exception ex) {
            logger.debug("returnObject error", ex);
        }
    }
}



