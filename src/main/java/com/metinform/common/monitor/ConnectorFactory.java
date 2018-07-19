package com.metinform.common.monitor;

import org.apache.commons.pool.BaseKeyedPoolableObjectFactory;
import org.apache.log4j.Logger;

import com.metinform.common.BaseConnector;
import com.metinform.common.impl.ActiveMQConn;
import com.metinform.common.impl.FTPConn;
import com.metinform.common.impl.IBMMQConn;
import com.metinform.common.impl.LocalFileConn;
import com.metinform.common.impl.MSMQConn;
import com.metinform.common.impl.RabbitMQConn;
import com.metinform.common.impl.SFTPConn;
import com.simple.datax.api.ConnectorException;

public class ConnectorFactory
        extends BaseKeyedPoolableObjectFactory<String, BaseConnector> {
    private static Logger logger = Logger.getLogger(SendConnectorPool.class);
    public static final String KEY_SPLITTER = ":";
    private static ConnectorFactory factory = null;

    public static synchronized ConnectorFactory getInstance() {
        if (factory == null) {
            factory = new ConnectorFactory();
        }
        return factory;
    }

    @Override
    public BaseConnector makeObject(String key)
            throws Exception {
        BaseConnector connector = null;
        if ((key != null) && (!key.isEmpty())) {
            int pos = key.indexOf(":");
            if (pos > 0) {
                String connType = key.substring(0, pos);
                if (connType != null) {
                    connType = connType.trim();
                }
                String connPara = key.substring(pos + 1);
                if ("FTP".equalsIgnoreCase(connType)) {
                    connector = new FTPConn(connPara);
                } else if ("FTP".equalsIgnoreCase(connType)) {
                    connector = new FTPConn(connPara, true);
                } else if ("ActiveMQ".equalsIgnoreCase(connType)) {
                    connector = new ActiveMQConn(connPara);
                } else if ("IBMMQ".equalsIgnoreCase(connType)) {
                    connector = new IBMMQConn(connPara);
                } else if ("MSMQ".equalsIgnoreCase(connType)) {
                    connector = new MSMQConn(connPara);
                } else if ("FILE".equalsIgnoreCase(connType)) {
                    connector = new LocalFileConn(connPara);
                } else if ("SFTP".equalsIgnoreCase(connType)) {
                    connector = new SFTPConn(connPara);
                } else if ("RabbitMQ".equalsIgnoreCase(connType)) {
                    connector = new RabbitMQConn(connPara);
                } else {
                    logger.error("Unsupported connector type - " + connType);
                    throw new ConnectorException("Unsupported connector type - " + connType);
                }
            }
        }
        return connector;
    }
}



