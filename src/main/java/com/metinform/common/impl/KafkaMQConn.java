package com.metinform.common.impl;

import javax.jms.MessageListener;

import com.metinform.common.monitor.MQBaseConnector;
import com.simple.datax.SimpleMessage;
import com.simple.datax.api.ConnectorException;

public class KafkaMQConn
        extends MQBaseConnector {
    public KafkaMQConn(String connPara)
            throws ConnectorException {
        super(connPara);
    }

    @Override
    public void disconnect() {
    }

    @Override
    public void setMessageListener(MessageListener listener)
            throws ConnectorException {
    }

    @Override
    protected void prepareReceive()
            throws ConnectorException {
    }

    @Override
    protected void internalSend(SimpleMessage message)
            throws ConnectorException {
    }

    @Override
    protected void prepareSend()
            throws ConnectorException {
    }
}



