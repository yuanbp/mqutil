package com.metinform.common.monitor;

import java.io.UnsupportedEncodingException;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageListener;

import com.metinform.common.BaseConnector;
import com.simple.datax.api.ConnectorException;

public abstract class MQBaseConnector
        extends BaseConnector {
    public MQBaseConnector(String connPara)
            throws ConnectorException {
        super(connPara);
    }

    @Override
    public String getMessage()
            throws ConnectorException {
        return null;
    }

    @Override
    public String getConnType() {
        return null;
    }

    public abstract void setMessageListener(MessageListener paramMessageListener)
            throws ConnectorException;

    public String getMessage(BytesMessage bytesMsg, String encoding)
            throws JMSException, UnsupportedEncodingException {
        if (bytesMsg != null) {
            long size = bytesMsg.getBodyLength();
            byte[] bytes = new byte[1024];
            byte[] ret = new byte[(int) size];
            int length = -1;
            int pos = 0;
            while ((length = bytesMsg.readBytes(bytes)) > -1) {
                System.arraycopy(bytes, 0, ret, pos, length);
                pos += length;
            }
            return getInputString(ret, encoding);
        }
        return null;
    }
}



