package com.metinform.common.impl;

import java.io.UnsupportedEncodingException;

import javax.jms.MessageListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.metinform.common.monitor.MQBaseConnector;
import com.simple.datax.SimpleMessage;
import com.simple.datax.api.ConnectorException;

import ionic.Msmq.Message;
import ionic.Msmq.MessageQueueException;
import ionic.Msmq.Queue;
import ionic.Msmq.TransactionType;

public class MSMQConn
        extends MQBaseConnector {
    private static final Logger logger = LoggerFactory.getLogger(MSMQConn.class);
    public static final String TYPE = "MSMQ";
    private Queue queue = null;
    private TransactionType type = null;
    private final int timeout = 2000;
    private String conn;
    private long recvTotal;
    private long sendTotal;

    public MSMQConn(String connPara)
            throws ConnectorException {
        super(connPara);
        this.connPara = connPara;
        parseConnPara();
    }

    private void parseConnPara()
            throws ConnectorException {
        String[] tmp = splitConnPara(connPara);
        if (tmp.length == 2) {
            int i = Integer.valueOf(tmp[0]).intValue();
            switch (i) {
                case 1:
                    type = TransactionType.None;
                    break;
                case 2:
                    type = TransactionType.SINGLE_MESSAGE;
                    break;
                case 3:
                    type = TransactionType.XA;
                    break;
                case 4:
                    type = TransactionType.MTS;
            }
            conn = tmp[1];
        } else {
            throw new ConnectorException("Connector failed");
        }
    }

    @Override
    public String getMessage()
            throws ConnectorException {
        String ret = null;
        prepareReceive();
        try {
            Message msg = queue.receive(2000);

            ret = new String(msg.getBody(), getEncoding());

            logger.debug("########Receiver Get Message: #" + ++recvTotal + " - " + connPara);
        } catch (MessageQueueException ex) {
            if (ex.hresult == -1072824293) {
                logger.warn("Receive timeout");
            } else {
                logger.warn("Receive failed", ex);


                queue = null;
                prepareReceive();
                try {
                    Message msg = queue.receive(2000);

                    ret = new String(msg.getBody(), getEncoding());
                    logger.debug("########Receiver Get Message: #" + ++recvTotal + " - " + connPara);
                } catch (MessageQueueException e) {
                    logger.error("Receive re-try failded", e);
                } catch (UnsupportedEncodingException e) {
                    logger.error("########Receiver Get Message Failed: #" + ++recvTotal + " - " + connPara);
                    throw new ConnectorException("Get message from MSMQ failed", ex);
                }
            }
        } catch (UnsupportedEncodingException ex) {
            logger.info("########Receiver Get Message Failed: #" + ++recvTotal + " - " + connPara);
            throw new ConnectorException("Get message from MSMQ failed", ex);
        }
        return ret;
    }

    private void prepareQueue(boolean isSend)
            throws ConnectorException {
        try {
            if (queue == null) {
                if (isSend) {
                    queue = new Queue(conn, Queue.Access.SEND);
                } else {
                    queue = new Queue(conn, Queue.Access.RECEIVE);
                }
                logger.debug("open: OK.");
            }
        } catch (MessageQueueException ex1) {
            throw new ConnectorException("Open queue failed", ex1);
        }
    }

    private void send(String message)
            throws MessageQueueException, UnsupportedEncodingException {
        try {
            Message msg = new Message(message.getBytes(getEncoding()), "label", "L:none");
            queue.send(msg, type);
            logger.debug("Sent out message");
        } catch (MessageQueueException ex) {
            throw ex;
        } catch (UnsupportedEncodingException ex) {
            throw ex;
        }
    }

    @Override
    protected void prepareSend()
            throws ConnectorException {
        prepareQueue(true);
    }

    @Override
    protected void internalSend(SimpleMessage message)
            throws ConnectorException {
        try {
            send(message.getContent());
            logger.debug("########Sender Sent Message: #" + ++sendTotal + " - " + connPara);
        } catch (MessageQueueException ex) {
            logger.error("########Sender Sent Message Failed: #" + ++sendTotal + " - " + connPara, ex);
            throw new ConnectorException("MSMQ internalSend failed", ex, true);
        } catch (Exception ex) {
            logger.error("########Sender Sent Message Failed: #" + ++sendTotal + " - " + connPara, ex);
            throw new ConnectorException("MSMQ internalSend failed", ex);
        }
    }

    @Override
    public void setMessageListener(MessageListener listener)
            throws ConnectorException {
    }

    @Override
    public String getConnType() {
        return "MSMQ";
    }

    @Override
    public void disconnect() {
        if (queue != null) {
            try {
                queue.close();
            } catch (MessageQueueException e) {
                logger.warn("MSMQ disconnect error", e);
            }
            queue = null;
        }
    }

    @Override
    protected void prepareReceive()
            throws ConnectorException {
        prepareQueue(false);
    }
}



