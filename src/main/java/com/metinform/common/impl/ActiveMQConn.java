package com.metinform.common.impl;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.metinform.common.monitor.MQBaseConnector;
import com.simple.datax.SimpleMessage;
import com.simple.datax.api.ConnectorException;

public class ActiveMQConn
        extends MQBaseConnector {
    private static final Logger logger = LoggerFactory.getLogger(ActiveMQConn.class);
    public static final String TYPE = "ActiveMQ";
    private String username;
    private String password;
    private String brokeURL;
    private Connection connection;
    private Session session;
    private String subject;
    private Destination destination = null;
    private MessageConsumer consumer = null;
    private MessageProducer producer = null;

    public ActiveMQConn(String connPara)
            throws ConnectorException {
        super(connPara);
        this.connPara = connPara;
        parseConnPara();
        createMQ();
    }

    private void createMQ()
            throws ConnectorException {
        try {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(username, password, brokeURL);

            connection = connectionFactory.createConnection();

            session = connection.createSession(false, 1);
            connection.start();
        } catch (JMSException e) {
            throw new ConnectorException("Creating ActiveMQ failed - brokeURL" + brokeURL, e);
        }
    }

    private void parseConnPara()
            throws ConnectorException {
        String[] tmpStr = splitConnPara(connPara);
        if (tmpStr.length == 4) {
            brokeURL = tmpStr[0];
            username = tmpStr[1];
            password = tmpStr[2];
            subject = tmpStr[3];
        } else {
            throw new ConnectorException("Parsing ActiveMQ connector parameters failed. Please check parameter - " + connPara);
        }
    }

    @Override
    public String getMessage()
            throws ConnectorException {
        String ret = null;
        prepareReceiveQueue();
        try {
            Message message = consumer.receive(2000L);
            if ((message instanceof TextMessage)) {
                TextMessage textMsg = (TextMessage) message;
                if (textMsg != null) {
                    ret = textMsg.getText();
                    ret = getInputString(ret, getEncoding());
                }
            }
            if ((message instanceof BytesMessage)) {
                BytesMessage bytesMsg = (BytesMessage) message;
                if (bytesMsg != null) {
                    ret = getMessage(bytesMsg, getEncoding());
                }
            }
        } catch (Exception ex) {
            throw new ConnectorException("Receive message from ActiveMQ failed.", ex);
        }
        return ret;
    }

    private void prepareSendQueue()
            throws ConnectorException {
        try {
            if ((destination == null) || (producer == null)) {
                destination = null;
                destination = session.createQueue(subject);
                producer = null;
                producer = session.createProducer(destination);
            }
        } catch (Exception e) {
            throw new ConnectorException("ActiveMQ producer initialization failed", e);
        }
    }

    private void prepareReceiveQueue()
            throws ConnectorException {
        try {
            if ((destination == null) || (consumer == null)) {
                destination = null;
                destination = session.createQueue(subject);
                consumer = null;
                consumer = session.createConsumer(destination);
            }
        } catch (Exception e) {
            throw new ConnectorException("ActiveMQ producer initialization failed", e);
        }
    }

    @Override
    public String getConnType() {
        return "ActiveMQ";
    }

    @Override
    public void setMessageListener(MessageListener listener)
            throws ConnectorException {
        try {
            prepareReceiveQueue();
            consumer.setMessageListener(listener);
        } catch (Exception e) {
            throw new ConnectorException("Setting ActiveMQ listener failed.", e);
        }
    }

    @Override
    protected void prepareSend()
            throws ConnectorException {
        prepareSendQueue();
    }

    @Override
    protected void internalSend(SimpleMessage message)
            throws ConnectorException {
        try {
            BytesMessage msg = session.createBytesMessage();
            msg.writeBytes(getOutputBytes(message.getContent(), getEncoding()));
            producer.send(msg);
        } catch (JMSException ex) {
            throw new ConnectorException("ActiveMQ internalSend failed", ex, true);
        } catch (Exception ex) {
            throw new ConnectorException("ActiveMQ internalSend failed", ex);
        }
    }

    @Override
    public void disconnect() {
        if (producer != null) {
            try {
                producer.close();
            } catch (JMSException e) {
                logger.warn("ActiveMQ producer disconnect error", e);
            }
            producer = null;
        }
        if (consumer != null) {
            try {
                consumer.close();
            } catch (JMSException e) {
                logger.warn("ActiveMQ consumer disconnect error", e);
            }
            consumer = null;
        }
        if (session != null) {
            try {
                session.close();
            } catch (JMSException e) {
                logger.warn("ActiveMQ session disconnect error", e);
            }
            session = null;
        }
        if (connection != null) {
            try {
                connection.stop();
            } catch (Exception e) {
                logger.warn("ActiveMQ connection disconnect error", e);
            }
            connection = null;
        }
    }

    @Override
    protected void prepareReceive()
            throws ConnectorException {
        prepareReceiveQueue();
    }
}



