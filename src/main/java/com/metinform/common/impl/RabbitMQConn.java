package com.metinform.common.impl;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import javax.jms.MessageListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.metinform.common.monitor.MQBaseConnector;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.MessageProperties;
import com.simple.datax.SimpleMessage;
import com.simple.datax.api.ConnectorException;

public class RabbitMQConn
        extends MQBaseConnector {
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQConn.class);
    public static final String TYPE = "RabbitMQ";
    private String url;
    private String queueName;
    private Connection connection;
    private Channel channel;

    public RabbitMQConn(String connPara)
            throws ConnectorException {
        super(connPara);
        this.connPara = connPara;
        parseConnPara();
        createMQ();
    }

    private void createMQ()
            throws ConnectorException {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUri(url);
            connection = factory.newConnection();
        } catch (Exception e) {
            throw new ConnectorException("Creating RabbitMQ failed - brokeURL" + url, e);
        }
    }

    private void parseConnPara()
            throws ConnectorException {
        String[] tmpStr = splitConnPara(connPara);
        if (tmpStr.length == 2) {
            url = tmpStr[0];
            queueName = tmpStr[1];
        } else {
            throw new ConnectorException("Parsing RabbitMQ connector parameters failed. Please check parameter - " + connPara);
        }
    }

    @Override
    public String getMessage()
            throws ConnectorException {
        String ret = null;
        try {
            prepareReceiveQueue();
            boolean autoAck = false;
            GetResponse response = channel.basicGet(queueName, autoAck);
            if (response == null) {
                logger.info("No message retrieved.");
            } else {
                byte[] body = response.getBody();
                ret = getInputString(body, getEncoding());
                long deliveryTag = response.getEnvelope().getDeliveryTag();
                channel.basicAck(deliveryTag, false);
            }
        } catch (IOException e) {
            throw new ConnectorException("getMessage RabbitMQ failed " + url, e);
        }
        return ret;
    }

    private void prepareSendQueue()
            throws ConnectorException {
        try {
            channel = connection.createChannel();
        } catch (IOException e) {
            throw new ConnectorException("Creating RabbitMQ failed - createChannel" + url, e);
        }
    }

    private void prepareReceiveQueue()
            throws ConnectorException {
        try {
            channel = connection.createChannel();
        } catch (IOException e) {
            throw new ConnectorException("Creating RabbitMQ failed - createChannel" + url, e);
        }
    }

    @Override
    public String getConnType() {
        return "RabbitMQ";
    }

    @Override
    public void setMessageListener(MessageListener listener)
            throws ConnectorException {
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
            prepareSend();
            byte[] messageBodyBytes = getOutputBytes(message.getContent(), getEncoding());
            channel.basicPublish("", queueName, MessageProperties.TEXT_PLAIN, messageBodyBytes);
        } catch (UnsupportedEncodingException ex) {
            throw new ConnectorException("RabbitMq internalSend failed", ex, true);
        } catch (IOException ex) {
            throw new ConnectorException("RabbitMq internalSend failed", ex, true);
        }
    }

    @Override
    public void disconnect() {
    }

    @Override
    protected void prepareReceive()
            throws ConnectorException {
    }
}



