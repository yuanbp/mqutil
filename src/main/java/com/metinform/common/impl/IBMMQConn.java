package com.metinform.common.impl;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import javax.jms.MessageListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.mq.MQC;
import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.metinform.common.monitor.MQBaseConnector;
import com.simple.datax.SimpleMessage;
import com.simple.datax.api.ConnectorException;

public class IBMMQConn
        extends MQBaseConnector {
    private static final Logger logger = LoggerFactory.getLogger(IBMMQConn.class);
    public static final String TYPE = "IBMMQ";
    private String host;
    private String port;
    private String ccsid;
    private String channel;
    private MQQueue queue;
    private String manager;
    private String queuename;
    private String username;
    private String password;
    private MQQueueManager qMgr;
    private MQGetMessageOptions mgo;
    private MQPutMessageOptions mqPutMessageOptions;
    private Hashtable<String, Object> prop;
    private long recvTotal;
    private long sendTotal;
    public static int cbsize = 1024 * 1024 * 2; // 切包的大小2M

    public IBMMQConn(String connPara)
            throws ConnectorException {
        super(connPara);
        this.connPara = connPara;
        try {
            parseConnPara();
            createMQ();
        } catch (Exception e) {
            throw new ConnectorException("Create IBMMQConnImpl failed", e);
        }
    }

    private void createMQ()
            throws Exception {
        prop = new Hashtable();
        prop.put("port", new Integer(port));
        prop.put("hostname", host);
        prop.put("channel", channel);
        prop.put("CCSID", new Integer(ccsid));
        if (!username.trim().isEmpty()) {
            prop.put("userID", username);
            prop.put("password", password.trim());
        }
    }

    @Override
    public void setMessageListener(MessageListener listener)
            throws ConnectorException {
    }

    private void prepareReceiveQueue()
            throws ConnectorException {
        if ((qMgr == null) || (queue == null) || (!qMgr.isOpen()) || (!qMgr.isConnected()) || (!queue.isOpen())) {
            disconnect();
            try {
                qMgr = new MQQueueManager(manager, prop);
                int openOptions = 8225;
                queue = qMgr.accessQueue(queuename, openOptions);
                mgo = new MQGetMessageOptions();
                mgo.options |= 0x0;
            } catch (MQException e) {
                throw new ConnectorException("IBMMQ initialize failed", e);
            }
        }
    }

    @Override
    public String getMessage()
            throws ConnectorException {
        String ret = null;
        prepareReceiveQueue();
        //MQMessage msg = new MQMessage();
        String msg = "";
        try {
            // 取消息
            /*queue.get(msg, mgo);
            if (msg != null) {
				byte[] xmlData = new byte[msg.getDataLength()];
				msg.readFully(xmlData);
				ret = new String(xmlData, getEncoding());
			}*/

            boolean isLastSegment = false;
            while (!isLastSegment) {
                MQMessage myMsg = new MQMessage();
                queue.get(myMsg, mgo);
                if (myMsg.messageFlags == MQC.MQMF_SEGMENT + MQC.MQMF_LAST_SEGMENT || myMsg.messageFlags == MQC.MQMF_NONE) {
                    isLastSegment = true;
                }
                byte[] b = new byte[myMsg.getMessageLength()];
                myMsg.readFully(b);
                msg += new String(b, encoding).trim();
            }
            qMgr.commit();
            ret = msg;
            logger.debug("########Receiver Get Message: #" + ++recvTotal + " - " + connPara);
        } catch (MQException ex) {
            // Reason Code - 2033 表示队列中没有数据
            if (ex.reasonCode != 2033 && ex.reasonCode != 2016) {
                logger.error("########Receiver Get Message Failed: reasonCode: #" + ex.reasonCode + " - " + + ++recvTotal + " - " + connPara);
                throw new ConnectorException("Read messge from MQ failed.", ex);
            } else {
                logger.error("########Receiver Get Message Failed[No Message Or Prohibit Taking Out!]: reasonCode: #" + ex.reasonCode + " - " + + ++recvTotal + " - " + connPara);
                //throw new ConnectorException("Read messge from MQ failed.", ex);
            }
        } catch (Exception e) {
            logger.error("########Receiver Get Messag Failed: #" + ++recvTotal + " - " + connPara);
            throw new ConnectorException("Read messge from MQ failed.", e);
        }
        return ret;
    }

    private void prepareSendQueue()
            throws ConnectorException {
        if ((qMgr == null) || (queue == null) || (!qMgr.isOpen()) || (!qMgr.isConnected()) || (!queue.isOpen())) {
            disconnect();
            try {
                int sendOptions = 8208;
                qMgr = new MQQueueManager(manager, prop);
                queue = qMgr.accessQueue(queuename, sendOptions, null, null, null);
                mqPutMessageOptions = new MQPutMessageOptions();
            } catch (MQException e) {
                throw new ConnectorException("IBMMQ initialize failed", e);
            }
        }
    }

    @Override
    protected void internalSend(SimpleMessage message)
            throws ConnectorException {
        MQMessage mqMessage = null;
        try {
            logger.debug("########Sender Begin=========Sent Message:  - " + connPara + " ---- " + message.toLog());
            /*mqMessage = new MQMessage();
            mqMessage.characterSet = Integer.parseInt(ccsid);
			mqMessage.format = MQC.MQFMT_STRING;
			mqMessage.write(getOutputBytes(message.getContent(), getEncoding()));
			queue.put(mqMessage, mqPutMessageOptions);
			qMgr.commit();
			*/

            List<byte[]> bys = getSegments(message.getContent());
            MQPutMessageOptions pmo = new MQPutMessageOptions();
            pmo.options = MQC.MQPMO_LOGICAL_ORDER + MQC.MQPMO_SYNCPOINT;
            for (int i = 0; i < bys.size(); i++) {
                MQMessage msg = new MQMessage();
                msg.encoding = Integer.parseInt("546");
                msg.characterSet = Integer.parseInt(ccsid);
                msg.format = MQC.MQFMT_STRING;
                if (bys.size() > 1) {//多个消息时分片
                    if (i == bys.size() - 1) {
                        msg.messageFlags = MQC.MQMF_LAST_SEGMENT;
                    } else {
                        msg.messageFlags = MQC.MQMF_SEGMENT;
                    }
                }
                msg.write(bys.get(i));
                queue.put(msg, pmo);
            }
            qMgr.commit();


            logger.debug("########Sender Sent Message: #" + ++sendTotal + " - " + connPara);
        } catch (MQException ex) {
            logger.error("########Sender Sent Message Failed: #" + ++sendTotal + " - " + connPara);
            throw new ConnectorException("IBMMQ internalSend failed", ex, true);
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("########Sender Sent Message Failed: #" + ++sendTotal + " - " + connPara);
            throw new ConnectorException("IBMMQ internalSend failed", ex);
        }
    }

    public List<byte[]> getSegments(String msg) throws UnsupportedEncodingException {
        List<byte[]> msgs = new ArrayList<>();
        byte[] all = msg.getBytes(encoding);
        if (all.length <= cbsize) {
            msgs.add(all);
            return msgs;
        } else {
            byte[] bs = null;
            for (int i = 0; i < all.length; i++) {
                int k = i % cbsize;
                if (k == 0) {
                    if (all.length - i > cbsize) {
                        bs = new byte[cbsize];
                    } else {
                        bs = new byte[all.length - i];
                    }
                    msgs.add(bs);
                }
                bs[k] = all[i];
            }
        }

        return msgs;
    }

    private void parseConnPara()
            throws ConnectorException {
        String[] tmpStr = splitConnPara(connPara);
        if (tmpStr.length == 8) {
            host = tmpStr[0];
            port = tmpStr[1];
            if (!port.matches("[\\d]+")) {
                throw new ConnectorException("IBMMQ Connector Port is not number -" + port);
            }
            username = tmpStr[2];
            password = tmpStr[3];
            ccsid = tmpStr[4];
            if (!ccsid.matches("[\\d]+")) {
                throw new ConnectorException("IBMMQ Connector CCSID is not number - " + ccsid);
            }
            manager = tmpStr[5];
            channel = tmpStr[6];
            queuename = tmpStr[7];
        } else {
            throw new ConnectorException("Parsing IBMMQ Connector parameters failed. Please check parameter - " +
                    connPara);
        }
    }

    @Override
    public String getConnType() {
        return "IBMMQ";
    }

    @Override
    protected void prepareSend()
            throws ConnectorException {
        prepareSendQueue();
    }

    public static void main(String[] args) {
        String connPara = "10.2.3.20$1502$ $ $1381$QMLTSC$JAVA.CHANNEL$QECA.RECE.LOCAL";
        try {
            IBMMQConn conn = new IBMMQConn(connPara);
            SimpleMessage object = new SimpleMessage();
            object.setContent("test_test");

            conn.send(object);
        } catch (ConnectorException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
    }

    @Override
    public void disconnect() {
        if (queue != null) {
            try {
                queue.close();
            } catch (MQException e) {
                logger.warn("IBMMQ disconnect error", e);
            }
            queue = null;
        }
        if (qMgr != null) {
            try {
                qMgr.disconnect();
            } catch (MQException e) {
                logger.warn("IBMMQ disconnect error", e);
            }
            try {
                qMgr.close();
            } catch (MQException e) {
                logger.warn("IBMMQ disconnect error", e);
            }
            qMgr = null;
        }
    }

    @Override
    protected void prepareReceive()
            throws ConnectorException {
        prepareReceiveQueue();
    }
}



