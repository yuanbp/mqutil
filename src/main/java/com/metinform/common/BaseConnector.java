package com.metinform.common;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.log4j.Logger;

import com.metinform.common.monitor.SendThreadPoolExecutor;
import com.metinform.common.process.IDataProcess;
import com.simple.datax.SimpleFailMessage;
import com.simple.datax.SimpleMessage;
import com.simple.datax.SimpleObject;
import com.simple.datax.api.Connector;
import com.simple.datax.api.ConnectorException;
import com.simple.datax.core.model.connpara.CommonConnPara;
import com.simple.datax.service.impl.SendStatusHandler;
import com.simple.datax.utils.MessageUtils;
import com.simple.datax.utils.SimpleConstants;

public abstract class BaseConnector
        implements Connector {
    private static Logger logger = Logger.getLogger(BaseConnector.class);
    protected static final int RECV_NO_MESSAGE_TIMOUT = 2000;
    protected String msgType = "EC";
    protected String msgFormat = "XML";
    protected String encoding = "UTF-8";
    protected ConcurrentLinkedQueue<SimpleObject> messageList = new ConcurrentLinkedQueue();
    protected ConcurrentLinkedQueue<SimpleObject> newMessageList = new ConcurrentLinkedQueue();
    public static final String TYPE = "BaseConnector";
    protected SendStatusHandler handler;
    protected String connPara;
    private static final char SPLITTER = '$';
    private static final char ESCAPE = '\\';
    protected SendExecutor executor;
    protected SendThreadPoolExecutor poolExecutor;
    protected ThreadPoolExecutor recvThreadPoolExecutor;
    private long total;
    private long successTotal;

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public String getMsgType() {
        return msgType;
    }

    public void setMsgType(String msgType) {
        this.msgType = msgType;
    }

    public String getMsgFormat() {
        return msgFormat;
    }

    public void setMsgFormat(String msgFormat) {
        this.msgFormat = msgFormat;
    }

    public BaseConnector(String connPara)
            throws ConnectorException {
        if (connPara == null) {
            throw new ConnectorException("Connetion Parameter String is NULL. ");
        }
    }

    public boolean recvMessageWithThreads(IDataProcess dataProcess, CommonConnPara para) {
        boolean bResult = false;
        try {
            int queueMsgSize = recvThreadPoolExecutor.getQueue().size();
            logger.debug("Current Thread pool queue size is " + queueMsgSize);
            String queuecapacityStr = PropertiesUtils.getProperty("recv.thread.pool.executor.queuecapacity");
            int queuecapacity = 100;
            if ((queuecapacityStr != null) && (!"".equals(queuecapacityStr))) {
                queuecapacity = Integer.parseInt(queuecapacityStr);
            }
            if (queuecapacity - queueMsgSize > 0) {
                String message = getMessage();
                if (message != null) {
                    logger.debug("New Thread To Process!");
                    recvThreadPoolExecutor.execute(new DataProcThread(dataProcess, message, para));
                    logger.debug("Data processed!");
                    bResult = true;
                }
            } else {
                logger.debug("#################Skip Add Thread!");
            }
        } catch (ConnectorException e) {
            e.printStackTrace();
            logger.error("BaseConnector::recvMessageWithThreads error==>" + e.toString());
        } catch (Exception e) {
            logger.error("Reciver get message error. Para:" + para.getTransportType() + ":" + para.getTransportPara(), e);
        }
        return bResult;
    }

    @Override
    public boolean send(SimpleObject object) {
        if ((object instanceof SimpleMessage)) {
            synchronized (newMessageList) {
                newMessageList.add(object);
                logger.debug("Call send - " + ((SimpleMessage) object).toLog());
                logger.debug("===message cout in queue: " + newMessageList.size());
                if (poolExecutor == null) {
                    poolExecutor = SendThreadPoolExecutor.getInstance();
                }
                executor = new SendExecutor(poolExecutor, connPara);
                poolExecutor.execute(executor, connPara);
            }
        }
        return true;
    }

    public void testSendConnect()
            throws Exception {
        prepareSend();
    }

    public void testRecvConnect()
            throws Exception {
        prepareReceive();
    }

    protected abstract void prepareReceive()
            throws ConnectorException;

    protected abstract void internalSend(SimpleMessage paramSimpleMessage)
            throws ConnectorException;

    protected abstract void prepareSend()
            throws ConnectorException;

    public abstract String getConnType();

    private class SendExecutor
            implements Runnable {
        private SendThreadPoolExecutor executor;
        private String connPara;

        public SendExecutor(SendThreadPoolExecutor executor, String connPara) {
            this.executor = executor;
            this.connPara = connPara;
        }

        @Override
        public void run() {
            synchronized (newMessageList) {
                messageList.addAll(newMessageList);
                newMessageList.clear();
            }
            for (; ; ) {
                BaseConnector.logger.debug(" start executor run:");
                SimpleMessage src = null;
                try {
                    long start = System.currentTimeMillis();
                    prepareSend();
                    long end = System.currentTimeMillis();
                    BaseConnector.logger.debug("prepareSend time ===========:" + (end - start) + "ms.");
                } catch (Exception e) {
                    BaseConnector.logger.error("Send Connector initialization failed.", e);

                    messageList.clear();
                    executor.workDone(connPara);
                    break;
                }
                for (SimpleObject object : messageList) {
                    try {
                        src = (SimpleMessage) object;
                        total += 1L;
                        BaseConnector.logger.debug("Begin to Send Msg================:" + src.toLog());
                        internalSend(src);
                        successTotal += 1L;
                        BaseConnector.logger.debug("Sent successed - " + src.toLog());
                        if (handler == null) {
                            handler = SendStatusHandler.getInstance();
                        }
                        handler.callSuccessService(src);
                    } catch (ConnectorException ex) {
                        if (!ex.isNetworkIssue()) {
                            handleError(ex, src);
                            BaseConnector.logger.error("Failed to send message caused by other issue." + src.toLog(), ex);
                        } else {
                            BaseConnector.logger.debug(
                                    "Failed to send message casued by network issue. Will re-init channel and try again." +
                                            src.toLog(), ex);
                            try {
                                disconnect();
                                prepareSend();
                                internalSend(src);


                                BaseConnector.logger.debug("ReSent successed - " + src.toLog());
                                if (handler == null) {
                                    handler = SendStatusHandler.getInstance();
                                }
                                handler.callSuccessService(src);
                            } catch (ConnectorException e) {
                                if (!e.isNetworkIssue()) {
                                    handleError(ex, src);
                                } else {
                                    BaseConnector.logger.warn("Wait to re-send - " + src.toLog(), ex);
                                }
                            } catch (Exception e) {
                                handleError(e, src);
                            }
                        }
                    } catch (Exception ex) {
                        handleError(ex, src);
                    }
                }
                messageList.clear();
                synchronized (newMessageList) {
                    if ((newMessageList == null) || (newMessageList.size() == 0)) {
                        executor.workDone(connPara);
                    } else {
                        messageList.addAll(newMessageList);
                        newMessageList.clear();
                    }
                }
            }
        }
    }

    public String getConnPara() {
        return connPara;
    }

    public void setConnPara(String connPara) {
        this.connPara = connPara;
    }

    public SendStatusHandler getHandler() {
        return handler;
    }

    public void setHandler(SendStatusHandler handler) {
        this.handler = handler;
    }

    private void handleError(Exception ex, SimpleMessage src) {
        logger.error("Fail to send out message.", ex);
        String errorinfo = ex.getMessage();
        try {
            SimpleFailMessage sfm = MessageUtils.castFailMessage(errorinfo, SimpleConstants.ERROR_TYPE_SEND, SimpleConstants.ERROR_LEVEL_NORMAL, src);
            handler.callFailService(sfm);
        } catch (Exception e) {
            logger.error("Sender FailMessage Error:", e);
        }
        try {
            int responseCode = SimpleConstants.RESPONSE_CODE_SENDER;
            String responseMsg = SimpleConstants.RESPONSE_MSG_SENDER;
            handler.callResponseService(src, responseCode, responseMsg, src.isNeedResponse());
        } catch (Exception e) {
            logger.error("Sender FailMessage Response Error:", e);
        }
    }

    public static String[] splitConnPara(String connPara)
            throws ConnectorException {
        if (connPara == null) {
            throw new ConnectorException("Connection Parameter String is NULL. ");
        }
        List<String> retList = new ArrayList();
        char b = '\000';
        StringBuilder sb = new StringBuilder();
        int duplicate = 1;
        int length = connPara.length();
        if (length == 0) {
            return new String[]{""};
        }
        for (int i = 0; i < length; i++) {
            b = connPara.charAt(i);
            if (b == '$') {
                retList.add(sb.toString());
                sb.delete(0, sb.length());
            } else if (b == '\\') {
                duplicate = 1;
                while ((i < length - 1) && (connPara.charAt(i + 1) == '\\')) {
                    duplicate++;
                    i++;
                }
                for (int j = 0; j < duplicate / 2; j++) {
                    sb.append('\\');
                }
                if (duplicate % 2 == 1) {
                    if (i < length - 1) {
                        if (connPara.charAt(i + 1) == '$') {
                            sb.append('$');
                            i++;
                        } else {
                            throw new ConnectorException(
                                    "Escape char \"\\\" needs double if it is used as normal char. ");
                        }
                    } else {
                        throw new ConnectorException("Escape char \"\\\" needs double if it is used as normal char. ");
                    }
                }
            } else {
                sb.append(b);
            }
        }
        if (sb.length() > 0) {
            retList.add(sb.toString());
            sb.delete(0, sb.length());
        }
        if (b == '$') {
            retList.add(sb.toString());
        }
        return (String[]) retList.toArray(new String[retList.size()]);
    }

    public SendThreadPoolExecutor getPoolExecutor() {
        return poolExecutor;
    }

    public void setPoolExecutor(SendThreadPoolExecutor poolExecutor) {
        this.poolExecutor = poolExecutor;
    }

    public long getTotal() {
        return total;
    }

    public long getSuccessTotal() {
        return successTotal;
    }

    public static byte[] getOutputBytes(String str, String encoding)
            throws UnsupportedEncodingException {
        if ((str == null) || (str.trim().isEmpty())) {
            return null;
        }
        return str.getBytes(encoding);
    }

    public static String getInputString(byte[] bytes, String encoding)
            throws UnsupportedEncodingException {
        if ((bytes == null) || (bytes.length == 0)) {
            return null;
        }
        return new String(bytes, encoding);
    }

    public static String getOutputString(String str, String encoding)
            throws UnsupportedEncodingException {
        if ((str == null) || (str.trim().isEmpty())) {
            return str;
        }
        return new String(str.getBytes(), encoding);
    }

    public static String getInputString(String str, String encoding)
            throws UnsupportedEncodingException {
        if ((str == null) || (str.trim().isEmpty())) {
            return str;
        }
        return new String(str.getBytes(encoding));
    }

    public ThreadPoolExecutor getRecvThreadPoolExecutor() {
        return recvThreadPoolExecutor;
    }

    public void setRecvThreadPoolExecutor(ThreadPoolExecutor recvThreadPoolExecutor) {
        this.recvThreadPoolExecutor = recvThreadPoolExecutor;
    }
}



