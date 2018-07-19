package com.metinform.common.loader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.metinform.common.BaseConnector;
import com.metinform.common.ConnectorPool;
import com.metinform.common.monitor.ConnectorPoolManager;
import com.metinform.common.monitor.SendThreadPoolExecutor;
import com.simple.datax.SimpleFailMessage;
import com.simple.datax.SimpleMessage;
import com.simple.datax.SimpleObject;
import com.simple.datax.api.ConnectorException;
import com.simple.datax.api.Sender;
import com.simple.datax.core.model.connpara.CommonConnPara;
import com.simple.datax.service.impl.SendStatusHandler;
import com.simple.datax.utils.MessageUtils;
import com.simple.datax.utils.SimpleConstants;

public class SenderImpl
        implements Sender {
    private static final Logger logger = LoggerFactory.getLogger(SenderImpl.class);
    private SendStatusHandler handler = null;
    private ConnectorPool connectorPool;
    private SendThreadPoolExecutor executor;

    public void init() {
        connectorPool = ConnectorPoolManager.getInstance().getConnectorPool("SEND", handler, executor);
    }

    @Override
    public void send(SimpleObject src) {
        if ((src instanceof SimpleMessage)) {
            String connPara = ((SimpleMessage) src).getSentCommunParam();
            String connType = ((SimpleMessage) src).getSentCommunType();
            String encoding = ((SimpleMessage) src).getSentEncoding();

            BaseConnector connector = null;
            try {
                CommonConnPara para = new CommonConnPara();
                para.setEncoding(encoding);
                para.setTransportType(connType);
                para.setTransportPara(connPara);
                connector = connectorPool.borrowObject(para);
                if (connector != null) {
                    connector.send(src);
                } else {
                    handleError((SimpleMessage) src, new ConnectorException("No available connector for sending message"), connType + ":" + connPara);
                }
            } catch (Exception e) {
                handleError((SimpleMessage) src, e, connType + ":" + connPara);
                if (connector == null) {
                    return;
                }
                try {
                    connectorPool.returnObject(connector);
                } catch (Exception ex) {
                    logger.error("Connector release error.", ex);
                }
            } finally {
                if (connector != null) {
                    try {
                        connectorPool.returnObject(connector);
                    } catch (Exception e) {
                        logger.error("Connector release error.", e);
                    }
                }
            }
            try {
                connectorPool.returnObject(connector);
            } catch (Exception e) {
                logger.error("Connector release error.", e);
            }
        } else {
            logger.info("########## No SimpleMessage");
            handleError((SimpleMessage) src, new InvalidSimpleObjectException(src.getClass().getName()), "");
        }
    }

    private void handleError(SimpleMessage src, Exception ex, String para) {
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

    public SendStatusHandler getHandler() {
        return handler;
    }

    public void setHandler(SendStatusHandler handler) {
        this.handler = handler;
    }

    public SendThreadPoolExecutor getExecutor() {
        return executor;
    }

    public void setExecutor(SendThreadPoolExecutor executor) {
        this.executor = executor;
    }
}



