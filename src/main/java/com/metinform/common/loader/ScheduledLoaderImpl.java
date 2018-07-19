package com.metinform.common.loader;

import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.metinform.common.BaseConnector;
import com.metinform.common.ConnectorPool;
import com.metinform.common.monitor.ConnectorPoolManager;
import com.metinform.common.param.ConnectParam;
import com.simple.datax.api.Loader;
import com.simple.datax.api.Receiver;
import com.simple.datax.core.model.connpara.CommonConnPara;

public class ScheduledLoaderImpl
        implements Loader {
    private ConnectParam connectParam = null;
    private Receiver receiver = null;
    private ThreadPoolTaskExecutor taskExecutor = null;
    private int threshold;
    private ThreadPoolExecutor recvThreadPoolExecutor;
    private ConnectorPool connectorPool = ConnectorPoolManager.getInstance().getConnectorPool("RECEIVE", null, null);
    private static final Logger logger = LoggerFactory.getLogger(ScheduledLoaderImpl.class);

    @Override
    @Async
    public void run() {
        logger.debug("========Load.run()");
        long start = System.currentTimeMillis();
        List<CommonConnPara> list = loadAllConnectorInfo();
        long end = System.currentTimeMillis();
        logger.debug("======== time:" + (end - start));
        if (list == null) {
            logger.debug("======== list is null.");
            return;
        }
        logger.debug("========" + list.size());
        BaseConnector connector = null;
        for (CommonConnPara para : list) {
            try {
                connector = connectorPool.borrowObject(para);
                logger.error("Reciver get message" + connector);
                if (connector != null) {
                    connector.setRecvThreadPoolExecutor(recvThreadPoolExecutor);
                    taskExecutor.execute(receiver.process(connector, para, threshold));
                } else {
                    logger.debug("No available Connector, skip this time.");
                }
            } catch (Exception e) {
                logger.error("Reciver get message error.", e);
            }
        }
    }

    private List<CommonConnPara> loadAllConnectorInfo() {
        logger.debug("loadAllConnectorInfo()");
        List<CommonConnPara> ret = connectParam.getRecvConnList();
        return ret;
    }

    public Receiver getReceiver() {
        return receiver;
    }

    public void setReceiver(Receiver receiver) {
        this.receiver = receiver;
    }

    public ThreadPoolTaskExecutor getTaskExecutor() {
        return taskExecutor;
    }

    public void setTaskExecutor(ThreadPoolTaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    public int getThreshold() {
        return threshold;
    }

    public void setThreshold(int threshold) {
        this.threshold = threshold;
    }

    public ConnectParam getConnectParam() {
        return connectParam;
    }

    public void setConnectParam(ConnectParam connectParam) {
        this.connectParam = connectParam;
    }

    public ThreadPoolExecutor getRecvThreadPoolExecutor() {
        return recvThreadPoolExecutor;
    }

    public void setRecvThreadPoolExecutor(ThreadPoolExecutor recvThreadPoolExecutor) {
        this.recvThreadPoolExecutor = recvThreadPoolExecutor;
    }
}



